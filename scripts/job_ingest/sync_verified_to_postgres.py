#!/usr/bin/env python3
"""Sync Mongo job_ingest verified rows into Postgres jobs table (source=job_board)."""

from __future__ import annotations

import argparse
import sys
import time
import warnings
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy.exc import OperationalError, ProgrammingError

from app.services.job_board_ml.postgres_sync import (
    build_sync_engine,
    run_db_with_retries,
    sync_job_board_rows,
)
from app.services.mongodb_job_ingest_service import MongoJobIngestService


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync verified Mongo job_board jobs to Postgres jobs table")
    ap.add_argument("--limit", type=int, default=50000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--include-synced",
        action="store_true",
        help="Also include already-synced verified rows (default: unsynced/changed only).",
    )
    args = ap.parse_args()
    print(f"sync_verified_to_postgres: loading verified rows from Mongo (limit={int(args.limit)})...")

    mongo_rows = None
    svc: MongoJobIngestService | None = None
    last_err: Exception | None = None
    mongo_retries = 5
    for attempt in range(1, mongo_retries + 1):
        try:
            svc = MongoJobIngestService()
            mongo_rows = svc.list_verified_payloads(
                limit=int(args.limit),
                unsynced_only=not bool(args.include_synced),
            )
            break
        except RuntimeError as e:
            last_err = e
            if attempt >= mongo_retries:
                break
            delay_s = min(20, 2 ** (attempt - 1))
            print(
                f"sync_verified_to_postgres: Mongo unavailable (attempt {attempt}/{mongo_retries}); "
                f"retrying in {delay_s}s ...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay_s)

    if mongo_rows is None:
        print(f"sync_verified_to_postgres: {last_err}", file=sys.stderr)
        return 1

    if svc is None:
        print("sync_verified_to_postgres: Mongo service init failed unexpectedly", file=sys.stderr)
        return 1

    rows = list(mongo_rows)
    print(f"sync_verified_to_postgres: syncing {len(rows)} rows to Postgres...")

    engine = build_sync_engine()
    try:

        def _run_all():
            return sync_job_board_rows(engine, rows, dry_run=bool(args.dry_run), batch_size=40)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*Did not recognize type 'vector'.*")
            stats = run_db_with_retries(_run_all)
    except ProgrammingError as e:
        orig = str(getattr(e, "orig", e) or "")
        if 'relation "jobs" does not exist' in orig.lower():
            print(
                "sync_verified_to_postgres: PostgreSQL tables are missing. "
                "Apply migrations from the project root:\n"
                "  ./venv/bin/alembic upgrade head",
                file=sys.stderr,
            )
            return 1
        print(f"sync_verified_to_postgres: database error: {orig}", file=sys.stderr)
        raise
    except OperationalError as e:
        print(
            "sync_verified_to_postgres: PostgreSQL connection failed. "
            "Often a brief Wi‑Fi/VPN drop to Neon. Re-run: completed batches are already committed.\n"
            f"  {e}",
            file=sys.stderr,
        )
        raise
    finally:
        engine.dispose()

    marked_synced = 0
    if not bool(args.dry_run):
        dedupe_keys = [str(r.get("_dedupe_key") or "").strip() for r in rows]
        marked_synced = svc.mark_pg_synced(dedupe_keys)

    print(
        f"synced_verified_to_postgres inserted={stats.inserted} updated={stats.updated} "
        f"skipped={stats.skipped} dry_run={args.dry_run} "
        f"marked_synced={marked_synced} include_synced={bool(args.include_synced)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
