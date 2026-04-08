#!/usr/bin/env python3
"""Mongo job_ingest: pending → quality + sklearn job classifier + India gate + enrichment → verified/rejected.

Optionally upserts each verified batch into Postgres (`source=job_board`); on DB failure appends rows to JSONL fallback.
The batch script `sync_verified_to_postgres.py` remains the idempotent backfill for anything missed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.job_board_ml.worker import run_job_board_ingest_ml


def main() -> int:
    ap = argparse.ArgumentParser(description="ML pipeline for Mongo job_ingest (job boards)")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--min-confidence", type=float, default=0.5)
    ap.add_argument("--no-strict-india", action="store_true", help="Disable india_job_gate after ML pass")
    ap.add_argument("--no-postgres", action="store_true", help="Skip inline Postgres upsert (use sync script only)")
    ap.add_argument("--postgres-batch-size", type=int, default=40)
    ap.add_argument(
        "--fallback-jsonl",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "jobs" / "job_board_postgres_fallback.jsonl",
        help="Append rows when Postgres upsert fails (JSON Lines)",
    )
    ap.add_argument(
        "--no-fallback-file",
        action="store_true",
        help="Do not write JSONL fallback on DB errors",
    )
    ap.add_argument(
        "--recover-stale-processing",
        type=int,
        default=0,
        metavar="MINUTES",
        help="Re-queue docs stuck in processing longer than N minutes (0 = disabled)",
    )
    ap.add_argument(
        "--no-depth-profile",
        action="store_true",
        help="Disable description-based depth filter (fresher/remote/track); use legacy title+exp gates only",
    )
    ap.add_argument("--no-require-remote", action="store_true", help="Allow jobs without remote/hybrid/WFH signals")
    ap.add_argument("--no-require-role-track", action="store_true", help="Allow jobs outside marketing/sales/tech tracks")
    args = ap.parse_args()

    fallback = None if args.no_fallback_file else args.fallback_jsonl
    if fallback and not fallback.is_absolute():
        fallback = PROJECT_ROOT / fallback

    try:
        return run_job_board_ingest_ml(
            limit=int(args.limit),
            min_confidence=float(args.min_confidence),
            strict_india=not args.no_strict_india,
            sync_postgres=not args.no_postgres,
            postgres_batch_size=int(args.postgres_batch_size),
            fallback_jsonl=fallback,
            recover_stale_minutes=int(args.recover_stale_processing),
            depth_profile_enabled=False if args.no_depth_profile else None,
            require_remote_signal=False if args.no_require_remote else None,
            require_role_track=False if args.no_require_role_track else None,
        )
    except RuntimeError as e:
        print(f"process_job_ingest_ml: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
