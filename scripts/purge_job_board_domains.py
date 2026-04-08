#!/usr/bin/env python3
"""
Remove bad job-board sources from Postgres + pause them in Mongo, then optionally
re-export a daily Google Sheet tab (replace mode recommended after purge).

Example (Webpulse, Postgres + sheet):
  ./venv/bin/python scripts/purge_job_board_domains.py \\
    --domain webpulseindia.com --domain webpulse \\
    --postgres-source job_board \\
    --pause-mongo \\
    --reexport-sheet-date 2026-04-08

Mongo only (remove source row after sheet is clean):
  ./venv/bin/python scripts/purge_job_board_domains.py \\
    --domain webpulseindia.com --domain webpulse \\
    --skip-postgres --delete-mongo-sources

JSON fallback sheet (filter master file + push sheet from JSON, no Postgres export):
  ./venv/bin/python scripts/purge_job_board_domains.py \\
    --domain webpulseindia.com --domain webpulse \\
    --skip-postgres \\
    --filter-jobs-json app/data/jobs/jobs_master.json \\
    --reexport-sheet-date 2026-04-08 \\
    --sheet-via-jobs-json app/data/jobs/jobs_master.json

Dry run:
  ./venv/bin/python scripts/purge_job_board_domains.py --domain webpulseindia.com --dry-run

If the script seems to hang on connect, set a shorter fail-fast timeout (seconds):
  PG_CONNECT_TIMEOUT=8 ./venv/bin/python scripts/purge_job_board_domains.py ...
Or point at a reachable DB:
  LOCAL_DATABASE_URL=postgresql://user:pass@127.0.0.1:5432/dbname ./venv/bin/python ...
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings  # noqa: E402


def _sync_engine_url() -> str:
    local_db_url = os.getenv("LOCAL_DATABASE_URL")
    if local_db_url:
        return local_db_url
    u = str(settings.DATABASE_URL).replace("+asyncpg", "")
    u = u.replace("?ssl=require", "?sslmode=require")
    u = u.replace("&ssl=require", "&sslmode=require")
    return u


def _connect_timeout_sec() -> int:
    try:
        return max(3, min(120, int(os.getenv("PG_CONNECT_TIMEOUT", "12"))))
    except ValueError:
        return 12


def _mask_url(url: str) -> str:
    if "@" not in url:
        return url.split("?")[0]
    try:
        head, tail = url.split("@", 1)
        if "://" in head:
            scheme, rest = head.split("://", 1)
            if ":" in rest:
                user, _ = rest.split(":", 1)
                return f"{scheme}://{user}:***@{tail.split('?')[0]}"
    except Exception:
        pass
    return url.split("?")[0]


def _json_job_matches(job: dict, tokens: list[str]) -> bool:
    parts = [
        job.get("company") or "",
        job.get("source_domain") or "",
        job.get("apply_url") or "",
        job.get("url") or "",
        job.get("title") or "",
        str(job.get("description") or "")[:4000],
    ]
    hay = " ".join(str(p) for p in parts).lower()
    return any(tok in hay for tok in tokens)


def _filter_jobs_json_file(path: Path, tokens: list[str], *, dry_run: bool) -> tuple[int, int]:
    """Return (kept_count, removed_count). Writes file unless dry_run."""
    raw_text = path.read_text(encoding="utf-8")
    raw = json.loads(raw_text)
    if isinstance(raw, list):
        jobs = [j for j in raw if isinstance(j, dict)]
        wrapper: dict[str, object] | None = None
    else:
        jobs = [j for j in (raw.get("jobs") or []) if isinstance(j, dict)]
        wrapper = raw
    kept = [j for j in jobs if not _json_job_matches(j, tokens)]
    removed = len(jobs) - len(kept)
    if not dry_run:
        if wrapper is None:
            path.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            out = dict(wrapper)
            out["jobs"] = kept
            path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(kept), removed


def _make_engine() -> object:
    url = _sync_engine_url()
    timeout = _connect_timeout_sec()
    # Fail fast if Postgres is unreachable (avoids long hangs on SQLAlchemy/psycopg2 connect hooks).
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": timeout},
    )


def _mongo_source_match_filter(tokens: list[str]) -> dict:
    or_clauses: list[dict] = []
    for tok in tokens:
        pat = f"{tok}"
        or_clauses.extend(
            [
                {"domain": {"$regex": pat, "$options": "i"}},
                {"url": {"$regex": pat, "$options": "i"}},
                {"url_norm": {"$regex": pat, "$options": "i"}},
                {"name": {"$regex": pat, "$options": "i"}},
            ]
        )
    return {"$or": or_clauses}


def main() -> int:
    ap = argparse.ArgumentParser(description="Purge job_board rows + pause Mongo sources by domain/name match.")
    ap.add_argument(
        "--domain",
        action="append",
        dest="domains",
        default=[],
        help="Substring to match (lowercase) in company_name, source_url, title. Repeatable.",
    )
    ap.add_argument(
        "--postgres-source",
        type=str,
        default="job_board",
        help="jobs.source value (default: job_board)",
    )
    ap.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Do not connect to Postgres (use with --filter-jobs-json / JSON sheet export only).",
    )
    ap.add_argument(
        "--filter-jobs-json",
        action="append",
        default=[],
        metavar="PATH",
        help="Remove matching jobs from this JSON file ({\"jobs\":[...]} or top-level array). Repeatable.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print counts only; no deletes or Mongo updates.")
    ap.add_argument("--pause-mongo", action="store_true", help="Set job_board_sources.status=paused for matches.")
    ap.add_argument(
        "--delete-mongo-sources",
        action="store_true",
        help="Permanently delete matching documents from Mongo job_board_sources (stronger than --pause-mongo).",
    )
    ap.add_argument(
        "--reexport-sheet-date",
        type=str,
        default="",
        metavar="YYYY-MM-DD",
        help="If set, run export_job_board_jobs_to_sheets.py for this IST date (no --append-jobs).",
    )
    ap.add_argument(
        "--sheet-via-jobs-json",
        type=str,
        default="",
        metavar="PATH",
        help="With --reexport-sheet-date: use --jobs-json PATH instead of --from-postgres.",
    )
    args = ap.parse_args()

    tokens = [t.strip().lower() for t in args.domains if t and t.strip()]
    if not tokens:
        print("Provide at least one --domain ...", file=sys.stderr)
        return 1

    filter_paths: list[Path] = []
    for raw in args.filter_jobs_json or []:
        if not (raw or "").strip():
            continue
        p = Path(raw.strip())
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        if not p.is_file():
            print(f"JSON file not found: {p}", file=sys.stderr)
            return 1
        filter_paths.append(p)

    engine = None
    if not args.skip_postgres:
        db_url_display = _mask_url(_sync_engine_url())
        print(
            f"Connecting to Postgres ({db_url_display}) "
            f"connect_timeout={_connect_timeout_sec()}s "
            f"(override with PG_CONNECT_TIMEOUT; use LOCAL_DATABASE_URL for a different host) …",
            flush=True,
        )
        engine = _make_engine()
        deleted = 0
        try:
            with engine.connect() as conn:
                conditions = []
                params: dict[str, str] = {"src": args.postgres_source}
                for i, tok in enumerate(tokens):
                    key = f"t{i}"
                    params[key] = f"%{tok}%"
                    conditions.append(
                        "("
                        f"lower(coalesce(company_name,'')) LIKE :{key} "
                        f"OR lower(coalesce(source_url,'')) LIKE :{key} "
                        f"OR lower(coalesce(title,'')) LIKE :{key}"
                        ")"
                    )
                where_sql = " OR ".join(conditions)
                count_sql = text(
                    f"SELECT count(*) FROM jobs WHERE source = :src AND ({where_sql})"
                )
                n = conn.execute(count_sql, params).scalar()
                print(
                    f"Postgres jobs matching source={args.postgres_source!r} and domains: count={n}",
                    flush=True,
                )
                if args.dry_run:
                    print("Dry run: no DELETE executed.", flush=True)
                else:
                    del_sql = text(f"DELETE FROM jobs WHERE source = :src AND ({where_sql})")
                    result = conn.execute(del_sql, params)
                    deleted = int(result.rowcount or 0)
                    conn.commit()
                    print(f"Deleted {deleted} job row(s).", flush=True)
        except OperationalError as e:
            print(
                f"Postgres connection failed: {e}\n"
                "  Check VPN/network, that PostgreSQL is running, and DATABASE_URL / LOCAL_DATABASE_URL in .env.\n"
                "  For JSON-only sheet workflows use --skip-postgres and --filter-jobs-json.",
                file=sys.stderr,
                flush=True,
            )
            engine.dispose()
            return 1
    else:
        print("Skipping Postgres (--skip-postgres).", flush=True)

    for p in filter_paths:
        k, r = _filter_jobs_json_file(p, tokens, dry_run=bool(args.dry_run))
        print(f"Jobs JSON {p.name}: removed={r} kept={k} (dry_run={args.dry_run})", flush=True)

    mongo_action = False
    if args.delete_mongo_sources and args.pause_mongo:
        print("Note: --delete-mongo-sources wins over --pause-mongo.", flush=True)
    if args.delete_mongo_sources or args.pause_mongo:
        mongo_action = True

    if mongo_action:
        try:
            from pymongo import MongoClient
        except ImportError:
            print("pymongo not installed; skipping Mongo.", file=sys.stderr)
        else:
            filt = _mongo_source_match_filter(tokens)
            if args.dry_run:
                client = MongoClient(settings.MONGODB_URI, serverSelectionTimeoutMS=8000)
                client.admin.command("ping")
                col = client[settings.MONGODB_DATABASE]["job_board_sources"]
                nmatch = col.count_documents(filt)
                print(
                    f"Mongo job_board_sources dry-run: documents matching filter count={nmatch}",
                    flush=True,
                )
                if args.delete_mongo_sources:
                    print("  (would DELETE these documents; re-run without --dry-run)", flush=True)
                else:
                    print("  (would PAUSE these documents; re-run without --dry-run)", flush=True)
                client.close()
            else:
                client = MongoClient(settings.MONGODB_URI, serverSelectionTimeoutMS=8000)
                client.admin.command("ping")
                col = client[settings.MONGODB_DATABASE]["job_board_sources"]
                if args.delete_mongo_sources:
                    r = col.delete_many(filt)
                    print(
                        f"Mongo job_board_sources: deleted_count={r.deleted_count}",
                        flush=True,
                    )
                else:
                    r = col.update_many(
                        filt, {"$set": {"status": "paused", "crawl_ready": False}}
                    )
                    print(
                        f"Mongo job_board_sources: matched={r.matched_count} modified={r.modified_count}",
                        flush=True,
                    )
                client.close()

    if args.reexport_sheet_date and not args.dry_run:
        py = sys.executable
        json_path = (args.sheet_via_jobs_json or "").strip()
        if json_path:
            jp = Path(json_path)
            if not jp.is_absolute():
                jp = PROJECT_ROOT / jp
            cmd = [
                py,
                "scripts/export_job_board_jobs_to_sheets.py",
                "--jobs-json",
                str(jp),
                "--date",
                args.reexport_sheet_date,
            ]
        else:
            cmd = [
                py,
                "scripts/export_job_board_jobs_to_sheets.py",
                "--from-postgres",
                "--date",
                args.reexport_sheet_date,
                "--postgres-source",
                str(args.postgres_source),
            ]
        print("Re-export sheet:", " ".join(cmd), flush=True)
        rc = subprocess.run(cmd, cwd=PROJECT_ROOT).returncode
        if rc != 0:
            return rc
    elif args.reexport_sheet_date and args.dry_run:
        print("Dry run: sheet re-export skipped.", flush=True)

    if engine is not None:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
