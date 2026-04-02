#!/usr/bin/env python3
"""Export Phase 1/2 job-board data to Google Sheets.

This script reads:

- app/data/discovery_sources_test.json  →  <IST_DATE>_sources tab
- app/data/jobs/jobs_master.json        →  <IST_DATE>_jobs tab

The target sheet is configured via JOB_BOARD_SHEET_ID in .env and uses the
same service-account credentials.json as the Telegram exporter.

Usage:
  python scripts/export_job_board_jobs_to_sheets.py
  python scripts/export_job_board_jobs_to_sheets.py --date 2026-03-16
  python scripts/export_job_board_jobs_to_sheets.py --append-jobs   # same-day batches: add rows, do not wipe tab
"""

import argparse
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.job_board_sheets_service import JobBoardSheetsService  # noqa: E402
from app.utils.timezone import ist_today_utc_window  # noqa: E402
from app.config import settings  # noqa: E402

try:
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover
    HttpError = type("HttpError", (Exception,), {})  # type: ignore[misc,assignment]


def _print_no_jobs_hint(*, from_postgres: bool, postgres_source: str, date_str: str) -> None:
    if not from_postgres:
        return
    print(
        "\n  No job rows matched for this export. For --from-postgres we select rows where "
        f"source={postgres_source!r} AND (created_at OR updated_at falls on IST date {date_str}).\n"
        "  Fix: run ./venv/bin/python3 scripts/job_ingest/sync_verified_to_postgres.py (uses DB now() for updated_at), "
        "then re-run. Or export every job_board row into this tab (no date filter):\n"
        "    ./venv/bin/python3 scripts/export_job_board_jobs_to_sheets.py --from-postgres --postgres-all-job-board "
        f"--date {date_str}\n"
        "  (omit --append-jobs to replace the tab; with --append-jobs you may duplicate rows.)\n",
        file=sys.stderr,
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Export job-board sources + jobs to Google Sheets")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="IST date string for tab names (YYYY-MM-DD). Defaults to today's IST date.",
    )
    parser.add_argument(
        "--append-jobs",
        action="store_true",
        help="Append jobs to <date>_jobs without clearing existing rows (keeps all batches for that day).",
    )
    parser.add_argument(
        "--jobs-json",
        type=Path,
        default=None,
        help="Jobs file (default: app/data/jobs/jobs_master.json). Use app/data/jobs/jobs_verified.json after ML pipeline.",
    )
    parser.add_argument(
        "--from-postgres",
        action="store_true",
        help="Export jobs from Postgres jobs table (source=job_board) instead of JSON.",
    )
    parser.add_argument(
        "--postgres-source",
        type=str,
        default="job_board",
        help="When --from-postgres: filter jobs.source by this value (default: job_board).",
    )
    parser.add_argument(
        "--postgres-all-job-board",
        action="store_true",
        help="When --from-postgres: ignore IST date filter and export all rows for --postgres-source "
        "(repair empty daily tab). Avoid --append-jobs unless you accept duplicates.",
    )
    parser.add_argument(
        "--max-jobs-per-domain",
        type=int,
        default=0,
        help="When --from-postgres: cap rows per source domain in daily export (0 = no cap).",
    )
    args = parser.parse_args()

    if args.postgres_all_job_board and not args.from_postgres:
        print("export_job_board_jobs_to_sheets: --postgres-all-job-board requires --from-postgres", file=sys.stderr)
        return 1

    if not getattr(settings, "JOB_BOARD_SHEET_ID", None):
        print(
            "export_job_board_jobs_to_sheets: JOB_BOARD_SHEET_ID is empty in .env / settings.",
            file=sys.stderr,
        )
        return 1

    if args.date:
        ist_date_str = args.date
    else:
        _, _, ist_date_str = ist_today_utc_window()

    data_dir = PROJECT_ROOT / "app" / "data"
    sources_path = data_dir / "discovery_sources_test.json"
    if args.jobs_json is not None:
        jobs_path = args.jobs_json
        if not jobs_path.is_absolute():
            jobs_path = PROJECT_ROOT / jobs_path
    else:
        jobs_path = data_dir / "jobs" / "jobs_master.json"

    try:
        service = JobBoardSheetsService()
    except ValueError as e:
        print(f"export_job_board_jobs_to_sheets: {e}", file=sys.stderr)
        print(
            "  Set JOB_BOARD_SHEET_ID in .env (spreadsheet id from the Google Sheet URL).",
            file=sys.stderr,
        )
        return 1

    print(">>> Sheets: sources tab ...", flush=True)
    try:
        sources_result = service.export_sources_from_json(sources_path, ist_date_str)
        if args.from_postgres:
            local_db_url = os.getenv("LOCAL_DATABASE_URL")
            if local_db_url:
                sync_database_url = local_db_url
            else:
                sync_database_url = str(settings.DATABASE_URL).replace("+asyncpg", "")
                sync_database_url = sync_database_url.replace("?ssl=require", "?sslmode=require")
                sync_database_url = sync_database_url.replace("&ssl=require", "&sslmode=require")
            engine = create_engine(sync_database_url, pool_pre_ping=True)
            SessionLocal = sessionmaker(bind=engine)
            db = SessionLocal()
            try:
                print(
                    ">>> Sheets: jobs tab from Postgres (append=%s, all_rows=%s) ..."
                    % (bool(args.append_jobs), bool(args.postgres_all_job_board)),
                    flush=True,
                )
                jobs_result = service.export_jobs_from_postgres(
                    db,
                    date_str=ist_date_str,
                    append=bool(args.append_jobs),
                    source_value=str(args.postgres_source or "job_board"),
                    ignore_date_filter=bool(args.postgres_all_job_board),
                    max_jobs_per_domain=int(args.max_jobs_per_domain or 0),
                )
            finally:
                db.close()
                engine.dispose()
        else:
            print(">>> Sheets: jobs tab from JSON (append=%s) ..." % bool(args.append_jobs), flush=True)
            jobs_result = service.export_jobs_from_json(
                jobs_path, ist_date_str, append=bool(args.append_jobs)
            )

        print("Sources export:", sources_result)
        print("Jobs export   :", jobs_result)
        if isinstance(jobs_result, dict) and jobs_result.get("status") == "no_jobs":
            _print_no_jobs_hint(
                from_postgres=bool(args.from_postgres),
                postgres_source=str(args.postgres_source or "job_board"),
                date_str=ist_date_str,
            )
    except HttpError as e:
        status = getattr(getattr(e, "resp", None), "status", "?")
        print(f"Google Sheets API error ({status}): {e}", file=sys.stderr)
        print(
            "  Share the spreadsheet with the service-account email from credentials.json (Editor). "
            "Verify JOB_BOARD_SHEET_ID matches the Sheet URL.",
            file=sys.stderr,
        )
        return 1
    except OSError as e:
        print(f"export_job_board_jobs_to_sheets: credentials / file error: {e}", file=sys.stderr)
        print(
            "  Ensure credentials.json exists at the project root (service account with Sheets access).",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

