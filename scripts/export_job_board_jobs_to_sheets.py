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
    args = parser.parse_args()

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

    service = JobBoardSheetsService()

    sources_result = service.export_sources_from_json(sources_path, ist_date_str)
    if args.from_postgres:
        local_db_url = os.getenv("LOCAL_DATABASE_URL")
        if local_db_url:
            sync_database_url = local_db_url
        else:
            sync_database_url = str(settings.DATABASE_URL).replace("+asyncpg", "")
            sync_database_url = sync_database_url.replace("?ssl=require", "?sslmode=require")
            sync_database_url = sync_database_url.replace("&ssl=require", "&sslmode=require")
        engine = create_engine(sync_database_url)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        try:
            jobs_result = service.export_jobs_from_postgres(
                db,
                date_str=ist_date_str,
                append=bool(args.append_jobs),
                source_value=str(args.postgres_source or "job_board"),
            )
        finally:
            db.close()
            engine.dispose()
    else:
        jobs_result = service.export_jobs_from_json(
            jobs_path, ist_date_str, append=bool(args.append_jobs)
        )

    print("Sources export:", sources_result)
    print("Jobs export   :", jobs_result)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

