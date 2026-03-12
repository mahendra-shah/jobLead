#!/usr/bin/env python3
"""Export Phase 1/2 job-board data to Google Sheets.

This script reads:

- app/data/discovery_sources_test.json  →  <IST_DATE>_sources tab
- app/data/jobs/jobs_master.json        →  <IST_DATE>_jobs tab

The target sheet is configured via JOB_BOARD_SHEET_ID in .env and uses the
same service-account credentials.json as the Telegram exporter.
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.job_board_sheets_service import JobBoardSheetsService  # noqa: E402
from app.utils.timezone import ist_today_utc_window  # noqa: E402


def main() -> int:
    _, _, ist_date_str = ist_today_utc_window()

    data_dir = PROJECT_ROOT / "app" / "data"
    sources_path = data_dir / "discovery_sources_test.json"
    jobs_path = data_dir / "jobs" / "jobs_master.json"

    service = JobBoardSheetsService()

    sources_result = service.export_sources_from_json(sources_path, ist_date_str)
    jobs_result = service.export_jobs_from_json(jobs_path, ist_date_str)

    print("Sources export:", sources_result)
    print("Jobs export   :", jobs_result)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

