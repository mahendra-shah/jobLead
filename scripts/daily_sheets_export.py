
"""
Standalone script to export daily jobs to Google Sheets
Designed to run via cron job (no Celery needed)
"""

import sys
from pathlib import Path
# Ensure project root is in sys.path for absolute imports to work
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


import sys
from pathlib import Path
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import settings
from app.services.google_sheets_service import GoogleSheetsService
from app.models.job import Job
from app.models.telegram_group import TelegramGroup
from app.models.telegram_account import TelegramAccount
from app.utils.timezone import now_ist



def export_jobs_for_prev_day(tab_date: datetime):
    """Export jobs for the previous day (tab_date - 1), and name the sheet/tab as that previous day."""
    print("=" * 60)
    print("\U0001F4CA DAILY GOOGLE SHEETS EXPORT")
    print("=" * 60)
    print(f"Started at: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")

    # Use sync database URL
    if hasattr(settings, 'LOCAL_DATABASE_URL'):
        database_url = settings.LOCAL_DATABASE_URL
    else:
        database_url = str(settings.DATABASE_URL).replace('+asyncpg', '')
        database_url = database_url.replace('?ssl=require', '?sslmode=require')
        database_url = database_url.replace('&ssl=require', '&sslmode=require')

    print(f"\U0001F4C2 Database: {database_url.split('@')[1] if '@' in database_url else 'local'}")

    # Calculate the previous day
    prev_day = tab_date - timedelta(days=1)

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        sheets_service = GoogleSheetsService()
        print(f"\U0001F4C5 Exporting jobs for IST date: {prev_day.strftime('%Y-%m-%d')} (tab name: {prev_day.strftime('%Y-%m-%d')})")
        result = sheets_service.export_daily_jobs(db, prev_day, tab_name_override=prev_day.strftime('%Y-%m-%d'))

        print("\n" + "=" * 60)
        print("✅ EXPORT COMPLETE")
        print("=" * 60)
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Tab Name: {result['tab_name']}")
            print(f"Jobs Exported: {result['jobs_exported']}")
            print(f"Sheet URL: {result['sheet_url']}")
            return 0
        elif result['status'] == 'no_jobs':
            print(f"⚠️  No jobs found for {result['date']}")
            return 0
        else:
            print(f"❌ Export failed")
            return 1

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        db.close()
        engine.dispose()
        print(f"\nFinished at: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export daily jobs to Google Sheets.")
    parser.add_argument('--date', type=str, help='IST date to use as reference (YYYY-MM-DD). Both tab and data will be for the previous day.')
    args = parser.parse_args()

    if args.date:
        try:
            ref_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        ist_now = now_ist()
        ref_date = ist_now.replace(hour=0, minute=0, second=0, microsecond=0)

    exit_code = export_jobs_for_prev_day(ref_date)
    sys.exit(exit_code)
