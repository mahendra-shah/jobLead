"""
Standalone script to export daily jobs to Google Sheets
Designed to run via cron job (no Celery needed)
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import settings
from app.services.google_sheets_service import GoogleSheetsService


def export_yesterday_jobs():
    """Export yesterday's processed jobs to Google Sheets."""
    
    print("=" * 60)
    print("📊 DAILY GOOGLE SHEETS EXPORT")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    # Use sync database URL
    if hasattr(settings, 'LOCAL_DATABASE_URL'):
        database_url = settings.LOCAL_DATABASE_URL
    else:
        # Convert async URL to sync URL
        database_url = str(settings.DATABASE_URL).replace('+asyncpg', '')
        database_url = database_url.replace('?ssl=require', '?sslmode=require')
        database_url = database_url.replace('&ssl=require', '&sslmode=require')
    
    print(f"📂 Database: {database_url.split('@')[1] if '@' in database_url else 'local'}")
    
    # Create sync engine and session
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    
    try:
        # Initialize Google Sheets service
        sheets_service = GoogleSheetsService()
        
        # Export yesterday's jobs
        yesterday = datetime.now() - timedelta(days=1)
        print(f"📅 Exporting jobs from: {yesterday.strftime('%Y-%m-%d')}")
        
        result = sheets_service.export_daily_jobs(db, yesterday)
        
        print("\n" + "=" * 60)
        print("✅ EXPORT COMPLETE")
        print("=" * 60)
        print(f"Status: {result['status']}")
        print(f"Date: {result['date']}")
        
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
        print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")


if __name__ == "__main__":
    exit_code = export_yesterday_jobs()
    sys.exit(exit_code)
