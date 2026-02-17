"""Google Sheets export tasks."""

import logging
from datetime import datetime, timedelta

from app.workers.celery_app import celery_app
from app.db.session import SyncSessionLocal
from app.services.google_sheets_service import GoogleSheetsService

logger = logging.getLogger(__name__)


@celery_app.task(name="app.workers.sheets_export.export_daily_jobs_to_sheets")
def export_daily_jobs_to_sheets():
    """
    Export yesterday's processed jobs to Google Sheets.
    
    Runs daily at 7 AM.
    Creates a new tab named with date (e.g., "2026-01-21_v2").
    """
    logger.info("🚀 Starting daily Google Sheets export...")
    
    db = SyncSessionLocal()
    try:
        sheets_service = GoogleSheetsService()
        
        # Export yesterday's jobs (jobs processed yesterday)
        yesterday = datetime.now() - timedelta(days=1)
        result = sheets_service.export_daily_jobs(db, yesterday)
        
        logger.info(f"✅ Export complete: {result}")
        
        return {
            'status': 'success',
            'result': result,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Export failed: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
    finally:
        db.close()


@celery_app.task(name="app.workers.sheets_export.export_today_jobs_to_sheets")
def export_today_jobs_to_sheets():
    """
    Export TODAY's processed jobs to Google Sheets (for testing).
    
    Use this to test the export without waiting for cron.
    """
    logger.info("🧪 Testing Google Sheets export with today's jobs...")
    
    db = SyncSessionLocal()
    try:
        sheets_service = GoogleSheetsService()
        
        # Export today's jobs
        today = datetime.now()
        result = sheets_service.export_daily_jobs(db, today)
        
        logger.info(f"✅ Test export complete: {result}")
        
        return {
            'status': 'success',
            'result': result,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Test export failed: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
    finally:
        db.close()


@celery_app.task(name="app.workers.sheets_export.export_custom_date_to_sheets")
def export_custom_date_to_sheets(date_str: str):
    """
    Export jobs from a specific date to Google Sheets.
    
    Args:
        date_str: Date in format "YYYY-MM-DD"
    
    Example:
        export_custom_date_to_sheets.delay("2026-01-20")
    """
    logger.info(f"📅 Exporting jobs from {date_str}...")
    
    db = SyncSessionLocal()
    try:
        sheets_service = GoogleSheetsService()
        
        # Parse date
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        result = sheets_service.export_daily_jobs(db, target_date)
        
        logger.info(f"✅ Custom date export complete: {result}")
        
        return {
            'status': 'success',
            'result': result,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Custom date export failed: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
    finally:
        db.close()
