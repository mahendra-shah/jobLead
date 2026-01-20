"""Celery Beat schedule configuration for periodic tasks."""

from celery.schedules import crontab

# Celery Beat Schedule
beat_schedule = {
    # Daily Google Sheets export at 7:00 AM IST
    'export-daily-jobs-to-sheets': {
        'task': 'app.workers.sheets_export.export_daily_jobs_to_sheets',
        'schedule': crontab(hour=7, minute=0),  # 7:00 AM every day
        'options': {
            'expires': 3600,  # Task expires after 1 hour if not executed
        }
    },
    
    # Telegram scraping (if using Celery instead of Lambda)
    # 'scrape-telegram-channels': {
    #     'task': 'app.workers.telegram_scraper.scrape_all_channels',
    #     'schedule': crontab(minute='*/30'),  # Every 30 minutes
    # },
    
    # Job cleanup (remove old jobs)
    # 'cleanup-old-jobs': {
    #     'task': 'app.workers.job_processor.cleanup_old_jobs',
    #     'schedule': crontab(hour=2, minute=0),  # 2:00 AM daily
    # },
}

# Celery Beat Configuration
beat_config = {
    'beat_schedule': beat_schedule,
    'timezone': 'Asia/Kolkata',  # IST timezone
}
