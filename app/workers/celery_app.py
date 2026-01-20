"""Celery application configuration."""

from celery import Celery
from celery.schedules import crontab

from app.config import settings

# Create Celery app
celery_app = Celery(
    "placement_dashboard",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.workers.telegram_scraper",
        "app.workers.job_processor",
        "app.workers.ml_tasks",
        "app.workers.email_tasks",
        "app.workers.sheets_export",
    ],
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",  # IST timezone for cron jobs
    enable_utc=False,  # Use local timezone (IST)
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)

# Periodic tasks schedule
celery_app.conf.beat_schedule = {
    # Daily Google Sheets export at 7:00 AM IST
    "export-daily-jobs-to-sheets": {
        "task": "app.workers.sheets_export.export_daily_jobs_to_sheets",
        "schedule": crontab(hour=7, minute=0),  # 7:00 AM IST
    },
    # Telegram scraping (optional - if not using Lambda)
    "scrape-telegram-channels": {
        "task": "app.workers.telegram_scraper.scrape_all_channels",
        "schedule": crontab(minute=f"*/{settings.TELEGRAM_SCRAPE_INTERVAL_MINUTES}"),
    },
    "process-pending-jobs": {
        "task": "app.workers.job_processor.process_pending_jobs",
        "schedule": crontab(minute="*/10"),  # Every 10 minutes
    },
    "cleanup-old-jobs": {
        "task": "app.workers.job_processor.cleanup_old_jobs",
        "schedule": crontab(hour=2, minute=0),  # Daily at 2 AM
    },
}

if __name__ == "__main__":
    celery_app.start()
