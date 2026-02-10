"""
Application Scheduler - APScheduler Integration

Manages scheduled tasks for the FastAPI application.
Includes daily Telegram scraping and other periodic tasks.

Author: Backend Team
Date: 2026-02-10
"""

import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from app.config import settings

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = AsyncIOScheduler(
    timezone='UTC',
    job_defaults={
        'coalesce': True,  # Combine multiple missed executions into one
        'max_instances': 1,  # Only one instance of each job at a time
        'misfire_grace_time': 3600  # Job can run up to 1 hour late
    }
)


def scheduler_listener(event):
    """
    Listener for scheduler events (executed jobs, errors).
    
    Args:
        event: APScheduler event object
    """
    if event.exception:
        logger.error(
            f"❌ Job '{event.job_id}' failed with exception: {event.exception}",
            exc_info=True
        )
    else:
        logger.info(f"✅ Job '{event.job_id}' executed successfully")


# Add event listener
scheduler.add_listener(scheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


async def run_telegram_scraper():
    """
    Scheduled task: Run Telegram scraper to fetch messages from all channels.
    
    This job runs daily at 12:30 AM UTC (6:00 AM IST).
    Fetches messages from all active Telegram channels using multi-account support.
    """
    from app.services.telegram_scraper_service import get_scraper_service
    
    logger.info("=" * 60)
    logger.info("TELEGRAM SCRAPER - SCHEDULED JOB")
    logger.info("=" * 60)
    
    try:
        # Get scraper service
        scraper = get_scraper_service()
        
        # Initialize if needed
        await scraper.initialize()
        
        # Run scraping
        result = await scraper.scrape_all_channels()
        
        # Log results
        logger.info(f"\n✅ Telegram scraper completed successfully:")
        logger.info(f"   Total channels: {result['total_channels']}")
        logger.info(f"   Successful: {result['successful']}")
        logger.info(f"   Failed: {result['failed']}")
        logger.info(f"   Total messages: {result['total_messages']}")
        logger.info(f"   Duration: {result['duration_seconds']:.2f}s")
        
        return result
    
    except Exception as e:
        logger.error(f"❌ Telegram scraper failed: {e}", exc_info=True)
        raise
    
    finally:
        logger.info("=" * 60)


def setup_jobs():
    """
    Setup all scheduled jobs.
    
    Called during application startup to configure periodic tasks.
    """
    logger.info("⏰ Setting up scheduled jobs...")
    
    # Job 1: Daily Telegram Scraper
    # Runs at 12:30 AM UTC = 6:00 AM IST
    scheduler.add_job(
        run_telegram_scraper,
        CronTrigger(hour=0, minute=30),  # 00:30 UTC
        id='telegram_scraper_daily',
        name='Daily Telegram Message Scraper',
        replace_existing=True
    )
    logger.info("   ✅ Added: telegram_scraper_daily (00:30 UTC / 06:00 AM IST)")
    
    # Job 2: (Optional) More frequent scraping during business hours
    # Uncomment if you want more frequent scraping (every 2 hours)
    # scheduler.add_job(
    #     run_telegram_scraper,
    #     IntervalTrigger(hours=2),
    #     id='telegram_scraper_frequent',
    #     name='Frequent Telegram Scraper (Every 2 hours)',
    #     replace_existing=True
    # )
    # logger.info("   ✅ Added: telegram_scraper_frequent (Every 2 hours)")
    
    logger.info("✅ All scheduled jobs configured")


def start_scheduler():
    """
    Start the scheduler.
    
    Called during application startup (in lifespan).
    """
    if not scheduler.running:
        setup_jobs()
        scheduler.start()
        logger.info("🚀 Scheduler started successfully")
        
        # Log next run times
        jobs = scheduler.get_jobs()
        logger.info(f"\n📅 Scheduled Jobs ({len(jobs)} total):")
        for job in jobs:
            next_run = job.next_run_time
            logger.info(f"   • {job.name}")
            logger.info(f"     ID: {job.id}")
            logger.info(f"     Next run: {next_run}")
            logger.info(f"     Trigger: {job.trigger}")
    else:
        logger.warning("⚠️  Scheduler already running")


def stop_scheduler():
    """
    Stop the scheduler.
    
    Called during application shutdown (in lifespan).
    """
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.info("🛑 Scheduler stopped")
    else:
        logger.warning("⚠️  Scheduler not running")


def get_scheduler_status() -> dict:
    """
    Get scheduler status and job information.
    
    Returns:
        Dict with scheduler status, jobs, and next run times
    """
    jobs = scheduler.get_jobs()
    
    jobs_info = []
    for job in jobs:
        jobs_info.append({
            'id': job.id,
            'name': job.name,
            'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
            'trigger': str(job.trigger),
            'pending': job.pending
        })
    
    return {
        'running': scheduler.running,
        'total_jobs': len(jobs),
        'jobs': jobs_info
    }


async def trigger_job_now(job_id: str) -> dict:
    """
    Manually trigger a scheduled job immediately.
    
    Args:
        job_id: Job ID to trigger
    
    Returns:
        Dict with execution result
    
    Raises:
        ValueError: If job not found
    """
    job = scheduler.get_job(job_id)
    
    if not job:
        raise ValueError(f"Job '{job_id}' not found")
    
    logger.info(f"🔄 Manually triggering job: {job_id}")
    
    # Get the job function
    job_func = job.func
    
    # Execute the job
    result = await job_func()
    
    logger.info(f"✅ Manual job execution completed: {job_id}")
    
    return result
