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
    timezone='Asia/Kolkata',  # IST timezone
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
    
    This job runs every 4 hours (4AM, 8AM, 12PM, 4PM, 8PM, 12AM IST).
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


async def send_daily_slack_summary():
    """
    Scheduled task: Send daily morning update at 9:00 AM IST.
    
    Includes:
    - System status overview
    - Previous day's statistics (jobs, messages, channels)
    - Current account health
    - Issues to address
    """
    from app.utils.slack_notifier import slack_notifier
    from app.db.session import AsyncSessionLocal
    
    logger.info("☀️ Sending daily morning update to Slack...")
    
    try:
        async with AsyncSessionLocal() as db:
            success = await slack_notifier.send_morning_update(db)
            
            if success:
                logger.info("✅ Morning update sent successfully to Slack")
            else:
                logger.warning("⚠️ Failed to send morning update to Slack")
                
    except Exception as e:
        logger.error(f"❌ Failed to send morning update: {e}", exc_info=True)


async def run_channel_sync():
    """
    Scheduled task: Sync PostgreSQL telegram_groups ↔ MongoDB channels.
    
    Ensures Lambda functions have up-to-date channel list with health scores.
    Runs every 6 hours to keep channels synchronized.
    """
    from app.services.channel_sync_service import ChannelSyncService
    from app.db.session import AsyncSessionLocal
    
    logger.info("🔄 Running channel synchronization...")
    
    try:
        sync_service = ChannelSyncService()
        await sync_service.initialize()
        
        async with AsyncSessionLocal() as db:
            stats = await sync_service.sync_all_channels(db)
            logger.info(
                f"✅ Channel sync complete: "
                f"{stats['synced']}/{stats['total']} synced, "
                f"{stats['failed']} failed"
            )
            return stats
        
    except Exception as e:
        logger.error(f"❌ Channel sync failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


def setup_jobs():
    """
    Setup all scheduled jobs.
    
    Called during application startup to configure periodic tasks.
    """
    logger.info("⏰ Setting up scheduled jobs...")
    
    # Job 1: Telegram Scraper - Every 4 hours (4AM, 8AM, 12PM, 4PM, 8PM, 12AM IST)
    scraping_hours = getattr(settings, 'SCRAPING_HOURS', [4, 8, 12, 16, 20, 0])
    hour_str = ','.join(map(str, scraping_hours))
    
    scheduler.add_job(
        run_telegram_scraper,
        CronTrigger(hour=hour_str, minute=0),  # Run at specified hours IST
        id='telegram_scraper_4hourly',
        name='Telegram Scraper (Every 4 hours IST)',
        replace_existing=True
    )
    logger.info(f"   ✅ Added: telegram_scraper_4hourly (Hours: {hour_str} IST)")
    
    # Job 2: Daily Morning Update at 9:00 AM IST
    scheduler.add_job(
        send_daily_slack_summary,
        CronTrigger(hour=9, minute=0),  # 9:00 AM IST
        id='daily_morning_update',
        name='Daily Morning Update (9:00 AM IST)',
        replace_existing=True
    )
    logger.info("   ✅ Added: daily_morning_update (09:00 IST)")
    
    # Job 3: Channel Sync (PostgreSQL ↔ MongoDB) - Every 6 hours
    # Syncs telegram_groups (PostgreSQL) to channels (MongoDB) for Lambda access
    scheduler.add_job(
        run_channel_sync,
        IntervalTrigger(hours=6),
        id='channel_sync',
        name='Channel Sync (PostgreSQL ↔ MongoDB) - Every 6 hours',
        replace_existing=True
    )
    logger.info("   ✅ Added: channel_sync (Every 6 hours)")
    
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
