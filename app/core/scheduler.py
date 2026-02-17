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
    
    This job runs every 4 hours (4AM, 8AM, 12PM, 4PM, 8PM, 12AM).
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
    Scheduled task: Send daily Slack summary at 9:00 AM.
    
    Includes previous day's statistics:
    - Jobs scraped
    - Channels active
    - Account health
    - Error summary
    """
    from app.utils.slack_notifier import SlackNotifier
    from app.services.telegram_scraper_service import get_scraper_service
    from app.db.session import SyncSessionLocal
    from app.models.telegram_account import TelegramAccount, HealthStatus
    from app.models.job import Job
    from datetime import datetime, timedelta
    
    logger.info("📊 Generating daily Slack summary...")
    
    try:
        db = SyncSessionLocal()
        scraper = get_scraper_service()
        notifier = SlackNotifier()
        
        # Get yesterday's stats
        yesterday = datetime.utcnow() - timedelta(days=1)
        
        # Jobs scraped yesterday
        jobs_yesterday = db.query(Job).filter(
            Job.created_at >= yesterday
        ).count()
        
        # Account health
        accounts = db.query(TelegramAccount).all()
        healthy_accounts = sum(1 for a in accounts if a.health_status == HealthStatus.HEALTHY)
        banned_accounts = sum(1 for a in accounts if a.health_status == HealthStatus.BANNED)
        
        # MongoDB stats
        total_messages = 0
        unprocessed = 0
        if scraper._initialized:
            mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
            total_messages = mongo_db.raw_messages.count_documents({})
            unprocessed = mongo_db.raw_messages.count_documents({'is_processed': False})
        
        # Send summary
        summary_text = f"""
📊 *Daily Telegram Scraping Summary*
_Previous 24 hours: {yesterday.strftime('%Y-%m-%d')}_

*Jobs Scraped:* {jobs_yesterday} new jobs
*Messages Total:* {total_messages} (Unprocessed: {unprocessed})

*Account Health:*
• Healthy: {healthy_accounts}/{len(accounts)}
• Banned: {banned_accounts}/{len(accounts)}

*System Status:* {"✅ Operational" if healthy_accounts > 0 else "⚠️ Degraded"}
"""
        
        notifier.send_message(
            title="Daily Scraping Summary",
            message=summary_text,
            severity="info"
        )
        
        logger.info("✅ Daily Slack summary sent successfully")
        
        db.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to send daily Slack summary: {e}", exc_info=True)


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
    
    # Job 1: Telegram Scraper - Every 4 hours (4AM, 8AM, 12PM, 4PM, 8PM, 12AM)
    # Runs at: 4, 8, 12, 16, 20, 0 hours UTC (adjust for IST if needed)
    scraping_hours = getattr(settings, 'SCRAPING_HOURS', [4, 8, 12, 16, 20, 0])
    hour_str = ','.join(map(str, scraping_hours))
    
    scheduler.add_job(
        run_telegram_scraper,
        CronTrigger(hour=hour_str, minute=0),  # Run at specified hours
        id='telegram_scraper_4hourly',
        name='Telegram Scraper (Every 4 hours)',
        replace_existing=True
    )
    logger.info(f"   ✅ Added: telegram_scraper_4hourly (Hours: {hour_str} UTC)")
    
    # Job 2: Daily Slack Summary at 9:00 AM
    scheduler.add_job(
        send_daily_slack_summary,
        CronTrigger(hour=9, minute=0),  # 9:00 AM UTC
        id='daily_slack_summary',
        name='Daily Slack Summary (9:00 AM)',
        replace_existing=True
    )
    logger.info("   ✅ Added: daily_slack_summary (09:00 UTC)")
    
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
