"""
Application Scheduler - APScheduler Integration

Manages scheduled tasks for the FastAPI application.
Includes daily Telegram scraping and other periodic tasks.

Author: Backend Team
Date: 2026-02-10
"""

import logging
from datetime import datetime
import pytz

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
    
    IMPORTANT: Automatically triggers ML processing after scraping completes.
    """
    from app.services.telegram_scraper_service import get_scraper_service
    
    logger.info("=" * 60)
    logger.info("TELEGRAM SCRAPER - SCHEDULED JOB")
    logger.info("=" * 60)
    
    scraper_result = None
    
    try:
        # Get scraper service
        scraper = get_scraper_service()
        
        # Initialize if needed
        await scraper.initialize()
        
        # Run scraping
        scraper_result = await scraper.scrape_all_channels()
        
        # Log results
        logger.info(f"\n✅ Telegram scraper completed successfully:")
        logger.info(f"   Total channels: {scraper_result['total_channels']}")
        logger.info(f"   Successful: {scraper_result['successful']}")
        logger.info(f"   Failed: {scraper_result['failed']}")
        logger.info(f"   Total messages: {scraper_result['total_messages']}")
        logger.info(f"   Duration: {scraper_result['duration_seconds']:.2f}s")
        
        # 🔥 AUTOMATICALLY TRIGGER ML PROCESSING if messages were fetched
        if scraper_result['total_messages'] > 0:
            logger.info("\n🤖 Auto-triggering ML pipeline to process new messages...")
            try:
                ml_result = await run_ml_processor()
                logger.info(f"✅ ML processing completed: {ml_result.get('jobs_created', 0)} jobs created")
            except Exception as ml_error:
                logger.error(f"⚠️  ML processing failed: {ml_error}", exc_info=True)
                # Don't fail the scraper job if ML fails
        else:
            logger.info("\nℹ️  No new messages to process")
        
        return scraper_result
    
    except Exception as e:
        logger.error(f"❌ Telegram scraper failed: {e}", exc_info=True)
        raise
    
    finally:
        logger.info("=" * 60)


async def run_ml_processor() -> dict:
    """
    Scheduled task: Run ML processor to classify and extract jobs from messages.
    
    Processes unprocessed messages from MongoDB:
    - Classifies messages (job vs non-job)
    - Extracts job details (title, company, location, skills, salary)
    - Applies quality scoring
    - Stores jobs in PostgreSQL
    
    Returns:
        Dict with processing statistics
    """
    from app.services.ml_processor_service import get_ml_processor
    
    logger.info("=" * 60)
    logger.info("ML PROCESSOR - SCHEDULED JOB")
    logger.info("=" * 60)
    
    try:
        # Get ML processor
        processor = get_ml_processor()
        
        # Process all unprocessed messages
        stats = processor.process_unprocessed_messages(
            limit=None,  # Process all
            min_confidence=0.6  # 60% confidence threshold
        )
        
        # Log results
        logger.info(f"\n✅ ML processing completed:")
        logger.info(f"   Messages processed: {stats.get('processed', 0)}")
        logger.info(f"   Jobs created: {stats.get('jobs_created', 0)}")
        logger.info(f"   Failed: {stats.get('failed', 0)}")
        logger.info(f"   Duration: {stats.get('duration_seconds', 0):.2f}s")

        # Export today's jobs to Google Sheets after every ML run
        try:
            await run_sheets_export()
        except Exception as sheets_err:
            logger.error(f"⚠️  Sheets export failed (non-fatal): {sheets_err}", exc_info=True)

        return stats

    except Exception as e:
        logger.error(f"❌ ML processor failed: {e}", exc_info=True)
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


async def run_telegram_group_joiner():
    """
    Scheduled task: Join Telegram groups (1 per account per cycle).
    
    Runs every 5 hours to gradually join channels.
    Early exits if no unjoined channels exist.
    """
    from app.services.telegram_group_joiner_service import TelegramGroupJoinerService
    
    logger.info("🔗 Starting Telegram group joiner cycle...")
    
    try:
        joiner = TelegramGroupJoinerService()
        result = await joiner.run_join_cycle()
        
        if result["success"]:
            stats = result["stats"]
            total_joined = stats["successful_joins"] + stats["already_joined"]
            
            logger.info(
                f"✅ Group joiner completed: {total_joined} joined, "
                f"{stats['failed_joins']} failed"
            )
            
            return result
        else:
            logger.warning(f"⚠️ Group joiner finished with issues: {result['message']}")
            return result
            
    except Exception as e:
        logger.error(f"❌ Group joiner failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


async def run_sheets_export() -> dict:
    """
    Export today's jobs to Google Sheets.
    Called automatically after every ML processing run.
    Creates a new tab per calendar day; idempotent (safe to run many times/day).
    """
    from app.config import settings
    from app.services.google_sheets_service import GoogleSheetsService
    from app.db.session import SyncSessionLocal

    if not settings.SHEET_ID:
        logger.info("📊 Sheets export skipped: SHEET_ID not configured")
        return {"status": "skipped"}

    logger.info("📊 Exporting today's jobs to Google Sheets...")
    try:
        sheets_service = GoogleSheetsService()
        db = SyncSessionLocal()
        try:
            result = sheets_service.export_daily_jobs(db, datetime.now())
            logger.info(
                f"✅ Sheets export: {result.get('jobs_exported', 0)} jobs → "
                f"tab '{result.get('tab_name', '?')}'"
            )
        finally:
            db.close()

        # Persist export status to Redis so the visibility dashboard can report it
        try:
            import json
            import redis as _redis
            _r = _redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=5)
            _r.setex(
                "sheets:last_export",
                90000,  # TTL: 25 h (survives into the next day until the first new export)
                json.dumps({
                    **result,
                    "exported_at": datetime.now().isoformat(),
                }),
            )
            _r.close()
        except Exception as redis_err:
            logger.warning(f"⚠️ Could not persist sheets status to Redis: {redis_err}")

        return result
    except Exception as e:
        logger.error(f"❌ Google Sheets export failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


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
        CronTrigger(hour=hour_str, minute=0, timezone='Asia/Kolkata'),  # Run at specified hours IST
        id='telegram_scraper_4hourly',
        name='Telegram Scraper (Every 4 hours IST)',
        replace_existing=True
    )
    logger.info(f"   ✅ Added: telegram_scraper_4hourly (Hours: {hour_str} IST)")
    
    # Job 2: Daily Morning Update at 9:00 AM IST
    scheduler.add_job(
        send_daily_slack_summary,
        CronTrigger(hour=9, minute=0, timezone='Asia/Kolkata'),  # 9:00 AM IST
        id='daily_morning_update',
        name='Daily Morning Update (9:00 AM IST)',
        replace_existing=True
    )
    logger.info("   ✅ Added: daily_morning_update (09:00 IST)")
    
    # Job 3: ML Processor - 15 minutes after scraper (catches any missed messages)
    for hour in scraping_hours:
        scheduler.add_job(
            run_ml_processor,
            CronTrigger(hour=hour, minute=20, timezone='Asia/Kolkata'),  # 20 minutes after scraper IST
            id=f'ml_processor_after_scrape_{hour}h',
            name=f'ML Processor (AfterScrape {hour:02d}:20 IST)',
            replace_existing=True
        )
    logger.info(f"   ✅ Added: ML Processor jobs (20 min after each scrape)")
    
    # Job 4: Telegram Group Joiner - Every 5 hours
    scheduler.add_job(
        run_telegram_group_joiner,
        IntervalTrigger(hours=5),
        id='telegram_group_joiner_5hourly',
        name='Telegram Group Joiner (Every 5 hours)',
        replace_existing=True
    )
    logger.info("   ✅ Added: telegram_group_joiner_5hourly (Every 5 hours)")
    
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
        ist = pytz.timezone('Asia/Kolkata')
        jobs = scheduler.get_jobs()
        logger.info(f"\n📅 Scheduled Jobs ({len(jobs)} total):")
        for job in jobs:
            next_run = job.next_run_time
            next_run_ist = next_run.astimezone(ist) if next_run else None
            logger.info(f"   • {job.name}")
            logger.info(f"     ID: {job.id}")
            logger.info(f"     Next run: {next_run_ist.strftime('%Y-%m-%d %H:%M:%S IST') if next_run_ist else 'N/A'}")
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
    ist = pytz.timezone('Asia/Kolkata')
    for job in jobs:
        next_run_utc = job.next_run_time
        next_run_ist = next_run_utc.astimezone(ist) if next_run_utc else None
        jobs_info.append({
            'id': job.id,
            'name': job.name,
            'next_run_time_ist': next_run_ist.strftime('%Y-%m-%d %H:%M:%S IST') if next_run_ist else None,
            'next_run_time_utc': next_run_utc.isoformat() if next_run_utc else None,
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
