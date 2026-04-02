"""
Application Scheduler - APScheduler Integration

Manages scheduled tasks for the FastAPI application.
Includes daily Telegram scraping and other periodic tasks.

Author: Backend Team
Date: 2026-02-10
"""

import asyncio
import json
import logging
import os
import fcntl
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import redis as redis_mod

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from app.config import settings
from app.utils.job_board_report import read_job_board_report, write_job_board_report
from app.utils.timezone import now_ist

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
JOB_BOARD_SOURCE_STATE_PATH = ROOT / "app" / "data" / "pipeline" / "source_yield_state.json"
JOB_BOARD_RUNNER_PATH = ROOT / "scripts" / "run_daily_ingest_automation.py"

_manual_trigger_tasks: Dict[str, asyncio.Task] = {}
_job_board_runner_task: Optional[asyncio.Task] = None
_job_board_process: Optional[subprocess.Popen] = None


def _clear_job_board_runner_task() -> None:
    """Clear finished JobBoard runner task reference."""
    global _job_board_runner_task
    _job_board_runner_task = None

# Scheduler lock state (prevents duplicate scheduler start across Gunicorn workers)
_scheduler_lock_fd = None
_scheduler_lock_acquired = False


def _read_json_file(path: Path) -> Dict[str, Any]:
    """Read a JSON file into a dictionary with safe fallback."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_utc_timestamp(value: str) -> Optional[datetime]:
    """Parse a UTC timestamp string into an aware datetime."""
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _collect_source_counts_since(
    started_at: datetime,
    ended_at: Optional[datetime] = None,
) -> Dict[str, int]:
    """Collect attempted/succeeded/failed source counts from source-yield state."""
    state = _read_json_file(JOB_BOARD_SOURCE_STATE_PATH)
    sources = state.get("sources") or {}

    attempted = 0
    succeeded = 0
    failed = 0
    total_jobs = 0

    for row in sources.values():
        if not isinstance(row, dict):
            continue
        run_at = _parse_utc_timestamp(str(row.get("last_run_at") or ""))
        if run_at is None:
            continue
        if run_at < started_at:
            continue
        if ended_at is not None and run_at > ended_at:
            continue

        attempted += 1
        jobs_count = int(row.get("last_jobs") or 0)
        total_jobs += max(0, jobs_count)
        if jobs_count > 0:
            succeeded += 1
        else:
            failed += 1

    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "jobs_total": total_jobs,
    }


def _collect_ml_counts() -> Dict[str, int]:
    """Collect ML status counts from Mongo job_ingest collection."""
    try:
        from app.services.mongodb_job_ingest_service import MongoJobIngestService

        counts = MongoJobIngestService().count_by_status()
    except Exception as exc:
        logger.warning("Failed to collect JobBoard ML counts: %s", exc)
        counts = {}

    return {
        "pending": int(counts.get("pending") or 0),
        "processing": int(counts.get("processing") or 0),
        "verified": int(counts.get("verified") or 0),
        "rejected": int(counts.get("rejected") or 0),
    }


def _count_job_board_rows() -> Optional[int]:
    """Count Postgres jobs rows with source='job_board'."""
    try:
        from sqlalchemy import create_engine, text

        local_db_url = os.getenv("LOCAL_DATABASE_URL")
        db_url = local_db_url or str(settings.DATABASE_URL)
        sync_url = db_url.replace("+asyncpg", "")
        sync_url = sync_url.replace("?ssl=require", "?sslmode=require")
        sync_url = sync_url.replace("&ssl=require", "&sslmode=require")

        engine = create_engine(sync_url, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM jobs WHERE source = 'job_board'")
                )
                return int(result.scalar() or 0)
        finally:
            engine.dispose()
    except Exception as exc:
        logger.warning("Failed to count Postgres job_board rows: %s", exc)
        return None


def _collect_sheets_export_count_since(started_at: datetime) -> int:
    """Read jobs_exported count from Redis sheets status after start time."""
    client = None
    try:
        client = redis_mod.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=3,
        )
        raw = client.get("sheets:last_export")
        if not raw:
            return 0
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return 0
        exported_at = _parse_utc_timestamp(str(payload.get("exported_at") or ""))
        if exported_at is None or exported_at < started_at:
            return 0
        return int(payload.get("jobs_exported") or 0)
    except Exception:
        return 0
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def _persist_job_board_report(report: Dict[str, Any]) -> None:
    """Persist latest JobBoard report in Redis."""
    write_job_board_report(redis_url=settings.REDIS_URL, report=report)


def _build_job_board_command() -> list[str]:
    """Build the default 5 AM all-day JobBoard command."""
    return [
        os.getenv("JOB_BOARD_PYTHON_BIN") or "python3",
        str(JOB_BOARD_RUNNER_PATH),
        "--all-day",
        "--spaced-batches",
        "12",
        "--batch-size",
        "12",
        "--sleep-min",
        "300",
        "--sleep-max",
        "600",
    ]


def _is_job_board_process_running() -> bool:
    """Check whether the JobBoard pipeline process is currently active."""
    if _job_board_process is None:
        return False
    return _job_board_process.poll() is None


async def _run_job_board_pipeline(
    *,
    run_id: str,
    trigger: str,
    command: list[str],
    started_at: datetime,
    postgres_before: Optional[int],
) -> None:
    """Execute JobBoard pipeline in background and persist completion report."""
    global _job_board_process

    log_dir = ROOT / "logs" / "job_board"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"job_board_{run_id}.log"

    report: Dict[str, Any] = {
        "run_id": run_id,
        "trigger": trigger,
        "status": "running",
        "started_at": started_at,
        "ended_at": None,
        "duration_seconds": None,
        "command": " ".join(command),
        "pid": None,
        "log_path": str(log_path),
        "source_counts": {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "jobs_total": 0,
        },
        "ml_counts": _collect_ml_counts(),
        "postgres_sync_count": 0,
        "sheets_export_count": 0,
        "last_error": None,
    }

    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(
                f"\n===== JobBoard Run {run_id} ({trigger}) "
                f"started {started_at.isoformat()} =====\n"
            )
            log_file.flush()

            _job_board_process = subprocess.Popen(
                command,
                cwd=ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )

        report["pid"] = _job_board_process.pid
        _persist_job_board_report(report)

        return_code = await asyncio.to_thread(_job_board_process.wait)
        ended_at = datetime.now(timezone.utc)

        postgres_after = _count_job_board_rows()
        postgres_delta = 0
        if postgres_before is not None and postgres_after is not None:
            postgres_delta = max(0, postgres_after - postgres_before)

        report.update(
            {
                "status": "success" if return_code == 0 else "failed",
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "source_counts": _collect_source_counts_since(started_at, ended_at),
                "ml_counts": _collect_ml_counts(),
                "postgres_sync_count": postgres_delta,
                "sheets_export_count": _collect_sheets_export_count_since(started_at),
                "last_error": (
                    None
                    if return_code == 0
                    else f"JobBoard pipeline exited with code {return_code}"
                ),
            }
        )
    except Exception as exc:
        ended_at = datetime.now(timezone.utc)
        report.update(
            {
                "status": "failed",
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "source_counts": _collect_source_counts_since(started_at, ended_at),
                "ml_counts": _collect_ml_counts(),
                "postgres_sync_count": 0,
                "sheets_export_count": _collect_sheets_export_count_since(started_at),
                "last_error": str(exc),
            }
        )
        logger.error("JobBoard pipeline run failed: %s", exc, exc_info=True)
    finally:
        _persist_job_board_report(report)
        _job_board_process = None


async def launch_job_board_pipeline(trigger: str) -> Dict[str, Any]:
    """Launch JobBoard ingest pipeline in background and return immediately."""
    global _job_board_runner_task

    if _is_job_board_process_running():
        latest = read_job_board_report(redis_url=settings.REDIS_URL)
        return {
            "status": "already_running",
            "job_id": "job_board_daily_5am",
            "message": "JobBoard pipeline is already running.",
            "run_id": latest.get("run_id"),
        }

    if _job_board_runner_task is not None and not _job_board_runner_task.done():
        latest = read_job_board_report(redis_url=settings.REDIS_URL)
        return {
            "status": "already_running",
            "job_id": "job_board_daily_5am",
            "message": "JobBoard runner task is already active.",
            "run_id": latest.get("run_id"),
        }

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc)
    command = _build_job_board_command()

    start_report: Dict[str, Any] = {
        "run_id": run_id,
        "trigger": trigger,
        "status": "starting",
        "started_at": started_at,
        "ended_at": None,
        "duration_seconds": None,
        "command": " ".join(command),
        "pid": None,
        "source_counts": {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "jobs_total": 0,
        },
        "ml_counts": _collect_ml_counts(),
        "postgres_sync_count": 0,
        "sheets_export_count": 0,
        "last_error": None,
    }
    _persist_job_board_report(start_report)

    postgres_before = _count_job_board_rows()
    _job_board_runner_task = asyncio.create_task(
        _run_job_board_pipeline(
            run_id=run_id,
            trigger=trigger,
            command=command,
            started_at=started_at,
            postgres_before=postgres_before,
        )
    )
    _job_board_runner_task.add_done_callback(lambda _t: _clear_job_board_runner_task())

    return {
        "status": "started",
        "job_id": "job_board_daily_5am",
        "message": "JobBoard pipeline started in background.",
        "run_id": run_id,
    }


async def _run_manual_job(job_id: str, job_func: Callable[..., Any]) -> None:
    """Execute a manually triggered job in background with safe logging."""
    try:
        if asyncio.iscoroutinefunction(job_func):
            await job_func()
        else:
            await asyncio.to_thread(job_func)
        logger.info("Manual job completed: %s", job_id)
    except Exception as exc:
        logger.error("Manual job failed: %s (%s)", job_id, exc, exc_info=True)


def _clear_manual_task(job_id: str) -> None:
    """Clear finished manual trigger task entry."""
    _manual_trigger_tasks.pop(job_id, None)


def _acquire_scheduler_lock() -> bool:
    """
    Acquire non-blocking process lock for scheduler startup.

    Returns:
        True if this process should run scheduler, False otherwise.
    """
    global _scheduler_lock_fd, _scheduler_lock_acquired

    if _scheduler_lock_acquired:
        return True

    lock_file_path = os.getenv("SCHEDULER_LOCK_FILE", "/tmp/placement_scheduler.lock")

    try:
        _scheduler_lock_fd = open(lock_file_path, "w")
        fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_fd.write(str(os.getpid()))
        _scheduler_lock_fd.flush()
        _scheduler_lock_acquired = True
        logger.info(f"🔐 Scheduler lock acquired: {lock_file_path} (pid={os.getpid()})")
        return True
    except OSError:
        logger.info(f"⏭️  Scheduler lock already held; skipping scheduler start in pid={os.getpid()}")
        if _scheduler_lock_fd:
            _scheduler_lock_fd.close()
            _scheduler_lock_fd = None
        return False


def _release_scheduler_lock() -> None:
    """Release scheduler process lock if held by this process."""
    global _scheduler_lock_fd, _scheduler_lock_acquired

    if not _scheduler_lock_acquired or _scheduler_lock_fd is None:
        return

    try:
        fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_UN)
    finally:
        _scheduler_lock_fd.close()
        _scheduler_lock_fd = None
        _scheduler_lock_acquired = False
        logger.info("🔓 Scheduler lock released")

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
        # exc_info=True does NOT work in APScheduler listeners (no active exception
        # context at this point).  The real traceback is in event.traceback.
        logger.error(
            f"❌ Job '{event.job_id}' failed: {event.exception}\n"
            f"{event.traceback}"
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
        
        # Log results — key names match what process_unprocessed_messages actually returns
        total_msg    = stats.get('total_messages', 0)
        jobs_created = stats.get('individual_jobs_created', 0)
        spam_rej     = stats.get('spam_rejected', 0)
        quality_fil  = stats.get('quality_filtered', 0)
        errors       = stats.get('errors', 0)
        duration_s   = stats.get('processing_time_ms', 0) / 1000
        logger.info(f"\n✅ ML processing completed:")
        logger.info(f"   Messages processed: {total_msg}")
        logger.info(f"   Jobs created:       {jobs_created}")
        logger.info(f"   Spam rejected:      {spam_rej}")
        logger.info(f"   Quality filtered:   {quality_fil}")
        logger.info(f"   Errors:             {errors}")
        logger.info(f"   Duration:           {duration_s:.2f}s")

        # Export today's jobs to Google Sheets — but only if something was actually
        # processed to avoid wasting Sheets API quota on the safety-net cron run
        # that fires after the inline ML trigger already handled everything.
        if total_msg > 0:
            try:
                await run_sheets_export()
            except Exception as sheets_err:
                logger.error(f"⚠️  Sheets export failed (non-fatal): {sheets_err}", exc_info=True)
        else:
            logger.info("   (no new messages — skipping Sheets export)")

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


async def run_job_board_daily_ingest() -> Dict[str, Any]:
    """Launch the JobBoard all-day ingest pipeline in background."""
    logger.info("🚀 Launching JobBoard daily ingest pipeline (background)")
    result = await launch_job_board_pipeline(trigger="scheduler")
    logger.info("JobBoard daily ingest launch status: %s", result.get("status"))
    return result


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
            result = sheets_service.export_daily_jobs(db, now_ist())
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
                    "exported_at": now_ist().isoformat(),
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

    # Job 5: JobBoard all-day ingest launcher at 5:00 AM IST
    scheduler.add_job(
        run_job_board_daily_ingest,
        CronTrigger(hour=5, minute=0, timezone='Asia/Kolkata'),
        id='job_board_daily_5am',
        name='Job Board Daily Ingest (05:00 IST)',
        replace_existing=True,
    )
    logger.info("   ✅ Added: job_board_daily_5am (05:00 IST)")
    
    logger.info("✅ All scheduled jobs configured")


def start_scheduler():
    """
    Start the scheduler.
    
    Called during application startup (in lifespan).
    """
    # Optional hard switch to disable scheduler in this process/container
    if os.getenv("ENABLE_SCHEDULER", "true").lower() not in {"1", "true", "yes"}:
        logger.info("⏸️  Scheduler disabled via ENABLE_SCHEDULER env var")
        return

    # Ensure only one process (e.g., one Gunicorn worker) starts APScheduler
    if not _acquire_scheduler_lock():
        return

    if not scheduler.running:
        setup_jobs()
        scheduler.start()
        logger.info("🚀 Scheduler started successfully")
        
        # Log next run times
        from app.utils.timezone import IST as ist
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

    _release_scheduler_lock()


def get_scheduler_status() -> dict:
    """
    Get scheduler status and job information.
    
    Returns:
        Dict with scheduler status, jobs, and next run times
    """
    jobs = scheduler.get_jobs()
    
    jobs_info = []
    from app.utils.timezone import IST as ist
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

    manual_only_jobs: Dict[str, Callable[..., Any]] = {
        "ml_processor_on_demand": run_ml_processor,
    }

    if job is None and job_id not in manual_only_jobs:
        raise ValueError(f"Job '{job_id}' not found")

    job_func = job.func if job is not None else manual_only_jobs[job_id]
    
    existing_task = _manual_trigger_tasks.get(job_id)
    if existing_task is not None and not existing_task.done():
        return {
            "status": "already_running",
            "job_id": job_id,
            "message": f"Job '{job_id}' is already running.",
        }

    logger.info(f"🔄 Manually triggering job in background: {job_id}")
    task = asyncio.create_task(_run_manual_job(job_id, job_func))
    _manual_trigger_tasks[job_id] = task
    task.add_done_callback(lambda _t, jid=job_id: _clear_manual_task(jid))

    return {
        "status": "started",
        "job_id": job_id,
        "message": f"Job '{job_id}' started in background.",
    }
