import logging
from app.config import settings
from app.utils.timezone import now_ist
from sqlalchemy import create_engine
from scripts.send_matched_jobs_emails import main as send_emails_main

logger = logging.getLogger(__name__)

async def send_matched_jobs_emails_job():
    logger.info("[SCHEDULER] Running send_matched_jobs_emails_job (async wrapper)")
    # This is a sync script, so we just call it directly
    try:
        # Pass --max-jobs-per-student=10 to main
        import sys
        orig_argv = sys.argv.copy()
        sys.argv = [sys.argv[0], '--max-jobs-per-student=10']
        result = send_emails_main()
        sys.argv = orig_argv
        logger.info(f"[SCHEDULER] send_matched_jobs_emails.py finished with result: {result}")
        return result
    except Exception as exc:
        logger.error(f"[SCHEDULER] send_matched_jobs_emails.py failed: {exc}", exc_info=True)
        return None
