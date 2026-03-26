import logging
from app.config import settings
from app.utils.timezone import now_ist
from sqlalchemy import create_engine
from scripts.export_student_matches_to_sheet import main as export_sheet_main

logger = logging.getLogger(__name__)

async def export_student_matches_to_sheet_job():
    logger.info("[SCHEDULER] Running export_student_matches_to_sheet_job (async wrapper)")
    # This is a sync script, so we just call it directly
    try:
        # Pass --max-jobs-per-student=10 to main
        import sys
        orig_argv = sys.argv.copy()
        sys.argv = [sys.argv[0], '--max-jobs-per-student=10']
        result = export_sheet_main()
        sys.argv = orig_argv
        logger.info(f"[SCHEDULER] export_student_matches_to_sheet.py finished with result: {result}")
        return result
    except Exception as exc:
        logger.error(f"[SCHEDULER] export_student_matches_to_sheet.py failed: {exc}", exc_info=True)
        return None
