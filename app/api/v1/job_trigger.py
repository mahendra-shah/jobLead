"""API endpoint to trigger scheduled jobs immediately (bypassing scheduler wait)."""

from fastapi import APIRouter, HTTPException, Query
from enum import Enum
from pydantic import BaseModel
from app.core.scheduler import trigger_job_now

router = APIRouter()

class JobOption(str, Enum):
    Fetch = "Fetch"
    Telegram = "Telegram"
    Classify = "Classify"
    DrawMessage = "DrawMessage"
    JobBoard = "jobBoard"

class JobTriggerResponse(BaseModel):
    status: str
    job: str
    detail: str = ""

# Map API parameter to scheduler job_id
JOB_ID_MAP = {
    "Fetch": "telegram_scraper_4hourly",
    "Telegram": "telegram_scraper_4hourly",
    "Classify": "ml_processor_on_demand",
    "DrawMessage": "telegram_group_joiner_5hourly",
    "jobBoard": "job_board_daily_5am",
}

@router.get("/trigger", response_model=JobTriggerResponse, tags=["Job Trigger"], summary="Trigger a scheduled job immediately")
async def trigger_job(
    job: JobOption = Query(..., description="Job to trigger (dropdown)")
):
    job_param = job.value
    job_id = JOB_ID_MAP.get(job_param)
    if not job_id:
        raise HTTPException(status_code=400, detail=f"Unknown job: {job_param}")
    try:
        result = await trigger_job_now(job_id)
        status = str(result.get("status") or "started")
        detail = str(result.get("message") or f"Job '{job_param}' started.")
        return JobTriggerResponse(
            status=status,
            job=job_param,
            detail=detail,
        )
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {e}")
