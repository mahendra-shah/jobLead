"""API endpoint to trigger scheduled jobs immediately (bypassing scheduler wait)."""



from fastapi import APIRouter, HTTPException, Query
from enum import Enum
from typing import Optional
from pydantic import BaseModel
from app.core.scheduler import trigger_job_now

router = APIRouter()

class JobOption(str, Enum):
    Fetch = "Fetch"
    Telegram = "Telegram"
    Classify = "Classify"
    DrawMessage = "DrawMessage"

class JobTriggerResponse(BaseModel):
    status: str
    job: str
    detail: str = ""
    result: dict | None = None

# Map API parameter to scheduler job_id
JOB_ID_MAP = {
    "Fetch": "telegram_scraper_4hourly",
    "Telegram": "telegram_scraper_4hourly",
    "Classify": "daily_morning_update",
    "DrawMessage": "channel_sync",
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
        return JobTriggerResponse(
            status="success",
            job=job_param,
            detail=f"Job '{job_param}' triggered successfully.",
            result=result if isinstance(result, dict) else None
        )
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {e}")
