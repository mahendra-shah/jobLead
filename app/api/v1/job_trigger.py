"""API endpoint to trigger scheduled jobs immediately (bypassing scheduler wait)."""



import os
import signal
import subprocess
import sys
import logging
import threading
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from app.core.scheduler import trigger_job_now
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
JOB_BOARD_RUNNER = ROOT / "scripts" / "job_board_flow" / "run_daily_ingest_automation.py"
JOB_BOARD_LOG_DIR = ROOT / "logs"


def _jobboard_default_args() -> list[str]:
    """Single-batch safe defaults shared by manual trigger runs."""
    args = [
        "--batch-size",
        str(int(settings.JOB_BOARD_BATCH_SIZE)),
        "--max-jobs-per-source",
        str(int(settings.JOB_BOARD_MAX_JOBS_PER_SOURCE)),
        "--ml-limit",
        str(int(settings.JOB_BOARD_ML_LIMIT)),
        "--sync-limit",
        str(int(settings.JOB_BOARD_SYNC_LIMIT)),
        "--source-request-delay",
        str(float(settings.JOB_BOARD_SOURCE_REQUEST_DELAY)),
        "--source-request-jitter",
        str(float(settings.JOB_BOARD_SOURCE_REQUEST_JITTER)),
    ]
    if bool(settings.JOB_BOARD_STUDENT_PIPELINE_ONLY):
        args.append("--student-pipeline-only")
    if bool(settings.JOB_BOARD_NO_STRICT_INDIA):
        args.append("--no-strict-india")
    return args

# In-process state for the currently running manual JobBoard trigger.
_jobboard_proc: subprocess.Popen | None = None
_jobboard_log_path: Path | None = None
_jobboard_started_at: datetime | None = None
_jobboard_last_exit_code: int | None = None
_jobboard_last_finished_at: datetime | None = None
_jobboard_log_thread: threading.Thread | None = None

class JobOption(str, Enum):
    Fetch = "Fetch"
    Telegram = "Telegram"
    Classify = "Classify"
    DrawMessage = "DrawMessage"
    JobBoard = "JobBoard"

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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _jobboard_is_running() -> bool:
    return _jobboard_proc is not None and _jobboard_proc.poll() is None


def _tail_log(path: Path, max_lines: int = 80) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-max_lines:]


def _jobboard_status_payload(max_output_lines: int = 80) -> dict:
    running = _jobboard_is_running()
    pid = _jobboard_proc.pid if running and _jobboard_proc else None
    duration_seconds = None
    if running and _jobboard_started_at is not None:
        duration_seconds = int((_utc_now() - _jobboard_started_at).total_seconds())

    output_tail: list[str] = []
    if _jobboard_log_path is not None:
        output_tail = _tail_log(_jobboard_log_path, max_lines=max_output_lines)

    return {
        "running": running,
        "pid": pid,
        "started_at": _jobboard_started_at.isoformat() if _jobboard_started_at else None,
        "duration_seconds": duration_seconds,
        "last_exit_code": _jobboard_last_exit_code,
        "last_finished_at": _jobboard_last_finished_at.isoformat() if _jobboard_last_finished_at else None,
        "log_file": str(_jobboard_log_path) if _jobboard_log_path else None,
        "command": [sys.executable, "-u", str(JOB_BOARD_RUNNER), *_jobboard_default_args()],
        "output_tail": output_tail,
    }


def _start_jobboard_run() -> dict:
    global _jobboard_proc, _jobboard_log_path, _jobboard_started_at
    global _jobboard_last_exit_code, _jobboard_last_finished_at
    global _jobboard_log_thread

    JOB_BOARD_LOG_DIR.mkdir(parents=True, exist_ok=True)
    started_at = _utc_now()
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    log_path = JOB_BOARD_LOG_DIR / f"jobboard_manual_{stamp}.log"

    cmd = [sys.executable, "-u", str(JOB_BOARD_RUNNER), *_jobboard_default_args()]
    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"[{started_at.isoformat()}] START {' '.join(cmd)}\n")

    child_env = os.environ.copy()
    child_env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
        env=child_env,
    )

    def _drain_output(process: subprocess.Popen, output_path: Path) -> None:
        with output_path.open("a", encoding="utf-8") as sink:
            stream = process.stdout
            if stream is None:
                return
            for line in iter(stream.readline, ""):
                sink.write(line)
                sink.flush()
                msg = line.rstrip("\n")
                if msg:
                    logger.info("[jobboard-manual] %s", msg)
            stream.close()

    _jobboard_log_thread = threading.Thread(
        target=_drain_output,
        args=(proc, log_path),
        name="jobboard-manual-log-drain",
        daemon=True,
    )
    _jobboard_log_thread.start()

    _jobboard_proc = proc
    _jobboard_log_path = log_path
    _jobboard_started_at = started_at
    _jobboard_last_exit_code = None
    _jobboard_last_finished_at = None

    return {
        "running": True,
        "pid": proc.pid,
        "started_at": started_at.isoformat(),
        "log_file": str(log_path),
        "command": cmd,
    }


def _refresh_jobboard_completion_state() -> None:
    global _jobboard_last_exit_code, _jobboard_last_finished_at, _jobboard_proc
    if _jobboard_proc is None:
        return
    exit_code = _jobboard_proc.poll()
    if exit_code is None:
        return
    _jobboard_last_exit_code = exit_code
    _jobboard_last_finished_at = _utc_now()
    _jobboard_proc = None


@router.get("/status", tags=["Job Trigger"], summary="Get trigger job status")
async def get_trigger_job_status(
    job: JobOption = Query(..., description="Job to inspect"),
    output_lines: int = Query(80, ge=10, le=400, description="How many log lines to return for JobBoard"),
):
    job_param = job.value
    if job_param != JobOption.JobBoard.value:
        raise HTTPException(status_code=400, detail="Status endpoint currently supports job=JobBoard only")

    _refresh_jobboard_completion_state()
    return {
        "status": "success",
        "job": job_param,
        "detail": "JobBoard trigger status",
        "result": _jobboard_status_payload(max_output_lines=output_lines),
    }


@router.post("/stop", tags=["Job Trigger"], summary="Stop a running manual trigger job")
async def stop_trigger_job(
    job: JobOption = Query(..., description="Job to stop"),
):
    job_param = job.value
    if job_param != JobOption.JobBoard.value:
        raise HTTPException(status_code=400, detail="Stop endpoint currently supports job=JobBoard only")

    _refresh_jobboard_completion_state()
    if not _jobboard_is_running() or _jobboard_proc is None:
        return {
            "status": "success",
            "job": job_param,
            "detail": "JobBoard is not running",
            "result": _jobboard_status_payload(),
        }

    try:
        os.killpg(_jobboard_proc.pid, signal.SIGTERM)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to stop JobBoard run: {exc}") from exc

    return {
        "status": "success",
        "job": job_param,
        "detail": "Stop signal sent to JobBoard run",
        "result": _jobboard_status_payload(),
    }

@router.get("/trigger", response_model=JobTriggerResponse, tags=["Job Trigger"], summary="Trigger a scheduled job immediately")
async def trigger_job(
    job: JobOption = Query(..., description="Job to trigger (dropdown)")
):
    job_param = job.value

    if job_param == JobOption.JobBoard.value:
        _refresh_jobboard_completion_state()
        if _jobboard_is_running():
            return JobTriggerResponse(
                status="running",
                job=job_param,
                detail="JobBoard daily ingest is already running.",
                result=_jobboard_status_payload(),
            )

        try:
            started = _start_jobboard_run()
            return JobTriggerResponse(
                status="success",
                job=job_param,
                detail="JobBoard daily ingest started in background.",
                result=started,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to trigger JobBoard run: {e}")

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
