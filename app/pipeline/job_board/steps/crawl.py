"""Step 1: crawl job-board sources and save jobs JSON artifact."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from app.pipeline.job_board.contracts import CrawlStepOutput


def _count_jobs_in_json(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return int(len(payload.get("jobs") or []))
    except Exception:
        return 0


def run_crawl_step(
    *,
    root: Path,
    source_offset: int,
    batch_size: int,
    max_jobs_per_source: int,
    popular_source_max_jobs: int = 10,
    source_request_delay: float = 0.0,
    source_request_jitter: float = 0.0,
    prefer_less_known_sources: bool = False,
    exclude_popular_sources: bool = False,
    focus_digital_marketing: bool = False,
    student_pipeline_only: bool = False,
    no_profile_filter: bool = False,
) -> CrawlStepOutput:
    """
    Crawl sources and emit a jobs JSON file.

    This step intentionally does NOT write to Mongo job_ingest;
    step 2 owns persistence into Mongo.
    """
    py = sys.executable
    batch_id = f"batch_{source_offset}_{batch_size}"
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    jobs_path = root / "app" / "data" / "jobs" / f"jobs_run_{run_ts}.json"
    jobs_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        py,
        "scripts/crawl_jobs_from_sources.py",
        "--from-mongo",
        "--max-sources",
        str(int(batch_size)),
        "--source-offset",
        str(int(source_offset)),
        "--max-jobs-per-source",
        str(int(max_jobs_per_source)),
        "--popular-source-max-jobs",
        str(int(popular_source_max_jobs)),
        "--source-request-delay",
        str(float(source_request_delay)),
        "--source-request-jitter",
        str(float(source_request_jitter)),
        "--crawl-batch-id",
        batch_id,
        "--out",
        str(jobs_path),
    ]
    if prefer_less_known_sources:
        cmd.append("--prefer-less-known-sources")
    if exclude_popular_sources:
        cmd.append("--exclude-popular-sources")
    if focus_digital_marketing:
        cmd.append("--focus-digital-marketing")
    if student_pipeline_only:
        cmd.append("--student-pipeline-only")
    if no_profile_filter:
        cmd.append("--no-profile-filter")

    r = subprocess.run(cmd, cwd=root)
    if r.returncode != 0:
        raise RuntimeError(f"crawl step failed with exit={r.returncode}")

    return CrawlStepOutput(
        batch_id=batch_id,
        source_offset=int(source_offset),
        jobs_json_path=jobs_path,
        jobs_count=_count_jobs_in_json(jobs_path),
    )
