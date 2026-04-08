"""Step 2: persist crawled jobs into Mongo `job_ingest`."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.pipeline.job_board.contracts import MongoIngestStats
from app.services.mongodb_job_ingest_service import MongoJobIngestService


def _load_jobs(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("jobs") or []
    return [x for x in rows if isinstance(x, dict)]


def ingest_jobs_to_mongo(
    *,
    jobs_json_path: Path,
    crawl_batch_id: str,
    source_platform: str = "job_board",
) -> MongoIngestStats:
    stats = MongoIngestStats()
    if not jobs_json_path.exists():
        return stats

    jobs = _load_jobs(jobs_json_path)
    svc = MongoJobIngestService()
    for job in jobs:
        stats.attempted += 1
        try:
            svc.upsert_from_crawl(
                job,
                crawl_batch_id=crawl_batch_id,
                source_platform=source_platform,
            )
            stats.upserted += 1
        except Exception:
            stats.errors += 1
    return stats
