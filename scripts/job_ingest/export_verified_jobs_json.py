#!/usr/bin/env python3
"""Export Mongo job_ingest documents with ml_status=verified to a JSON file."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.mongodb_job_ingest_service import MongoJobIngestService


def main() -> int:
    parser = argparse.ArgumentParser(description="Export verified job_ingest rows to JSON")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("app/data/jobs/jobs_verified.json"),
        help="Output path (default: app/data/jobs/jobs_verified.json)",
    )
    parser.add_argument("--limit", type=int, default=50000, help="Max jobs to export")
    args = parser.parse_args()

    out = args.out
    if not out.is_absolute():
        out = PROJECT_ROOT / out

    svc = MongoJobIngestService()
    rows = svc.list_verified_payloads(limit=int(args.limit))

    clean_jobs: list[dict] = []
    for row in rows:
        job = {k: v for k, v in row.items() if not str(k).startswith("_")}
        ml = row.get("_ml_scores") or {}
        if ml:
            job["ml_verification"] = ml
        clean_jobs.append(job)

    payload = {
        "meta": {
            "exported_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total": len(clean_jobs),
            "source": "mongo_job_ingest",
        },
        "jobs": clean_jobs,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(clean_jobs)} verified jobs -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
