#!/usr/bin/env python3
"""Re-check ml_status=verified documents with strict India gate; demote failures to rejected."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.mongodb_job_ingest_service import MongoJobIngestService
from app.utils.india_job_gate import passes_india_relevance


def main() -> int:
    parser = argparse.ArgumentParser(description="Demote verified → rejected if India gate fails")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    svc = MongoJobIngestService()
    svc._ensure_indexes()
    col = svc._col
    assert col is not None

    now = datetime.now(timezone.utc)
    n = 0
    for doc in col.find({"ml_status": "verified"}, {"dedupe_key": 1, "payload": 1, "ml_scores": 1}):
        p = doc.get("payload") or {}
        if passes_india_relevance(p):
            continue
        n += 1
        if args.dry_run:
            continue
        scores = dict(doc.get("ml_scores") or {})
        scores["reason_profile"] = "failed_india_relevance_recheck"
        scores["india_recheck_at"] = now.isoformat()
        col.update_one(
            {"dedupe_key": doc.get("dedupe_key")},
            {"$set": {"ml_status": "rejected", "ml_scores": scores, "updated_at": now}},
        )

    print(f"verified_demoted={n} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
