#!/usr/bin/env python3
"""
Delete old job-board source documents from MongoDB `job_ingest`.

By default, this removes records where:
  - source_platform == "job_board"
  - created_at < now - 7 days
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.mongodb_job_ingest_service import MongoJobIngestService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete old MongoDB job_ingest docs for job-board sources"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Delete documents older than this many days (default: 7)",
    )
    parser.add_argument(
        "--source-platform",
        type=str,
        default="job_board",
        help='Filter by source_platform value (default: "job_board")',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print count, do not delete",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.days < 1:
        print("❌ --days must be >= 1")
        return 1

    svc = MongoJobIngestService()
    svc._ensure_indexes()
    col = svc._col
    assert col is not None

    cutoff = datetime.now(timezone.utc) - timedelta(days=int(args.days))
    query = {
        "source_platform": str(args.source_platform),
        "created_at": {"$lt": cutoff},
    }

    count = int(col.count_documents(query))
    print(
        f"Matched {count} documents in '{svc.collection_name}' "
        f"(source_platform={args.source_platform}, older_than_days={args.days})"
    )

    if args.dry_run:
        print("Dry run enabled. No documents deleted.")
        return 0

    if count == 0:
        print("No documents to delete.")
        return 0

    result = col.delete_many(query)
    print(f"✅ Deleted {int(result.deleted_count)} documents.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
