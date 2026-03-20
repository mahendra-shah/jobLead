#!/usr/bin/env python3
"""
Import crawl-ready sources into MongoDB collection `job_board_sources`.

Reads:
  - app/data/crawl_ready_sources.json

Writes:
  - Mongo: job_board_sources (upsert by normalized URL)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService


def main() -> int:
    parser = argparse.ArgumentParser(description="Import crawl-ready sources into MongoDB")
    parser.add_argument(
        "--json-path",
        type=str,
        default="app/data/crawl_ready_sources.json",
        help="Path to crawl_ready_sources.json (default: app/data/crawl_ready_sources.json)",
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run HTTP health check during import (slower, more accurate)",
    )
    parser.add_argument(
        "--keep-stale",
        action="store_true",
        help="Do not delete sources that are no longer crawl-ready in the latest JSON",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Import only first N sources (for testing)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    json_path = Path(args.json_path)
    if not json_path.is_absolute():
        json_path = repo_root / json_path

    service = MongoJobBoardSourcesService()
    result = service.import_crawl_ready_sources_from_json(
        str(json_path),
        health_check=bool(args.health_check),
        delete_non_crawl_ready=not bool(args.keep_stale),
        limit=args.limit,
    )
    print("Import result:", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

