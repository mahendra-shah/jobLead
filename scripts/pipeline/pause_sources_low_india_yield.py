#!/usr/bin/env python3
"""
Pause Mongo job_board_sources that rarely produce ML-verified (India-gated) jobs.

Uses stats from job_ingest grouped by payload.source_domain (fallback: source_ref.source_domain).
Paused sources are excluded from Phase-2 crawl (status != active).

Unpause domains that recover with --unpause-good (requires enough ingested + ratio).
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.mongodb_job_board_source_service import JOB_BOARD_SOURCES_COLLECTION
from app.services.mongodb_job_ingest_service import MongoJobIngestService
from app.config import settings
from pymongo import MongoClient


def _norm_domain(d: str) -> str:
    x = (d or "").strip().lower()
    if x.startswith("www."):
        return x[4:]
    return x


def _connect_sources_col():
    client = MongoClient(
        settings.MONGODB_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    client.admin.command("ping")
    return client[settings.MONGODB_DATABASE][JOB_BOARD_SOURCES_COLLECTION]


def _collect_ingest_stats() -> dict[str, dict[str, int]]:
    ingest = MongoJobIngestService()
    ingest._ensure_indexes()
    col = ingest._col
    assert col is not None

    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"ingested": 0, "verified": 0, "rejected": 0, "pending": 0, "processing": 0, "error": 0}
    )

    for doc in col.find(
        {},
        {"ml_status": 1, "payload": 1, "source_ref": 1},
    ):
        p = doc.get("payload") or {}
        ref = doc.get("source_ref") or {}
        raw = p.get("source_domain") or ref.get("source_domain") or ""
        dom = _norm_domain(str(raw))
        if not dom:
            continue
        ml_st = doc.get("ml_status") or "pending"
        stats[dom]["ingested"] += 1
        if ml_st in ("verified", "rejected", "pending", "processing", "error"):
            stats[dom][ml_st] += 1
        else:
            stats[dom]["rejected"] += 1

    return dict(stats)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pause job_board_sources with low India/ML yield")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--min-ingested",
        type=int,
        default=20,
        help="Minimum job_ingest rows for a domain before we may pause",
    )
    parser.add_argument(
        "--zero-verify-pause",
        type=int,
        default=20,
        help="Pause if verified==0 and ingested>=this (default 20)",
    )
    parser.add_argument(
        "--min-verify-ratio",
        type=float,
        default=0.06,
        help="Pause if verified/ingested < this (and ingested>=min-ingested)",
    )
    parser.add_argument(
        "--unpause-good",
        action="store_true",
        help="Set status=active for domains that meet ratio / have verified>0 with enough data",
    )
    parser.add_argument(
        "--unpause-min-ingested",
        type=int,
        default=15,
        help="With --unpause-good: need at least this many ingested to trust ratio",
    )
    parser.add_argument(
        "--unpause-min-ratio",
        type=float,
        default=0.08,
        help="With --unpause-good: verified/ingested >= this to reactivate",
    )
    args = parser.parse_args()

    stats = _collect_ingest_stats()
    sources = _connect_sources_col()
    now = datetime.now(timezone.utc)

    paused = 0
    unpaused = 0

    for dom, cnt in sorted(stats.items(), key=lambda x: -x[1]["ingested"]):
        ing = cnt["ingested"]
        ver = cnt["verified"]

        should_pause = False
        if ing >= int(args.zero_verify_pause) and ver == 0:
            should_pause = True
        if ing >= int(args.min_ingested) and ing > 0:
            if (ver / ing) < float(args.min_verify_ratio):
                should_pause = True

        should_unpause = False
        if args.unpause_good and ing >= int(args.unpause_min_ingested) and ing > 0:
            if ver > 0 and (ver / ing) >= float(args.unpause_min_ratio):
                should_unpause = True

        flt = {"$or": [{"domain": dom}, {"domain": f"www.{dom}"}]}

        if should_unpause and not should_pause:
            if args.dry_run:
                unpaused += 1
                print(f"DRY unpause {dom} ingested={ing} verified={ver} ratio={ver/ing:.3f}")
            else:
                r = sources.update_many(
                    flt,
                    {
                        "$set": {
                            "status": "active",
                            "updated_at": now,
                            "metadata.india_yield_last_eval": now.isoformat(),
                            "metadata.india_yield_stats": dict(cnt),
                            "metadata.paused_reason": None,
                        }
                    },
                )
                unpaused += int(r.modified_count)

        elif should_pause:
            if args.dry_run:
                paused += 1
                print(
                    f"DRY pause {dom} ingested={ing} verified={ver} ratio={(ver/ing if ing else 0):.3f}"
                )
            else:
                r = sources.update_many(
                    flt,
                    {
                        "$set": {
                            "status": "paused",
                            "updated_at": now,
                            "metadata.india_yield_last_eval": now.isoformat(),
                            "metadata.india_yield_stats": dict(cnt),
                            "metadata.paused_reason": "low_india_job_yield",
                        }
                    },
                )
                paused += int(r.modified_count)

    print(f"domains_tracked={len(stats)} paused_updates={paused} unpaused_updates={unpaused} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
