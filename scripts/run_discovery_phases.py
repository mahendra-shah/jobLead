#!/usr/bin/env python3
"""
Phase 1 Discovery Runner: Import discovery sources from seed JSON in phases.
Run until total reaches target (default 1000). Use --phase to run a single phase.

Usage:
  python scripts/run_discovery_phases.py                    # Run all phases
  python scripts/run_discovery_phases.py --phase 1          # Run only phase 1
  python scripts/run_discovery_phases.py --target 500        # Stop when 500 sources
  python scripts/run_discovery_phases.py --dry-run          # Show what would be imported
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, func
from app.db.session import get_sync_db
from app.models.discovery_source import DiscoverySource


SEED_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "app", "data", "discovery_sources_seed.json"
)


def load_seed(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_phase(session, sources: list, phase: int, dry_run: bool) -> tuple[int, int]:
    """Upsert sources for given phase. Returns (inserted, skipped)."""
    inserted, skipped = 0, 0
    for s in sources:
        if s.get("phase") != phase:
            continue
        url = (s.get("url") or "").strip()
        if not url:
            continue
        existing = session.execute(select(DiscoverySource).where(DiscoverySource.url == url).limit(1)).scalar_one_or_none()
        if existing:
            skipped += 1
            continue
        if dry_run:
            inserted += 1
            continue
        rec = DiscoverySource(
            name=(s.get("name") or url)[:500],
            url=url,
            source_type=s.get("source_type") or "website",
            platform=s.get("platform"),
            city=s.get("city"),
            region=s.get("region"),
            country_code=s.get("country_code"),
            phase=int(s.get("phase", 1)),
            is_shortlisted=False,
        )
        session.add(rec)
        inserted += 1
    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description="Run discovery phases from seed data")
    parser.add_argument("--phase", type=int, choices=[1, 2, 3, 4], help="Run only this phase")
    parser.add_argument("--target", type=int, default=1000, help="Target total sources (default 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument("--seed", type=str, default=SEED_PATH, help="Path to seed JSON")
    args = parser.parse_args()

    if not os.path.isfile(args.seed):
        print(f"❌ Seed file not found: {args.seed}")
        sys.exit(1)

    data = load_seed(args.seed)
    sources = data.get("sources") or []
    phases = args.phase if args.phase else [1, 2, 3, 4]
    if isinstance(phases, int):
        phases = [phases]

    print(f"📂 Loaded {len(sources)} sources from seed")
    if args.dry_run:
        print("🔍 DRY RUN — no DB changes")

    session = next(get_sync_db())
    try:
        # Current total
        total_before = session.scalar(select(func.count()).select_from(DiscoverySource)) or 0
        print(f"📊 Current discovery_sources count: {total_before}")

        for phase in phases:
            ins, sk = run_phase(session, sources, phase, args.dry_run)
            print(f"   Phase {phase}: +{ins} new, {sk} already present")
        if not args.dry_run:
            session.commit()

        total_after = session.scalar(select(func.count()).select_from(DiscoverySource)) or 0
        print(f"📊 Total after run: {total_after} (target: {args.target})")
        if total_after >= args.target:
            print("✅ Target reached.")
        else:
            print(f"   Add more seed data or run search-based discovery to reach {args.target}.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
