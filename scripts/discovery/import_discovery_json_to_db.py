"""
Import discovery sources from JSON (e.g. discovery_sources_test.json) into discovery_sources table.
Run after testing with JSON and when ready to use the DB.

Usage:
  cd jobLead && python scripts/discovery/import_discovery_json_to_db.py
  python scripts/discovery/import_discovery_json_to_db.py --file app/data/discovery_sources_test.json --dry-run
"""
import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from sqlalchemy import select
from app.db.session import get_sync_db
from app.models.discovery_source import DiscoverySource
from scripts.discovery.base import DISCOVERY_JSON_PATH, load_discovery_sources_json


def main():
    parser = argparse.ArgumentParser(description="Import discovery JSON into discovery_sources table")
    parser.add_argument("--file", type=Path, default=None, help="Input JSON (default: app/data/discovery_sources_test.json)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = parser.parse_args()
    path = (JOBLEAD_ROOT / args.file) if args.file else DISCOVERY_JSON_PATH
    if not path.is_absolute():
        path = JOBLEAD_ROOT / path

    if not path.exists():
        print(f"File not found: {path}")
        return 1

    sources = load_discovery_sources_json(path)
    if not sources:
        print("No sources in file.")
        return 0

    session = next(get_sync_db())
    inserted, skipped = 0, 0
    try:
        for s in sources:
            url = (s.get("url") or "").strip()
            if not url:
                continue
            existing = session.execute(select(DiscoverySource).where(DiscoverySource.url == url).limit(1)).scalar_one_or_none()
            if existing:
                skipped += 1
                continue
            if args.dry_run:
                inserted += 1
                continue
            meta = dict(s.get("metadata") or {})
            meta.setdefault("domain", s.get("domain"))
            meta.setdefault("confidence_score", s.get("confidence_score"))
            meta.setdefault("last_checked", s.get("last_checked"))
            meta.setdefault("status", s.get("status"))
            if s.get("country"):
                meta.setdefault("country", s.get("country"))
            rec = DiscoverySource(
                name=(s.get("name") or url)[:500],
                url=url,
                source_type=s.get("type") or s.get("source_type") or "website",
                platform=s.get("platform") or "web",
                city=s.get("city"),
                region=s.get("region") or s.get("country"),
                metadata_=meta,
                phase=int(s.get("phase", 1)),
                is_shortlisted=bool(s.get("confidence_score", 0) > 5),
            )
            session.add(rec)
            inserted += 1
        if not args.dry_run:
            session.commit()
    finally:
        session.close()

    print(f"Inserted: {inserted}, Skipped (already in DB): {skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
