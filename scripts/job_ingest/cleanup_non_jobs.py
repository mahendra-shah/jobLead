#!/usr/bin/env python3
"""
Remove non-job / hub / spam rows from Mongo job_ingest, then optionally:
  - rewrite app/data/jobs/jobs_verified.json
  - replace today's Google Sheet jobs tab (no append)

Does NOT edit jobs_master.json or jobs_run_*.json (re-crawl or merge separately if needed).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.mongodb_job_ingest_service import MongoJobIngestService
from scripts.merge_job_runs import _is_non_job_or_spam

_URL_NAV_MARKERS = (
    "/jobs/culture",
    "/culture",
    "/how-we-operate",
    "/how_we_operate",
    "/life-at",
    "/life_at",
    "/benefits",
    "/working-here",
    "/working_here",
    "/diversity",
    "/about-us",
    "/about_us",
    "/university",
    "/students/",
    "/early-careers",
    "/early_careers",
)
_URL_BROKEN = ("/job/-/", "jobs/[tag", "[tag1]")
_TITLE_HUB = re.compile(
    r"^(remote|careers|jobs|how we operate|life at|benefits|open roles?|view all jobs)\s*$",
    re.I,
)


def should_remove(doc: dict) -> bool:
    p = doc.get("payload") if isinstance(doc.get("payload"), dict) else {}
    title = (p.get("title") or "").strip()
    url = (p.get("url") or p.get("apply_url") or "").strip()
    desc = (p.get("description") or "")[:8000]
    combined = f"{title} {desc} {url}"
    if _is_non_job_or_spam(title, url, combined):
        return True
    u = url.lower()
    if any(m in u for m in _URL_NAV_MARKERS):
        return True
    if any(m in u for m in _URL_BROKEN):
        return True
    if _TITLE_HUB.match(title.strip()):
        return True
    if re.search(r"/jobs?/remote/?$", u) and "/job/" not in u:
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge bad job_ingest docs + refresh verified JSON / sheet")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; do not delete")
    parser.add_argument(
        "--scope",
        choices=("all", "verified"),
        default="all",
        help="Scan all documents or only ml_status=verified",
    )
    parser.add_argument("--reexport-verified-json", action="store_true", help="Run export_verified_jobs_json.py after delete")
    parser.add_argument(
        "--refresh-sheet",
        action="store_true",
        help="After reexport, push jobs_verified.json to sheet WITHOUT append (replaces data rows)",
    )
    parser.add_argument(
        "--sheet-date",
        type=str,
        default=None,
        help="IST YYYY-MM-DD tab suffix (default: today's IST from exporter)",
    )
    args = parser.parse_args()

    svc = MongoJobIngestService()
    svc._ensure_indexes()
    col = svc._col
    assert col is not None

    query: dict = {}
    if args.scope == "verified":
        query["ml_status"] = "verified"

    to_delete: list[str] = []
    scanned = 0
    for doc in col.find(query, {"dedupe_key": 1, "payload": 1, "ml_status": 1}):
        scanned += 1
        if should_remove(doc):
            dk = doc.get("dedupe_key")
            if dk:
                to_delete.append(str(dk))

    print(f"Scanned={scanned} marked_for_removal={len(to_delete)} dry_run={args.dry_run}")
    if args.dry_run:
        for dk in to_delete[:30]:
            print(f"  would delete dedupe_key={dk[:16]}...")
        if len(to_delete) > 30:
            print(f"  ... and {len(to_delete) - 30} more")
        return 0

    deleted = 0
    if not to_delete:
        print("Nothing to delete.")
    else:
        chunk = 200
        for i in range(0, len(to_delete), chunk):
            part = to_delete[i : i + chunk]
            r = col.delete_many({"dedupe_key": {"$in": part}})
            deleted += int(r.deleted_count)
        print(f"Deleted {deleted} documents from job_ingest.")

    py = sys.executable
    if args.reexport_verified_json:
        r = subprocess.run([py, str(ROOT / "scripts/job_ingest/export_verified_jobs_json.py")], cwd=ROOT)
        if r.returncode != 0:
            return r.returncode

    if args.refresh_sheet:
        cmd = [
            py,
            str(ROOT / "scripts/export_job_board_jobs_to_sheets.py"),
            "--jobs-json",
            str(ROOT / "app/data/jobs/jobs_verified.json"),
        ]
        if args.sheet_date:
            cmd.extend(["--date", args.sheet_date])
        r = subprocess.run(cmd, cwd=ROOT)
        if r.returncode != 0:
            return r.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
