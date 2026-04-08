#!/usr/bin/env python3
"""
Analyze jobs: Tech vs Non-tech, category, fresher, remote, experienced, digital marketing.

  Postgres:  ./venv/bin/python scripts/analyze_job_board_jobs.py [--source job_board] [--limit N]
  JSON file: ./venv/bin/python scripts/analyze_job_board_jobs.py --json app/data/jobs/jobs_master.json

JSON expects ``{"jobs": [ {...}, ... ]}`` or a top-level array (crawl export format).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import warnings
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import inspect, text

from app.services.job_board_ml.postgres_sync import build_sync_engine
from app.services.job_board_sheets_service import JobBoardSheetsService
from app.utils.job_parser import parse_experience

_DM = re.compile(
    r"\b(?:digital\s+marketing|performance\s+marketing|seo\b|sem\b|"
    r"social\s+media\s+marketing|content\s+marketing|growth\s+marketing)\b",
    re.I,
)


def _domain(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        return (urlparse(str(url)).netloc or "").lower()
    except Exception:
        return ""


def _exp_text(r: Dict[str, Any]) -> str:
    return str(
        r.get("experience")
        or r.get("experience_required")
        or ""
    )


def _blob(r: Dict[str, Any]) -> str:
    parts = [
        r.get("title"),
        r.get("description"),
        _exp_text(r),
        r.get("location"),
        r.get("company_name"),
    ]
    return " ".join(str(p or "") for p in parts).lower()


def _remote_like(r: Dict[str, Any]) -> bool:
    wt = f"{r.get('work_type') or ''} {r.get('job_type') or ''}".lower()
    if any(x in wt for x in ("remote", "hybrid", "wfh")):
        return True
    b = _blob(r)
    return bool(
        re.search(
            r"\b(remote|work\s+from\s+home|wfh|hybrid|work\s+from\s+anywhere)\b",
            b,
        )
    )


def _digital_marketing(r: Dict[str, Any], category: str) -> bool:
    if "marketing" in (category or "").lower():
        return True
    return bool(_DM.search(_blob(r)))


def _experienced_track(r: Dict[str, Any]) -> bool:
    if r.get("is_fresher") is True:
        return False
    p = parse_experience(_exp_text(r))
    mn = p.get("min")
    mx = p.get("max")
    try:
        if mn is not None and float(mn) >= 2.0:
            return True
        if mx is not None and float(mx) >= 3.0 and (mn is None or float(mn) >= 1.5):
            return True
    except (TypeError, ValueError):
        pass
    b = _blob(r)
    if re.search(r"\b(?:senior|lead|principal|staff)\s+(?:engineer|developer)\b", b):
        return True
    if re.search(r"\b(?:5|6|7|8|10)\s*\+\s*years?\b", b):
        return True
    return False


def _valid_row(r: Dict[str, Any]) -> bool:
    t = str(r.get("title") or "").strip()
    c = str(r.get("company_name") or "").strip()
    return bool(r.get("is_active")) and len(t) >= 3 and len(c) >= 2


def _normalize_json_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Map crawl JSON keys → same shape as Postgres row dict."""
    sen = str(job.get("seniority") or "")
    title = str(job.get("title") or "")
    desc = str(job.get("description") or "")[:8000]
    blob = f"{title} {sen} {desc}".lower()
    is_fresher = bool(
        job.get("is_fresher") is True
        or re.search(r"fresher|entry\s*level|internship|graduate|0\s*-\s*1\s*yrs?", blob, re.I)
    )
    return {
        "title": job.get("title"),
        "company_name": job.get("company_name") or job.get("company"),
        "description": job.get("description"),
        "location": job.get("location") or job.get("location_detail"),
        "source_url": job.get("apply_url") or job.get("url"),
        "is_active": True,
        "is_fresher": is_fresher,
        "work_type": job.get("work_type"),
        "job_type": job.get("job_type"),
        "experience": sen or job.get("experience") or "",
        "experience_required": job.get("experience_required") or "",
    }


def load_jobs_from_json(path: Path) -> List[Dict[str, Any]]:
    raw_text = path.read_text(encoding="utf-8")
    data = json.loads(raw_text)
    if isinstance(data, list):
        raw_jobs = data
    else:
        raw_jobs = data.get("jobs") or data.get("data") or []
    out: List[Dict[str, Any]] = []
    for j in raw_jobs:
        if isinstance(j, dict):
            out.append(_normalize_json_job(j))
    return out


def _select_columns(colset: set[str]) -> List[str]:
    want = [
        "title",
        "company_name",
        "description",
        "location",
        "source_url",
        "is_active",
        "is_fresher",
        "work_type",
        "job_type",
    ]
    out = [c for c in want if c in colset]
    for extra in ("experience", "experience_required"):
        if extra in colset and extra not in out:
            out.append(extra)
            break
    return out


def run_analysis(rows: Iterable[Dict[str, Any]], *, label: str) -> None:
    seg_c: Counter[str] = Counter()
    cat_c: Counter[str] = Counter()
    valid_n = 0
    fresher_n = 0
    remote_n = 0
    exp_n = 0
    dm_n = 0
    tech_n = 0
    nontech_n = 0
    unknown_seg = 0
    total = 0

    for r in rows:
        total += 1
        dom = _domain(r.get("source_url"))
        segment, category = JobBoardSheetsService._classify_job(
            str(r.get("title") or ""),
            dom,
        )
        seg_c[segment] += 1
        cat_c[category] += 1

        if segment == "Tech":
            tech_n += 1
        elif segment == "Non-tech":
            nontech_n += 1
        else:
            unknown_seg += 1

        if _valid_row(r):
            valid_n += 1
        if r.get("is_fresher") is True:
            fresher_n += 1
        if _remote_like(r):
            remote_n += 1
        if _experienced_track(r):
            exp_n += 1
        if _digital_marketing(r, category):
            dm_n += 1

    print("=" * 72)
    print(f"Job analysis — {label}")
    print(f"  (scanned {total} rows)")
    print("=" * 72)
    print()
    print("Segment (title + domain heuristic, same as sheet classifier)")
    print(f"  Tech:        {tech_n}")
    print(f"  Non-tech:    {nontech_n}")
    print(f"  Unknown:     {unknown_seg}")
    print()
    print("Validity (active + title≥3 chars + company≥2 chars)")
    print(f"  Valid:       {valid_n}")
    print(f"  Invalid-ish: {total - valid_n}")
    print()
    print("Signals (row can match multiple)")
    print(f"  Fresher (is_fresher or JSON heuristic): {fresher_n}")
    print(f"  Remote / hybrid / WFH (text+cols):       {remote_n}")
    print(f"  Experienced-heavy (heuristic):           {exp_n}")
    print(f"  Digital marketing (category+text):      {dm_n}")
    print()
    print("Top categories (sheet classifier)")
    for name, n in cat_c.most_common(15):
        print(f"  {n:5d}  {name}")
    print()
    print("Done.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze jobs in Postgres or JSON file.")
    ap.add_argument(
        "--json",
        type=Path,
        default=None,
        metavar="PATH",
        help="Analyze jobs from a JSON file (jobs[] or top-level array) instead of Postgres",
    )
    ap.add_argument("--source", type=str, default="job_board", help="jobs.source filter (Postgres only)")
    ap.add_argument("--limit", type=int, default=100_000, help="Max rows to scan (Postgres or JSON)")
    args = ap.parse_args()

    if args.json is not None:
        path = args.json
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if not path.is_file():
            print(f"File not found: {path}", file=sys.stderr)
            return 1
        rows = load_jobs_from_json(path)
        if args.limit < len(rows):
            rows = rows[: int(args.limit)]
        run_analysis(rows, label=f"JSON file {path.name}")
        return 0

    else:
        warnings.filterwarnings("ignore", message=".*Did not recognize type 'vector'.*")
        engine = build_sync_engine()
        insp = inspect(engine)
        try:
            colset = {c["name"] for c in insp.get_columns("jobs")}
        except Exception as e:
            print(f"Could not inspect jobs table: {e}", file=sys.stderr)
            return 1

        cols = _select_columns(colset)
        if "title" not in cols:
            print("jobs table has no title column — cannot analyze.", file=sys.stderr)
            return 1

        sql = f"SELECT {', '.join(cols)} FROM jobs WHERE source = :src LIMIT :lim"
        rows: List[Dict[str, Any]] = []
        with engine.connect() as conn:
            result = conn.execute(
                text(sql),
                {"src": args.source, "lim": int(args.limit)},
            )
            for row in result:
                rows.append(dict(row._mapping))

        run_analysis(rows, label=f"Postgres source={args.source!r} limit={args.limit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
