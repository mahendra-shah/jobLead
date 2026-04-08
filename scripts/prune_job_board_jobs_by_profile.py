#!/usr/bin/env python3
"""
Remove Postgres job_board rows that fail current depth profile + India gates (same family as ML step).

Use after tightening rules so daily sheets are not full of legacy rows.

  # Dry-run rows for one IST calendar day (matches sheet date filter)
  ./venv/bin/python scripts/prune_job_board_jobs_by_profile.py --ist-date 2026-04-08 --dry-run

  # Delete + replace sheet tab from Postgres
  ./venv/bin/python scripts/prune_job_board_jobs_by_profile.py --ist-date 2026-04-08 --reexport-sheet

  # All job_board rows (no date filter) — careful
  ./venv/bin/python scripts/prune_job_board_jobs_by_profile.py --all-job-board --dry-run
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings  # noqa: E402
from app.services.job_board_ml.depth_profile import evaluate_depth_profile  # noqa: E402
from app.utils.india_job_gate import passes_india_relevance  # noqa: E402
from app.utils.timezone import ist_today_utc_window  # noqa: E402


def _sync_url() -> str:
    local = os.getenv("LOCAL_DATABASE_URL")
    if local:
        return local
    u = str(settings.DATABASE_URL).replace("+asyncpg", "")
    u = u.replace("?ssl=require", "?sslmode=require")
    u = u.replace("&ssl=require", "&sslmode=require")
    return u


def _norm_location_type(job_type: Any, work_type: Any) -> str:
    raw = f"{job_type or ''} {work_type or ''}".strip().lower()
    if any(x in raw for x in ("remote", "wfh", "work from home")):
        return "Remote"
    if "hybrid" in raw:
        return "Hybrid"
    if any(x in raw for x in ("office", "onsite", "on-site")):
        return "Onsite"
    return ""


def _row_to_payload(row: Dict[str, Any]) -> Tuple[dict, dict]:
    title = row.get("title") or ""
    desc = (row.get("description") or row.get("raw_text") or "") or ""
    company = row.get("company_name") or ""
    loc = row.get("location") or ""
    src = (row.get("source_url") or "") or ""
    exp = (
        row.get("experience")
        or row.get("experience_required")
        or ""
    )
    if isinstance(exp, str):
        exp_s = exp
    else:
        exp_s = str(exp) if exp is not None else ""

    payload = {
        "title": title,
        "company": company,
        "description": desc,
        "location": loc,
        "location_detail": loc,
        "experience": exp_s,
        "experience_raw": exp_s,
        "experience_text": exp_s,
        "url": src,
        "apply_url": src,
    }
    india_job = {
        "title": title,
        "description": desc,
        "location": loc,
        "location_detail": loc,
        "country": (row.get("country") or "") if isinstance(row.get("country"), str) else "",
        "url": src,
        "apply_url": src,
        "location_type": _norm_location_type(row.get("job_type"), row.get("work_type")),
    }
    return payload, india_job


def _evaluate_row(row: Dict[str, Any], *, strict_india: bool) -> Tuple[bool, str]:
    payload, india_job = _row_to_payload(row)
    ok_d, reason, _det = evaluate_depth_profile(
        payload,
        max_fresher_years=float(settings.MAX_FRESHER_EXPERIENCE_YEARS),
        require_remote_signal=bool(settings.JOB_BOARD_REQUIRE_REMOTE_SIGNAL),
        require_role_track=bool(settings.JOB_BOARD_REQUIRE_ROLE_TRACK_MATCH),
    )
    if not ok_d:
        return False, f"depth:{reason}"
    if strict_india and not passes_india_relevance(india_job):
        return False, "failed_india_relevance"
    return True, ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Prune job_board Postgres rows failing depth + India gates.")
    ap.add_argument("--ist-date", type=str, default="", metavar="YYYY-MM-DD", help="IST day: same window as sheet export.")
    ap.add_argument(
        "--all-job-board",
        action="store_true",
        help="Scan all rows with source=job_board (ignores --ist-date).",
    )
    ap.add_argument("--postgres-source", type=str, default="job_board")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-strict-india", action="store_true")
    ap.add_argument(
        "--reexport-sheet",
        action="store_true",
        help="After delete, run export_job_board_jobs_to_sheets.py --from-postgres for --ist-date (no append).",
    )
    args = ap.parse_args()

    if not args.all_job_board and not args.ist_date.strip():
        print("Provide --ist-date YYYY-MM-DD or --all-job-board.", file=sys.stderr)
        return 1
    if args.all_job_board and args.ist_date.strip():
        print("Use either --all-job-board or --ist-date, not both.", file=sys.stderr)
        return 1

    ist_date = args.ist_date.strip()
    start_utc = end_utc = None
    if not args.all_job_board:
        ref = datetime.strptime(ist_date, "%Y-%m-%d")
        start_utc, end_utc, _ = ist_today_utc_window(ref)

    eng = create_engine(
        _sync_url(),
        pool_pre_ping=True,
        connect_args={"connect_timeout": int(os.getenv("PG_CONNECT_TIMEOUT", "15"))},
    )

    cols_needed = [
        "id",
        "title",
        "company_name",
        "description",
        "raw_text",
        "location",
        "source_url",
        "source",
        "job_type",
        "work_type",
        "experience",
        "experience_required",
    ]
    ids_remove: List[Any] = []
    counts = Counter()

    try:
        with eng.connect() as conn:
            insp = inspect(conn)
            job_cols = {c["name"] for c in insp.get_columns("jobs")}
            select_parts = ["id"] + [c for c in cols_needed[1:] if c in job_cols]
            if "source" not in job_cols:
                print("jobs.source column missing; abort.", file=sys.stderr)
                return 1

            sql_sel = f"SELECT {', '.join(select_parts)} FROM jobs WHERE source = :src"
            params: Dict[str, Any] = {"src": args.postgres_source}
            if not args.all_job_board:
                if "created_at" in job_cols and "updated_at" in job_cols:
                    sql_sel += (
                        " AND ( (created_at >= :su AND created_at < :eu) "
                        "OR (updated_at >= :su AND updated_at < :eu) )"
                    )
                    params["su"] = start_utc
                    params["eu"] = end_utc
                elif "created_at" in job_cols:
                    sql_sel += " AND created_at >= :su AND created_at < :eu"
                    params["su"] = start_utc
                    params["eu"] = end_utc
                elif "updated_at" in job_cols:
                    sql_sel += " AND updated_at >= :su AND updated_at < :eu"
                    params["su"] = start_utc
                    params["eu"] = end_utc
                else:
                    print("No created_at/updated_at on jobs; use --all-job-board.", file=sys.stderr)
                    return 1

            rows = conn.execute(text(sql_sel), params).mappings().all()
            strict_india = not bool(args.no_strict_india)

            for row in rows:
                rdict = dict(row)
                ok, reason = _evaluate_row(rdict, strict_india=strict_india)
                counts["scanned"] += 1
                if ok:
                    counts["kept"] += 1
                else:
                    counts["remove"] += 1
                    counts[f"drop::{reason}"] += 1
                    ids_remove.append(rdict["id"])

            print(
                f"Prune: scanned={counts['scanned']} keep={counts['kept']} remove={counts['remove']} "
                f"dry_run={args.dry_run}",
                flush=True,
            )
            for k, v in sorted(counts.items()):
                if k.startswith("drop::"):
                    print(f"  {k}: {v}", flush=True)

            if not args.dry_run and ids_remove:
                for iid in ids_remove:
                    conn.execute(text("DELETE FROM jobs WHERE id = :id"), {"id": iid})
                conn.commit()
                print(f"Deleted {len(ids_remove)} job row(s).", flush=True)
            elif args.dry_run and ids_remove:
                print(f"Dry-run: would delete {len(ids_remove)} row(s).", flush=True)

    except OperationalError as e:
        print(f"Postgres error: {e}", file=sys.stderr)
        eng.dispose()
        return 1
    finally:
        eng.dispose()

    if args.reexport_sheet and not args.dry_run:
        if not ist_date:
            print("--reexport-sheet requires --ist-date.", file=sys.stderr)
            return 1
        cmd = [
            sys.executable,
            "scripts/export_job_board_jobs_to_sheets.py",
            "--from-postgres",
            "--date",
            ist_date,
            "--postgres-source",
            str(args.postgres_source),
        ]
        print("Re-export:", " ".join(cmd), flush=True)
        return int(subprocess.run(cmd, cwd=PROJECT_ROOT).returncode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
