#!/usr/bin/env python3
"""
Record real-world student outcome feedback for daily shared jobs.

This supports the mandatory feedback loop: what was shared, who applied,
and what response/interview outcome happened.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
FEEDBACK_PATH = ROOT / "app" / "data" / "feedback" / "student_job_feedback.jsonl"

VALID_OUTCOMES = {"shared", "applied", "response", "interview", "rejected", "no_response"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_record(record: dict) -> None:
    FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FEEDBACK_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _print_summary(date_str: str) -> None:
    if not FEEDBACK_PATH.exists():
        print("No feedback file yet.")
        return
    counts = {k: 0 for k in VALID_OUTCOMES}
    student_seen = set()
    for line in FEEDBACK_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if str(row.get("date") or "") != date_str:
            continue
        outcome = str(row.get("outcome") or "")
        if outcome in counts:
            counts[outcome] += 1
        sid = str(row.get("student_id") or "").strip()
        if sid:
            student_seen.add(sid)
    print(f"feedback_date={date_str}")
    print(f"unique_students={len(student_seen)}")
    print("outcomes=", counts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Log student job-share feedback")
    parser.add_argument("--date", required=True, help="Feedback date in YYYY-MM-DD")
    parser.add_argument("--student-id", required=True, help="Student identifier")
    parser.add_argument("--job-url", required=True, help="Apply/job URL")
    parser.add_argument(
        "--outcome",
        required=True,
        choices=sorted(VALID_OUTCOMES),
        help="Feedback outcome state",
    )
    parser.add_argument("--job-title", default="", help="Optional job title")
    parser.add_argument("--source-domain", default="", help="Optional source domain")
    parser.add_argument("--notes", default="", help="Optional notes")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print summary for --date without adding a record.",
    )
    args = parser.parse_args()

    if args.summary_only:
        _print_summary(str(args.date))
        return 0

    record = {
        "logged_at_utc": _utc_now(),
        "date": str(args.date),
        "student_id": str(args.student_id),
        "job_url": str(args.job_url),
        "job_title": str(args.job_title or ""),
        "source_domain": str(args.source_domain or ""),
        "outcome": str(args.outcome),
        "notes": str(args.notes or ""),
    }
    _append_record(record)
    print("Feedback logged.")
    _print_summary(str(args.date))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
