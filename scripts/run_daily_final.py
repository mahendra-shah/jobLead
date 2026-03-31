#!/usr/bin/env python3
"""
Final daily entry point — everything in one command after discussion:

  1) Same as scripts/run_daily_ingest_automation.py (single part or --all-day spaced):
     crawl → Mongo job_ingest → ML (+ India gate) → sync verified to Postgres → Sheet (append in pipeline)

  2) Optional: reapply_india_gate.py (strict India re-check in Mongo)

  3) Sync verified Mongo rows into Postgres jobs (unless --skip-postgres-sync)

  4) Student matching report: student_job_verification_<date>.md / .json

  5) Optional sheet refresh from Postgres (or JSON if you pass ``--sheet-from-json`` / fallback).

Date (--date) is IST calendar YYYY-MM-DD. If omitted, uses today's IST.

Examples
--------
  # One batch + reports + sheet for today's IST tab
  ./venv/bin/python3 scripts/run_daily_final.py

  # Workday: many parts (10–15 sources each), spaced, then final export + reports
  ./venv/bin/python3 scripts/run_daily_final.py --all-day

  # Target 26 Mar tab explicitly
  ./venv/bin/python3 scripts/run_daily_final.py --date 2026-03-26 --batch-size 15

  # Re-check India gate in Mongo before Postgres sync + sheet
  ./venv/bin/python3 scripts/run_daily_final.py --reapply-india-gate --date 2026-03-26

  # Forward any run_daily_ingest_automation flag (see that script)
  ./venv/bin/python3 scripts/run_daily_final.py --all-day --spaced-batches 12 --sleep-min 300 --sleep-max 600

Mongo-down note: ingest may fall back to JSON-only crawl (merge → sheet **already in step 1**). If
Postgres sync then fails, the student report uses ``jobs_master.json`` and **Step 5 is skipped** so we
do not wipe the sheet (a second JSON export uses ``replace`` and would drop rows appended in step 1).
Use ``--force-final-sheet`` if you really want Step 5 anyway.

Neon: the database name in your URL must exist on the server (often ``neondb``).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Final daily: ingest → Postgres sync → student report (Postgres) → sheet (Postgres)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="IST date YYYY-MM-DD for student report + optional final sheet export (default: today IST)",
    )
    parser.add_argument(
        "--reapply-india-gate",
        action="store_true",
        help="Run reapply_india_gate.py after pipeline, then sync_verified_to_postgres",
    )
    parser.add_argument(
        "--skip-postgres-sync",
        action="store_true",
        help="Do not run sync_verified_to_postgres.py",
    )
    parser.add_argument(
        "--no-student-report",
        action="store_true",
        help="Skip verify_jobs_for_students.py",
    )
    parser.add_argument(
        "--no-sheet-refresh",
        action="store_true",
        help="Skip final export_job_board_jobs_to_sheets.py (pipeline may already have appended)",
    )
    parser.add_argument(
        "--sheet-append",
        action="store_true",
        help="Append to <date>_jobs on final sheet export; default replaces data rows for a clean snapshot",
    )
    parser.add_argument(
        "--jobs-json",
        type=Path,
        default=Path("app/data/jobs/jobs_master.json"),
        help="JSON jobs file when using --sheet-from-json (fallback crawl path).",
    )
    parser.add_argument(
        "--student-report-jobs",
        type=Path,
        default=None,
        help="Optional JSON for student report; default reads job_board rows from Postgres for --date.",
    )
    parser.add_argument(
        "--sheet-from-json",
        action="store_true",
        help="Use JSON for final sheet export instead of Postgres (fallback mode).",
    )
    parser.add_argument(
        "--force-final-sheet",
        action="store_true",
        help="When Postgres sync failed, still run Step 5 (can replace <date>_jobs; default skips to preserve step-1 append).",
    )

    args, rest = parser.parse_known_args()
    py = sys.executable

    daily = [py, str(ROOT / "scripts/run_daily_ingest_automation.py")] + rest
    print(">>> Step 1/inject: run_daily_ingest_automation.py", " ".join(rest) if rest else "")
    r = subprocess.run(daily, cwd=ROOT)
    if r.returncode != 0:
        return r.returncode

    from app.utils.timezone import ist_today_utc_window

    date_str = args.date or ist_today_utc_window()[2]

    jobs_path = args.jobs_json
    if not jobs_path.is_absolute():
        jobs_path = ROOT / jobs_path

    student_jobs = args.student_report_jobs
    if student_jobs is not None and not student_jobs.is_absolute():
        student_jobs = ROOT / student_jobs

    if args.reapply_india_gate:
        print(">>> Step 2: reapply_india_gate.py")
        r2 = subprocess.run([py, str(ROOT / "scripts/job_ingest/reapply_india_gate.py")], cwd=ROOT)
        if r2.returncode != 0:
            return r2.returncode

    # Reapply changes Mongo — always sync to Postgres afterward. Otherwise honor --skip-postgres-sync.
    need_pg_sync = bool(args.reapply_india_gate) or not bool(args.skip_postgres_sync)
    postgres_sync_ok: bool | None = None
    if need_pg_sync:
        print(">>> Step 3: sync_verified_to_postgres.py")
        r3 = subprocess.run([py, str(ROOT / "scripts/job_ingest/sync_verified_to_postgres.py")], cwd=ROOT)
        postgres_sync_ok = r3.returncode == 0
        if not postgres_sync_ok:
            print(
                "WARNING: sync_verified_to_postgres failed (Mongo down, or Postgres URL/DB name wrong?). "
                "Student report will use jobs_master.json if available; Step 5 sheet export skipped "
                "(pipeline already wrote the sheet) unless you pass --force-final-sheet.",
                file=sys.stderr,
            )
    else:
        postgres_sync_ok = None

    use_json_not_postgres = postgres_sync_ok is False
    master_path = ROOT / "app/data/jobs/jobs_master.json"

    if not args.no_student_report:
        print(f">>> Step 4: verify_jobs_for_students.py --date {date_str}")
        vcmd = [
            py,
            str(ROOT / "scripts/verify_jobs_for_students.py"),
            "--date",
            date_str,
        ]
        if student_jobs is not None:
            vcmd.extend(["--jobs", str(student_jobs)])
        elif use_json_not_postgres:
            if master_path.exists():
                vcmd.extend(["--jobs", str(master_path)])
                print(
                    f">>> Student report: --jobs {master_path} (Postgres sync did not complete).",
                    file=sys.stderr,
                )
            else:
                print(
                    f"WARNING: expected {master_path} for JSON fallback but file is missing; "
                    "student report will query Postgres (may fail or be empty).",
                    file=sys.stderr,
                )
        r4 = subprocess.run(vcmd, cwd=ROOT)
        if r4.returncode != 0:
            return r4.returncode

    if not args.no_sheet_refresh:
        skip_final_sheet = use_json_not_postgres and not args.force_final_sheet
        if skip_final_sheet:
            print(
                ">>> Step 5: skipped export_job_board_jobs_to_sheets (JSON-fallback pipeline already updated the sheet; "
                "use --force-final-sheet to run it anyway).",
                file=sys.stderr,
            )
        else:
            cmd = [py, str(ROOT / "scripts/export_job_board_jobs_to_sheets.py"), "--date", date_str]
            sheet_from_json = bool(args.sheet_from_json or use_json_not_postgres)
            if sheet_from_json:
                cmd.extend(["--jobs-json", str(jobs_path)])
                if use_json_not_postgres and not args.sheet_from_json:
                    print(
                        f">>> Final sheet export: jobs JSON (not Postgres), --jobs-json {jobs_path}",
                        file=sys.stderr,
                    )
            else:
                cmd.append("--from-postgres")
            if args.sheet_append:
                cmd.append("--append-jobs")
            print(f">>> Step 5: export_job_board_jobs_to_sheets.py --date {date_str}")
            r5 = subprocess.run(cmd, cwd=ROOT)
            if r5.returncode != 0:
                return r5.returncode

    print(">>> Daily final run finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
