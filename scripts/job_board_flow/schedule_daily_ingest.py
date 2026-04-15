#!/usr/bin/env python3
"""
Scheduler for job-ingest automation on EC2.

Default mode runs one batch every 30 minutes.
Optional daily mode is available with --every-minutes 0 and --time HH:MM.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_PYTHON = ROOT / "venv" / "bin" / "python3"
RUNNER = ROOT / "scripts" / "job_board_flow" / "run_daily_ingest_automation.py"


def _parse_hhmm(value: str) -> tuple[int, int]:
    parts = (value or "").strip().split(":")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("time must be HH:MM")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError("time must be HH:MM") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise argparse.ArgumentTypeError("time must be HH:MM")
    return hour, minute


def _next_run(now_local: datetime, hour: int, minute: int) -> datetime:
    candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return candidate


def _build_command(args: argparse.Namespace) -> list[str]:
    cmd = [
        str(args.python_bin),
        str(RUNNER),
        "--batch-size",
        str(args.batch_size),
        "--max-jobs-per-source",
        str(args.max_jobs_per_source),
        "--ml-limit",
        str(args.ml_limit),
        "--sync-limit",
        str(args.sync_limit),
        "--source-request-delay",
        str(args.source_request_delay),
        "--source-request-jitter",
        str(args.source_request_jitter),
    ]
    if args.disable_mongo_fallback:
        cmd.append("--disable-mongo-fallback")
    if args.mongo_fallback_json:
        cmd.append("--mongo-fallback-json")
    if args.student_pipeline_only:
        cmd.append("--student-pipeline-only")
    if args.no_strict_india:
        cmd.append("--no-strict-india")
    if args.no_append_sheet:
        cmd.append("--no-append-sheet")
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Schedule run_daily_ingest_automation.py as single batches",
    )
    parser.add_argument(
        "--every-minutes",
        type=float,
        default=30.0,
        help="Run one batch every N minutes (default: 30). Set 0 to use daily --time mode.",
    )
    parser.add_argument("--time", default="06:00", help="Daily local run time HH:MM (default: 06:00)")
    parser.add_argument("--timezone", default="Asia/Kolkata", help="IANA timezone (default: Asia/Kolkata)")
    parser.add_argument("--python-bin", type=Path, default=DEFAULT_PYTHON, help="Python interpreter path")
    parser.add_argument("--batch-size", type=int, default=12, help="Sources per batch")
    parser.add_argument("--max-jobs-per-source", type=int, default=40, help="Cap jobs per source")
    parser.add_argument("--ml-limit", type=int, default=500, help="Max ML rows per batch")
    parser.add_argument("--sync-limit", type=int, default=120, help="Max Postgres sync rows per batch")
    parser.add_argument(
        "--source-request-delay",
        type=float,
        default=1.8,
        help="Base delay between source requests (seconds)",
    )
    parser.add_argument(
        "--source-request-jitter",
        type=float,
        default=1.0,
        help="Random extra delay 0..N seconds per request",
    )
    parser.add_argument("--disable-mongo-fallback", action="store_true", help="Fail if Mongo is unavailable")
    parser.add_argument("--mongo-fallback-json", action="store_true", help="Allow JSON fallback when Mongo is unavailable")
    parser.add_argument("--student-pipeline-only", action="store_true", help="Use student eligible sources only")
    parser.add_argument("--no-strict-india", action="store_true", help="Disable strict India gate in ML stage")
    parser.add_argument("--no-append-sheet", action="store_true", help="Skip appending to Google Sheets")
    parser.add_argument("--run-now", action="store_true", help="Run immediately once, then continue scheduling")
    parser.add_argument("--once", action="store_true", help="Run only one scheduled execution, then exit")
    args = parser.parse_args()

    try:
        tz = ZoneInfo(args.timezone)
    except Exception as exc:
        print(f"Invalid timezone: {args.timezone} ({exc})", file=sys.stderr)
        return 2

    command = _build_command(args)
    interval_seconds = max(60.0, float(args.every_minutes) * 60.0)

    print("Scheduler started")
    print(f"Timezone         : {args.timezone}")
    if float(args.every_minutes) > 0:
        print(f"Mode             : interval ({args.every_minutes} minutes)")
    else:
        print(f"Mode             : daily at {args.time}")
    print(f"Pipeline command : {' '.join(command)}")

    if args.run_now:
        print("\n>>> Immediate run requested")
        result = subprocess.run(command, cwd=ROOT)
        print(f">>> Immediate run finished with code {result.returncode}")
        if args.once:
            return result.returncode

    if float(args.every_minutes) > 0:
        while True:
            print(f"\nSleeping {int(interval_seconds)} seconds before next batch...")
            time.sleep(interval_seconds)
            start_local = datetime.now(tz)
            print(f"\n>>> Starting scheduled run at {start_local.isoformat(timespec='seconds')}")
            result = subprocess.run(command, cwd=ROOT)
            end_local = datetime.now(tz)
            print(f">>> Scheduled run finished at {end_local.isoformat(timespec='seconds')} code={result.returncode}")

            if args.once:
                return result.returncode

    hour, minute = _parse_hhmm(args.time)

    while True:
        now_local = datetime.now(tz)
        next_local = _next_run(now_local, hour, minute)
        wait_seconds = max(1.0, (next_local - now_local).total_seconds())

        print(f"\nNow   : {now_local.isoformat(timespec='seconds')}")
        print(f"Next  : {next_local.isoformat(timespec='seconds')}")
        print(f"Sleep : {int(wait_seconds)} seconds")
        time.sleep(wait_seconds)

        start_local = datetime.now(tz)
        print(f"\n>>> Starting scheduled run at {start_local.isoformat(timespec='seconds')}")
        result = subprocess.run(command, cwd=ROOT)
        end_local = datetime.now(tz)
        print(f">>> Scheduled run finished at {end_local.isoformat(timespec='seconds')} code={result.returncode}")

        if args.once:
            return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
