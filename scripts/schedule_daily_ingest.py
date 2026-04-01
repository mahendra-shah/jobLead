#!/usr/bin/env python3
"""
Daily scheduler for job-ingest automation on EC2.

Runs once per day at a fixed local time (default: 06:00 Asia/Kolkata),
and triggers the existing daily pipeline in all-day mode with 12 batches.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PYTHON = ROOT / "venv" / "bin" / "python3"
RUNNER = ROOT / "scripts" / "run_daily_ingest_automation.py"


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
        "--all-day",
        "--spaced-batches",
        str(args.spaced_batches),
        "--batch-size",
        str(args.batch_size),
        "--max-jobs-per-source",
        str(args.max_jobs_per_source),
        "--ml-limit",
        str(args.ml_limit),
        "--sync-limit",
        str(args.sync_limit),
        "--sleep-min",
        str(args.sleep_min),
        "--sleep-max",
        str(args.sleep_max),
        "--no-append-sheet",
    ]
    if args.disable_mongo_fallback:
        cmd.append("--disable-mongo-fallback")
    if args.student_pipeline_only:
        cmd.append("--student-pipeline-only")
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Schedule run_daily_ingest_automation.py once daily on EC2",
    )
    parser.add_argument("--time", default="06:00", help="Daily local run time HH:MM (default: 06:00)")
    parser.add_argument("--timezone", default="Asia/Kolkata", help="IANA timezone (default: Asia/Kolkata)")
    parser.add_argument("--python-bin", type=Path, default=DEFAULT_PYTHON, help="Python interpreter path")
    parser.add_argument("--spaced-batches", type=int, default=12, help="Daily spaced batches count")
    parser.add_argument("--batch-size", type=int, default=12, help="Sources per batch")
    parser.add_argument("--max-jobs-per-source", type=int, default=40, help="Cap jobs per source")
    parser.add_argument("--ml-limit", type=int, default=600, help="Max ML rows per batch")
    parser.add_argument("--sync-limit", type=int, default=200, help="Max Postgres sync rows per batch")
    parser.add_argument("--sleep-min", type=float, default=300.0, help="Seconds between batches (min)")
    parser.add_argument("--sleep-max", type=float, default=600.0, help="Seconds between batches (max)")
    parser.add_argument("--disable-mongo-fallback", action="store_true", help="Fail if Mongo is unavailable")
    parser.add_argument("--student-pipeline-only", action="store_true", help="Use student eligible sources only")
    parser.add_argument("--run-now", action="store_true", help="Run immediately once, then continue scheduling")
    parser.add_argument("--once", action="store_true", help="Run only one scheduled execution, then exit")
    args = parser.parse_args()

    try:
        tz = ZoneInfo(args.timezone)
    except Exception as exc:
        print(f"Invalid timezone: {args.timezone} ({exc})", file=sys.stderr)
        return 2

    hour, minute = _parse_hhmm(args.time)
    command = _build_command(args)

    print("Scheduler started")
    print(f"Timezone         : {args.timezone}")
    print(f"Daily run time   : {args.time}")
    print(f"Pipeline command : {' '.join(command)}")

    if args.run_now:
        print("\n>>> Immediate run requested")
        result = subprocess.run(command, cwd=ROOT)
        print(f">>> Immediate run finished with code {result.returncode}")
        if args.once:
            return result.returncode

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
