#!/usr/bin/env python3
"""
Pilot: run several crawl→ingest→ML→export cycles spaced apart to reduce blocking / rate limits.

Each iteration calls scripts/run_job_ingest_pipeline.py (Mongo sources + checkpoint).

Example (workday, random 3–8 min between batches):
  python3 scripts/pipeline/run_pilot_day_batches.py --iterations 12 --sleep-min 180 --sleep-max 480
"""

from __future__ import annotations

import argparse
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description="Spaced pilot batches: job_ingest pipeline")
    parser.add_argument("--iterations", type=int, default=8, help="Number of pipeline runs")
    parser.add_argument("--batch-size", type=int, default=12, help="Sources (resources) per batch")
    parser.add_argument("--source-request-delay", type=float, default=0.0, help="Extra per-request delay inside each source crawl")
    parser.add_argument("--source-request-jitter", type=float, default=0.0, help="Random 0..N per-request delay inside each source crawl")
    parser.add_argument(
        "--max-jobs-per-source",
        type=int,
        default=60,
        help="Cap job listings fetched per source (reduces rate-limit / timeout risk)",
    )
    parser.add_argument("--prefer-less-known-sources", action="store_true")
    parser.add_argument("--exclude-popular-sources", action="store_true")
    parser.add_argument("--focus-digital-marketing", action="store_true")
    parser.add_argument(
        "--student-pipeline-only",
        action="store_true",
        help="Only crawl student_pipeline_eligible sources (India/remote boards)",
    )
    parser.add_argument("--ml-limit", type=int, default=400)
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=120.0,
        help="Minimum seconds between successful batches",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=360.0,
        help="Maximum seconds between successful batches (random jitter)",
    )
    parser.add_argument(
        "--max-wall-minutes",
        type=float,
        default=0.0,
        help="Stop after this many wall-clock minutes (0 = no limit)",
    )
    parser.add_argument("--no-append-sheet", action="store_true")
    parser.add_argument("--no-strict-india", action="store_true")
    parser.add_argument(
        "--disable-mongo-fallback",
        action="store_true",
        help="Do not auto-switch to JSON-only crawl when Mongo is down.",
    )
    parser.add_argument(
        "--sleep-after-fail",
        type=float,
        default=60.0,
        help="Extra sleep after a failed iteration before retrying next",
    )
    args = parser.parse_args()

    py = sys.executable
    t0 = time.monotonic()

    for i in range(int(args.iterations)):
        if args.max_wall_minutes > 0:
            elapsed_min = (time.monotonic() - t0) / 60.0
            if elapsed_min >= args.max_wall_minutes:
                print(f"Stopping: max_wall_minutes={args.max_wall_minutes} reached.")
                break

        print(f"\n=== Pilot batch {i + 1}/{args.iterations} ===")
        cmd = [
            py,
            str(ROOT / "scripts/run_job_ingest_pipeline.py"),
            "--batch-size",
            str(args.batch_size),
            "--source-request-delay",
            str(args.source_request_delay),
            "--source-request-jitter",
            str(args.source_request_jitter),
            "--max-jobs-per-source",
            str(args.max_jobs_per_source),
            "--ml-limit",
            str(args.ml_limit),
        ]
        if not args.disable_mongo_fallback:
            cmd.append("--mongo-fallback-json")
        if args.prefer_less_known_sources:
            cmd.append("--prefer-less-known-sources")
        if args.exclude_popular_sources:
            cmd.append("--exclude-popular-sources")
        if args.focus_digital_marketing:
            cmd.append("--focus-digital-marketing")
        if args.student_pipeline_only:
            cmd.append("--student-pipeline-only")
        if not args.no_append_sheet:
            cmd.append("--append-sheet")
        if args.no_strict_india:
            cmd.append("--no-strict-india")

        r = subprocess.run(cmd, cwd=ROOT)
        if r.returncode != 0:
            print(f"Pipeline failed (exit {r.returncode}); sleeping {args.sleep_after_fail}s")
            time.sleep(float(args.sleep_after_fail))
            continue

        if i + 1 < int(args.iterations):
            gap = random.uniform(float(args.sleep_min), float(args.sleep_max))
            print(f"Sleeping {gap:.0f}s before next batch...")
            time.sleep(gap)

    print("Pilot day runner finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
