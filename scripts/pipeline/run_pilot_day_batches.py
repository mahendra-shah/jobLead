#!/usr/bin/env python3
"""
Pilot: run several crawl→ingest→ML→export cycles spaced apart to reduce blocking / rate limits.

Each iteration calls scripts/run_job_ingest_pipeline.py (Mongo sources + checkpoint).

Example (workday, random 3–8 min between batches):
  python3 scripts/pipeline/run_pilot_day_batches.py --iterations 12 --sleep-min 180 --sleep-max 480
"""

from __future__ import annotations

import argparse
import json
import math
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STATE_PATH = ROOT / "app" / "data" / "pipeline" / "crawl_batch_state.json"


def _resolve_dynamic_iterations(*, batch_size: int, student_pipeline_only: bool) -> int:
    from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService

    svc = MongoJobBoardSourcesService()
    total = int(
        svc.count_crawl_ready_active(
            student_pipeline_priority=True,
            student_pipeline_only=bool(student_pipeline_only),
        )
        or 0
    )
    offset = 0
    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            offset = int(state.get("source_offset") or 0)
        except Exception:
            offset = 0
    if total <= 0:
        return 0
    if offset >= total:
        offset = 0
    remaining = max(0, total - offset)
    return int(math.ceil(float(remaining) / float(max(1, batch_size))))


def main() -> int:
    parser = argparse.ArgumentParser(description="Spaced pilot batches: job_ingest pipeline")
    parser.add_argument("--iterations", type=int, default=8, help="Number of pipeline runs")
    parser.add_argument(
        "--dynamic-iterations",
        action="store_true",
        help="Ignore --iterations and compute runs from remaining crawl-ready sources and checkpoint.",
    )
    parser.add_argument("--batch-size", type=int, default=12, help="Sources (resources) per batch")
    parser.add_argument("--source-request-delay", type=float, default=0.0, help="Extra per-request delay inside each source crawl")
    parser.add_argument("--source-request-jitter", type=float, default=0.0, help="Random 0..N per-request delay inside each source crawl")
    parser.add_argument(
        "--max-jobs-per-source",
        type=int,
        default=60,
        help="Cap job listings fetched per source (reduces rate-limit / timeout risk)",
    )
    parser.add_argument(
        "--min-jobs-per-source",
        type=int,
        default=0,
        help="Track source yield and flag runs below threshold.",
    )
    parser.add_argument(
        "--auto-pause-low-yield",
        action="store_true",
        help="Auto-pause sources repeatedly below --min-jobs-per-source.",
    )
    parser.add_argument(
        "--low-yield-runs-threshold",
        type=int,
        default=3,
        help="Consecutive low-yield runs before auto-pause.",
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
        "--sync-limit",
        type=int,
        default=200,
        help="Max verified rows to sync to Postgres per iteration.",
    )
    parser.add_argument(
        "--max-jobs-per-domain",
        type=int,
        default=0,
        help="Cap rows per source domain in sheets export (0 = no cap).",
    )
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

    total_iterations = int(args.iterations)
    if args.dynamic_iterations:
        try:
            total_iterations = _resolve_dynamic_iterations(
                batch_size=int(args.batch_size),
                student_pipeline_only=bool(args.student_pipeline_only),
            )
            print(
                f"Dynamic iterations resolved from checkpoint: {total_iterations} "
                f"(batch_size={args.batch_size})"
            )
        except Exception as e:
            print(f"Dynamic iteration resolution failed ({e}); using --iterations={args.iterations}")
            total_iterations = int(args.iterations)

    if total_iterations <= 0:
        print("No remaining sources to process; exiting pilot day runner.")
        return 0

    for i in range(total_iterations):
        if args.max_wall_minutes > 0:
            elapsed_min = (time.monotonic() - t0) / 60.0
            if elapsed_min >= args.max_wall_minutes:
                print(f"Stopping: max_wall_minutes={args.max_wall_minutes} reached.")
                break

        print(f"\n=== Pilot batch {i + 1}/{total_iterations} ===")
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
            "--sync-limit",
            str(args.sync_limit),
        ]
        if int(args.max_jobs_per_domain) > 0:
            cmd.extend(["--max-jobs-per-domain", str(int(args.max_jobs_per_domain))])
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
        if int(args.min_jobs_per_source) > 0:
            cmd.extend(["--min-jobs-per-source", str(int(args.min_jobs_per_source))])
        if args.auto_pause_low_yield:
            cmd.append("--auto-pause-low-yield")
        if int(args.low_yield_runs_threshold) > 0:
            cmd.extend(["--low-yield-runs-threshold", str(int(args.low_yield_runs_threshold))])
        if not args.no_append_sheet:
            cmd.append("--append-sheet")
        if args.no_strict_india:
            cmd.append("--no-strict-india")

        r = subprocess.run(cmd, cwd=ROOT)
        if r.returncode != 0:
            print(f"Pipeline failed (exit {r.returncode}); sleeping {args.sleep_after_fail}s")
            time.sleep(float(args.sleep_after_fail))
            continue

        if i + 1 < total_iterations:
            gap = random.uniform(float(args.sleep_min), float(args.sleep_max))
            print(f"Sleeping {gap:.0f}s before next batch...")
            time.sleep(gap)

    print("Pilot day runner finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
