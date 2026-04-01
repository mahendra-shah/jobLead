#!/usr/bin/env python3
"""
Daily job-ingest automation — one entry point.

Each pipeline run (one "part"):
  Crawl next N Mongo sources (checkpoint) → upsert job_ingest → ML (+ India gate)
  → sync verified rows to Postgres (source=job_board) → Google Sheet from Postgres (append by default).

Modes
-----
  * Single part (default): one batch, then exit.
  * All day (--all-day): many parts spaced apart so you crawl all sources over time
    without hammering boards (rate limits / 403s). Same ML → Postgres → sheet each part.

Prerequisites: MongoDB, PostgreSQL (DATABASE_URL or LOCAL_DATABASE_URL), job_board_sources, credentials + JOB_BOARD_SHEET_ID.
If Mongo is down, --mongo-fallback-json (on by default) switches to JSON-only crawl merge (no ML / no Postgres sync).

Recommended (workday, 10–15 sources per part, ~5–10 min between parts):
  python3 scripts/run_daily_ingest_automation.py --all-day

Human-like India/remote fresher daily preset (niche-only, slow crawling, all day):
  python3 scripts/run_daily_ingest_automation.py --india-remote-fresher-day

More control:
  python3 scripts/run_daily_ingest_automation.py --all-day --spaced-batches 24 \\
    --batch-size 12 --sleep-min 300 --sleep-max 600 --max-wall-minutes 540

Examples:
  python3 scripts/run_daily_ingest_automation.py
  python3 scripts/run_daily_ingest_automation.py --pause-low-sources
  python3 scripts/run_daily_ingest_automation.py --spaced-batches 8 --sleep-min 240 --sleep-max 420

Full day + student report + dated sheet snapshot: use scripts/run_daily_final.py instead.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Daily automation: job_ingest pipeline (one command)",
    )
    parser.add_argument("--batch-size", type=int, default=15, help="Sources per batch")
    parser.add_argument(
        "--source-request-delay",
        type=float,
        default=0.0,
        help="Extra delay before each request inside a single source crawl (anti-ban).",
    )
    parser.add_argument(
        "--source-request-jitter",
        type=float,
        default=0.0,
        help="Random extra delay 0..N seconds before each request inside a source crawl.",
    )
    parser.add_argument(
        "--max-jobs-per-source",
        type=int,
        default=60,
        help="Cap job candidates per source (faster + less waste)",
    )
    parser.add_argument("--ml-limit", type=int, default=600, help="Max ML rows per batch")
    parser.add_argument(
        "--sync-limit",
        type=int,
        default=200,
        help="Max verified rows to sync to Postgres per batch (controls sync time).",
    )
    parser.add_argument("--prefer-less-known-sources", action="store_true", help="Prioritize lesser-known source domains")
    parser.add_argument("--exclude-popular-sources", action="store_true", help="Skip major/common boards")
    parser.add_argument("--focus-digital-marketing", action="store_true", help="Focus output on digital-marketing roles")
    parser.add_argument(
        "--student-pipeline-only",
        action="store_true",
        help="Only crawl Mongo sources marked student_pipeline_eligible (India/remote–oriented boards)",
    )
    parser.add_argument(
        "--no-append-sheet",
        action="store_true",
        help="Skip Google Sheets (Mongo + verified JSON only)",
    )
    parser.add_argument(
        "--pause-low-sources",
        action="store_true",
        help="Run pause_sources_low_india_yield.py first (pause bad domains)",
    )
    parser.add_argument(
        "--pause-dry-run",
        action="store_true",
        help="With --pause-low-sources: only print what would pause",
    )
    parser.add_argument(
        "--all-day",
        action="store_true",
        help="Spaced mode: run many batches over the day (sets --spaced-batches default if 0). "
        "Each batch: crawl → ingest → ML → JSON → sheet.",
    )
    parser.add_argument(
        "--spaced-batches",
        type=int,
        default=0,
        help="Number of spaced pipeline runs (parts). With --all-day and 0 here, defaults to 20.",
    )
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=240.0,
        help="Seconds between parts (min; random jitter to sleep-max)",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=480.0,
        help="Seconds between parts (max; random jitter)",
    )
    parser.add_argument(
        "--max-wall-minutes",
        type=float,
        default=0.0,
        help="With spaced-batches: stop after N minutes (0 = no limit)",
    )
    parser.add_argument(
        "--no-strict-india",
        action="store_true",
        help="Disable India-only ML gate (forwarded to pipeline)",
    )
    parser.add_argument(
        "--mongo-fallback-json",
        action="store_true",
        help="If Mongo is down, crawl from app/data/crawl_ready_sources.json so daily run still completes.",
    )
    parser.add_argument(
        "--disable-mongo-fallback",
        action="store_true",
        help="Do not auto-switch to JSON-only flow when Mongo is down (may crash if Mongo is unavailable).",
    )
    parser.add_argument(
        "--sources-file",
        type=Path,
        default=None,
        help="Optional sources JSON file used with --mongo-fallback-json.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Restart fallback/crawl from source_offset=0 for this run.",
    )
    parser.add_argument(
        "--india-remote-fresher-day",
        action="store_true",
        help=(
            "Preset for all-day, human-like crawling focused on India/remote fresher jobs from "
            "student-pipeline niche sources only."
        ),
    )
    args = parser.parse_args()

    if args.india_remote_fresher_day:
        # Human-like daily preset for niche India/remote fresher targeting.
        args.all_day = True
        args.student_pipeline_only = True
        args.prefer_less_known_sources = True
        args.exclude_popular_sources = True
        if args.batch_size == 15:
            args.batch_size = 12
        if args.source_request_delay == 0.0:
            args.source_request_delay = 2.8
        if args.source_request_jitter == 0.0:
            args.source_request_jitter = 2.4
        if args.max_jobs_per_source == 60:
            args.max_jobs_per_source = 40
        if args.spaced_batches == 0:
            args.spaced_batches = 32
        if args.sleep_min == 240.0 and args.sleep_max == 480.0:
            args.sleep_min = 420.0
            args.sleep_max = 900.0

    if args.all_day:
        if int(args.spaced_batches) <= 0:
            args.spaced_batches = 20
        # Gentler spacing for all-day unless user already changed sleeps from argparse defaults
        if args.sleep_min == 240.0 and args.sleep_max == 480.0:
            args.sleep_min = 300.0
            args.sleep_max = 600.0

    py = sys.executable

    if args.pause_low_sources:
        pause_cmd = [py, str(ROOT / "scripts/pipeline/pause_sources_low_india_yield.py")]
        if args.pause_dry_run:
            pause_cmd.append("--dry-run")
        print(">>> Step: pause low-yield sources")
        r = subprocess.run(pause_cmd, cwd=ROOT)
        if r.returncode != 0:
            return r.returncode

    if int(args.spaced_batches) > 0:
        pilot_cmd = [
            py,
            str(ROOT / "scripts/pipeline/run_pilot_day_batches.py"),
            "--iterations",
            str(int(args.spaced_batches)),
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
            "--sleep-min",
            str(args.sleep_min),
            "--sleep-max",
            str(args.sleep_max),
        ]
        if args.prefer_less_known_sources:
            pilot_cmd.append("--prefer-less-known-sources")
        if args.exclude_popular_sources:
            pilot_cmd.append("--exclude-popular-sources")
        if args.focus_digital_marketing:
            pilot_cmd.append("--focus-digital-marketing")
        if args.student_pipeline_only:
            pilot_cmd.append("--student-pipeline-only")
        if args.max_wall_minutes > 0:
            pilot_cmd.extend(["--max-wall-minutes", str(args.max_wall_minutes)])
        if args.no_append_sheet:
            pilot_cmd.append("--no-append-sheet")
        if args.no_strict_india:
            pilot_cmd.append("--no-strict-india")
        # Match single-pass resilience: pilot forwards --mongo-fallback-json by default.
        if args.disable_mongo_fallback:
            pilot_cmd.append("--disable-mongo-fallback")
        mode = "all-day spaced" if args.all_day else "spaced pilot"
        print(f">>> Step: {mode} ({args.spaced_batches} parts, batch_size={args.batch_size})")
        return subprocess.run(pilot_cmd, cwd=ROOT).returncode

    pipe_cmd = [
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
    if args.prefer_less_known_sources:
        pipe_cmd.append("--prefer-less-known-sources")
    if args.exclude_popular_sources:
        pipe_cmd.append("--exclude-popular-sources")
    if args.focus_digital_marketing:
        pipe_cmd.append("--focus-digital-marketing")
    if args.student_pipeline_only:
        pipe_cmd.append("--student-pipeline-only")
    if not args.no_append_sheet:
        pipe_cmd.append("--append-sheet")
    if args.no_strict_india:
        pipe_cmd.append("--no-strict-india")
    # Default: be resilient. If Mongo is down, the pipeline will switch to JSON-only fallback
    # (and will print a WARNING). This lets the "one daily command" always succeed.
    should_enable_fallback = bool(args.mongo_fallback_json) or not bool(args.disable_mongo_fallback)

    if should_enable_fallback:
        pipe_cmd.append("--mongo-fallback-json")
        if args.sources_file is not None:
            pipe_cmd.extend(["--sources-file", str(args.sources_file)])
    if args.reset_checkpoint:
        pipe_cmd.append("--reset-checkpoint")

    print(">>> Step: run_job_ingest_pipeline (crawl → ingest → ML → Postgres → sheet, or JSON-fallback → sheet)")
    return subprocess.run(pipe_cmd, cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
