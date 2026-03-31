#!/usr/bin/env python3
"""
Daily pipeline: discover new sources → export crawl-ready → crawl jobs → merge → export to Google Sheet.

Run from project root:
  python scripts/run_daily_job_board_pipeline.py

Steps:
  1. GitHub discovery (add new sources to discovery_sources_test.json)
  2. Export crawl-ready sources → app/data/crawl_ready_sources.json
  3. Crawl jobs from those sources → app/data/jobs/jobs_run_<timestamp>.json
  4. Merge all runs → app/data/jobs/jobs_master.json
  5. Export sources + jobs to JOB_BOARD_SHEET_ID (tabs <date>_sources, <date>_jobs)

Flags:
  --skip-discovery   Use existing discovery_sources_test.json only
  --skip-crawl       Use existing jobs_run_*.json; only merge + export
  --max-sources N    Max sources to crawl (default 15)
  --dry-run          Discovery only, no crawl/merge/export
"""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run(cmd: list[str], step_name: str) -> bool:
    print(f"\n{'='*60}\n  {step_name}\n{'='*60}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"  ❌ {step_name} failed (exit {result.returncode})")
        return False
    print(f"  ✅ {step_name} done")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily job-board pipeline: discovery → crawl → merge → export")
    parser.add_argument("--skip-discovery", action="store_true", help="Skip GitHub discovery; use existing sources JSON")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip crawl; only merge existing runs + export to sheet")
    parser.add_argument("--max-sources", type=int, default=15, help="Max crawl-ready sources to crawl (default 15)")
    parser.add_argument("--dry-run", action="store_true", help="Run only discovery (no crawl, merge, or export)")
    args = parser.parse_args()

    py = sys.executable

    if not args.skip_discovery:
        if not run(
            [py, "scripts/discovery/pipeline_2_github.py", "--delay", "1.5"],
            "Step 1: GitHub discovery (add new sources)",
        ):
            return 1
    else:
        print("\n  [Skipping discovery - using existing discovery_sources_test.json]")

    if args.dry_run:
        print("\n  [Dry run: stopping after discovery]")
        return 0

    if not run(
        [py, "scripts/discovery/export_crawl_ready_sources.py"],
        "Step 2: Export crawl-ready sources",
    ):
        return 1

    if not args.skip_crawl:
        if not run(
            [
                py,
                "scripts/crawl_jobs_from_sources.py",
                "--from-mongo",
                "--max-sources",
                str(args.max_sources),
            ],
            "Step 3: Crawl jobs from sources (Mongo: India/remote boards first)",
        ):
            return 1
    else:
        print("\n  [Skipping crawl - using existing jobs_run_*.json]")

    if not run([py, "scripts/merge_job_runs.py"], "Step 4: Merge job runs → jobs_master.json"):
        return 1

    if not run([py, "scripts/export_job_board_jobs_to_sheets.py"], "Step 5: Export to Google Sheet"):
        return 1

    print("\n" + "=" * 60)
    print("  ✅ Daily pipeline complete. Check your Job Board Google Sheet.")
    print("=" * 60 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
