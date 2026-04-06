#!/usr/bin/env python3
"""
One batched end-to-end run:

  1) Crawl next window of Mongo sources (checkpointed offset)
  2) Upsert crawled jobs → Mongo job_ingest
  3) ML + profile gate → verified / rejected
  4) Sync verified Mongo rows → Postgres jobs (source=job_board)
  5) Optional: Google Sheet from Postgres (--append-sheet for same-day accumulation)

State file: app/data/pipeline/crawl_batch_state.json
"""

from __future__ import annotations

import argparse
import json
import sys
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STATE_PATH = ROOT / "app" / "data" / "pipeline" / "crawl_batch_state.json"


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"source_offset": 0}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def _save_state(data: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batched crawl → job_ingest → ML → Postgres sync → Sheet (or JSON-fallback → Sheet)"
    )
    parser.add_argument("--batch-size", type=int, default=12, help="Sources to crawl this run")
    parser.add_argument(
        "--source-request-delay",
        type=float,
        default=0.0,
        help="Extra delay inside each source crawl request (anti-ban).",
    )
    parser.add_argument(
        "--source-request-jitter",
        type=float,
        default=0.0,
        help="Random extra per-request delay 0..N seconds inside each source crawl.",
    )
    parser.add_argument(
        "--fallback-max-retries",
        type=int,
        default=1,
        help="If Mongo is down and fallback JSON-only crawl yields 0 jobs, skip ahead and retry (avoid empty daily exports).",
    )
    parser.add_argument("--max-jobs-per-source", type=int, default=60, help="Cap job candidates per source crawl")
    parser.add_argument(
        "--prefer-less-known-sources",
        action="store_true",
        help="Prioritize lesser-known source domains over major boards.",
    )
    parser.add_argument(
        "--exclude-popular-sources",
        action="store_true",
        help="Skip major/common boards during crawl.",
    )
    parser.add_argument(
        "--focus-digital-marketing",
        action="store_true",
        help="Keep digital-marketing oriented jobs in profile filter step.",
    )
    parser.add_argument("--ml-limit", type=int, default=500, help="Max job_ingest docs to process this run")
    parser.add_argument(
        "--sync-limit",
        type=int,
        default=200,
        help="Max verified rows to sync to Postgres this run (keeps each batch fast).",
    )
    parser.add_argument("--no-sheet", action="store_true", help="Skip Google Sheets export")
    parser.add_argument(
        "--append-sheet",
        action="store_true",
        help="Pass --append-jobs to sheet export (keep earlier rows on today's tab)",
    )
    parser.add_argument(
        "--student-pipeline-only",
        action="store_true",
        help="Only crawl student_pipeline_eligible sources",
    )
    parser.add_argument("--sleep-after-crawl", type=float, default=0.0, help="Seconds to pause before ML step")
    parser.add_argument(
        "--no-strict-india",
        action="store_true",
        help="Forward to process_job_ingest_ml.py (disable India-only gate)",
    )
    parser.add_argument(
        "--mongo-fallback-json",
        action="store_true",
        help="If Mongo is unavailable, still run crawl using app/data/crawl_ready_sources.json (or --sources-file).",
    )
    parser.add_argument(
        "--sources-file",
        type=Path,
        default=Path("app/data/crawl_ready_sources.json"),
        help="Sources JSON used when --mongo-fallback-json is enabled.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore saved crawl_batch_state.json and start from source_offset=0 for this run.",
    )
    args = parser.parse_args()

    py = sys.executable

    from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService

    src = MongoJobBoardSourcesService()
    st = _load_state()
    off = 0 if args.reset_checkpoint else int(st.get("source_offset") or 0)
    try:
        total = src.count_crawl_ready_active(
            student_pipeline_priority=True,
            student_pipeline_only=bool(args.student_pipeline_only),
        )
    except Exception as e:
        if not args.mongo_fallback_json:
            print(
                "ERROR: MongoDB is required for this run but is not reachable.\n"
                f"  {e}\n"
                "  Fix: start MongoDB and/or set MONGODB_URI in .env. "
                "Or run without --disable-mongo-fallback to use JSON-only crawl when Mongo is down.",
                file=sys.stderr,
            )
            return 1
        print(f"WARNING: Mongo unavailable ({e}); running JSON-only fallback (crawl → merge → sheets).")

        def _count_jobs_master() -> int:
            master_path = ROOT / "app" / "data" / "jobs" / "jobs_master.json"
            if not master_path.exists():
                return 0
            try:
                payload = json.loads(master_path.read_text(encoding="utf-8"))
                return len(payload.get("jobs") or [])
            except Exception:
                return 0

        attempt_off = off
        jobs_count = 0
        last_batch_id = ""
        max_retries = int(max(0, args.fallback_max_retries))

        # Attempt N+1 times (N retries).
        for attempt in range(max_retries + 1):
            batch_id = f"fallback_{attempt_off}_{args.batch_size}_try{attempt}"
            last_batch_id = batch_id
            print(
                f"Checkpoint source_offset={attempt_off} batch_size={args.batch_size} total_active_sources=0 (try {attempt + 1}/{max_retries + 1})"
            )

            jobs_run_out = ROOT / "app" / "data" / "jobs" / f"jobs_run_{batch_id}.json"
            crawl_cmd = [
                py,
                "scripts/crawl_jobs_from_sources.py",
                "--sources-file",
                str(args.sources_file),
                "--max-sources",
                str(args.batch_size),
                "--source-offset",
                str(attempt_off),
                "--max-jobs-per-source",
                str(args.max_jobs_per_source),
                "--out",
                str(jobs_run_out),
            ]
            if args.source_request_delay > 0:
                crawl_cmd.extend(["--source-request-delay", str(args.source_request_delay)])
            if args.source_request_jitter > 0:
                crawl_cmd.extend(["--source-request-jitter", str(args.source_request_jitter)])
            if args.prefer_less_known_sources:
                crawl_cmd.append("--prefer-less-known-sources")
            if args.exclude_popular_sources:
                crawl_cmd.append("--exclude-popular-sources")
            if args.focus_digital_marketing:
                crawl_cmd.append("--focus-digital-marketing")
            r1 = subprocess.run(crawl_cmd, cwd=ROOT)
            if r1.returncode != 0:
                return r1.returncode

            r2 = subprocess.run(
                [py, "scripts/merge_job_runs.py", "--jobs-run-files", str(jobs_run_out)],
                cwd=ROOT,
            )
            if r2.returncode != 0:
                return r2.returncode

            jobs_count = _count_jobs_master()
            if jobs_count > 0:
                break
            if attempt < max_retries:
                print("WARNING: fallback attempt produced 0 jobs; skipping ahead and retrying...")
                attempt_off += int(args.batch_size)

        if not args.no_sheet:
            cmd = [
                py,
                "scripts/export_job_board_jobs_to_sheets.py",
                "--jobs-json",
                "app/data/jobs/jobs_master.json",
            ]
            if args.append_sheet:
                cmd.append("--append-jobs")
            r3 = subprocess.run(cmd, cwd=ROOT)
            if r3.returncode != 0:
                return r3.returncode

        new_off = attempt_off + int(args.batch_size)
        _save_state(
            {
                "source_offset": new_off,
                "total_crawl_ready_last": 0,
                "last_batch_id": last_batch_id,
            }
        )
        print(f"Saved checkpoint: next source_offset={new_off}")
        return 0

    if total > 0 and off >= total:
        off = 0

    batch_id = f"batch_{off}_{args.batch_size}"
    print(f"Checkpoint source_offset={off} batch_size={args.batch_size} total_active_sources={total}")

    crawl_cmd = [
        py,
        "scripts/crawl_jobs_from_sources.py",
        "--from-mongo",
        "--max-sources",
        str(args.batch_size),
        "--source-offset",
        str(off),
        "--max-jobs-per-source",
        str(args.max_jobs_per_source),
        "--source-request-delay",
        str(args.source_request_delay),
        "--source-request-jitter",
        str(args.source_request_jitter),
        "--write-job-ingest",
        "--crawl-batch-id",
        batch_id,
    ]
    if args.prefer_less_known_sources:
        crawl_cmd.append("--prefer-less-known-sources")
    if args.exclude_popular_sources:
        crawl_cmd.append("--exclude-popular-sources")
    if args.focus_digital_marketing:
        crawl_cmd.append("--focus-digital-marketing")
    if args.mongo_fallback_json:
        crawl_cmd.append("--mongo-fallback-json")
        crawl_cmd.extend(["--sources-file", str(args.sources_file)])
    if args.student_pipeline_only:
        crawl_cmd.append("--student-pipeline-only")

    r1 = subprocess.run(crawl_cmd, cwd=ROOT)
    if r1.returncode != 0:
        return r1.returncode

    if args.sleep_after_crawl > 0:
        time.sleep(float(args.sleep_after_crawl))

    print(">>> ML: process_job_ingest_ml (this can take several minutes) ...", flush=True)
    ml_cmd = [py, "scripts/job_ingest/process_job_ingest_ml.py", "--limit", str(args.ml_limit)]
    if args.no_strict_india:
        ml_cmd.append("--no-strict-india")
    r2 = subprocess.run(ml_cmd, cwd=ROOT)
    if r2.returncode != 0:
        return r2.returncode

    print(f">>> Postgres: sync_verified_to_postgres (limit={args.sync_limit}) ...", flush=True)
    r3 = subprocess.run(
        [
            py,
            "scripts/job_ingest/sync_verified_to_postgres.py",
            "--limit",
            str(args.sync_limit),
        ],
        cwd=ROOT,
    )
    if r3.returncode != 0:
        return r3.returncode

    if not args.no_sheet:
        print(">>> Google Sheets: export_job_board_jobs_to_sheets (chunked writes) ...", flush=True)
        cmd = [
            py,
            "scripts/export_job_board_jobs_to_sheets.py",
            "--from-postgres",
        ]
        if args.append_sheet:
            cmd.append("--append-jobs")
        r4 = subprocess.run(cmd, cwd=ROOT)
        if r4.returncode != 0:
            return r4.returncode

    new_off = off + int(args.batch_size)
    if total > 0 and new_off >= total:
        new_off = 0
    _save_state(
        {
            "source_offset": new_off,
            "total_crawl_ready_last": total,
            "last_batch_id": batch_id,
        }
    )
    print(f"Saved checkpoint: next source_offset={new_off}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
