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

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.utils.timezone import ist_today_utc_window

STATE_PATH = ROOT / "app" / "data" / "pipeline" / "crawl_batch_state.json"


def _resolve_sources_file(explicit: Path | None) -> Path | None:
    """Resolve fallback JSON source file from explicit path or known defaults."""
    candidates: list[Path] = []
    if explicit is not None:
        p = explicit if explicit.is_absolute() else (ROOT / explicit)
        candidates.append(p)
    candidates.extend(
        [
            ROOT / "app" / "data" / "crawl_ready_sources.json",
            ROOT / "app" / "data" / "discovery_sources_test.json",
            ROOT / "app" / "data" / "discovery_sources_seed.json",
        ]
    )
    return next((p for p in candidates if p.exists()), None)


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
        default=100,
        help=(
            "Max verified Mongo rows to sync to Postgres this run. "
            "Use a small value for fast per-batch exports (recommended: 50-200)."
        ),
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
        help="If Mongo is unavailable, still run crawl using resolved JSON sources file (or --sources-file).",
    )
    parser.add_argument(
        "--sources-file",
        type=Path,
        default=None,
        help=(
            "Optional sources JSON for fallback mode. If omitted, resolves first existing from "
            "app/data/crawl_ready_sources.json, app/data/discovery_sources_test.json, "
            "app/data/discovery_sources_seed.json."
        ),
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore saved crawl_batch_state.json and start from source_offset=0 for this run.",
    )
    parser.add_argument(
        "--no-jobs-json",
        action="store_true",
        help="Skip writing jobs_run JSON during Mongo-backed runs (fallback mode still writes JSON).",
    )
    args = parser.parse_args()

    required_scripts = [
        ROOT / "scripts" / "job_board_flow" / "crawl_jobs_from_sources.py",
        ROOT / "scripts" / "job_board_flow" / "merge_job_runs.py",
        ROOT / "scripts" / "job_board_flow" / "job_ingest" / "process_job_ingest_ml.py",
        ROOT / "scripts" / "job_board_flow" / "job_ingest" / "sync_verified_to_postgres.py",
    ]
    if not args.no_sheet:
        required_scripts.append(ROOT / "scripts" / "job_board_flow" / "export_job_board_jobs_to_sheets.py")

    missing = [p for p in required_scripts if not p.exists()]
    if missing:
        print(
            "ERROR: Required pipeline script(s) are missing. "
            "Refusing to start crawl to avoid repeated batches without sync/checkpoint advancement.",
            file=sys.stderr,
        )
        for path in missing:
            print(f"  - {path}", file=sys.stderr)
        return 2

    py = sys.executable
    _, _, ist_date_str = ist_today_utc_window()
    daily_jobs_run_out = ROOT / "app" / "data" / "jobs" / f"jobs_run_{ist_date_str}.json"

    from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService

    src = MongoJobBoardSourcesService()
    st = _load_state()
    off = 0 if args.reset_checkpoint else int(st.get("source_offset") or 0)
    resolved_sources_file = _resolve_sources_file(args.sources_file)

    if bool(args.mongo_fallback_json) and resolved_sources_file is None:
        print(
            "ERROR: --mongo-fallback-json is enabled but no sources JSON file was found.\n"
            "Checked: app/data/crawl_ready_sources.json, app/data/discovery_sources_test.json, "
            "app/data/discovery_sources_seed.json",
            file=sys.stderr,
        )
        return 1

    use_json_fallback = bool(args.mongo_fallback_json)

    try:
        total = src.count_crawl_ready_active(
            student_pipeline_priority=True,
            student_pipeline_only=bool(args.student_pipeline_only),
        )
    except Exception as e:
        if not use_json_fallback:
            print(
                "ERROR: MongoDB is required for this run but is not reachable.\n"
                f"  {e}\n"
                "  Fix: start MongoDB and/or set MONGODB_URI in .env. "
                "Or run with --mongo-fallback-json to use JSON-only crawl when Mongo is down.",
                file=sys.stderr,
            )
            return 1
        use_json_fallback = True
        print(f"WARNING: Mongo unavailable ({e}); running JSON-only fallback (crawl → sheets).")

        def _count_jobs_in_file(path: Path) -> int:
            if not path.exists():
                return 0
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
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

            jobs_run_out = daily_jobs_run_out
            crawl_cmd = [
                py,
                "scripts/job_board_flow/crawl_jobs_from_sources.py",
                "--sources-file",
                str(resolved_sources_file),
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
                [py, "scripts/job_board_flow/merge_job_runs.py", "--jobs-run-files", str(jobs_run_out)],
                cwd=ROOT,
            )
            if r2.returncode != 0:
                return r2.returncode

            jobs_count = _count_jobs_in_file(jobs_run_out)
            if jobs_count > 0:
                break
            if attempt < max_retries:
                print("WARNING: fallback attempt produced 0 jobs; skipping ahead and retrying...")
                attempt_off += int(args.batch_size)

        if not args.no_sheet:
            cmd = [
                py,
                "scripts/job_board_flow/export_job_board_jobs_to_sheets.py",
                "--jobs-json",
                str(daily_jobs_run_out),
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

    if total == 0 and resolved_sources_file is not None:
        use_json_fallback = True
        print(f"WARNING: No crawl-ready Mongo sources found; enabling JSON fallback using {resolved_sources_file}")

    if use_json_fallback:
        if resolved_sources_file is None:
            print(
                "ERROR: JSON fallback is enabled but no sources JSON file was found.\n"
                "Checked: app/data/crawl_ready_sources.json, app/data/discovery_sources_test.json, "
                "app/data/discovery_sources_seed.json",
                file=sys.stderr,
            )
            return 1

        if off > 0:
            # Keep checkpointed offsets stable for Mongo-backed runs, but JSON fallback should still start at 0 when the
            # source list is shorter than the saved offset.
            off = max(0, off)

        batch_id = f"fallback_{off}_{args.batch_size}"
        print(f"Checkpoint source_offset={off} batch_size={args.batch_size} total_active_sources=0")

        crawl_cmd = [
            py,
            "scripts/job_board_flow/crawl_jobs_from_sources.py",
            "--sources-file",
            str(resolved_sources_file),
            "--max-sources",
            str(args.batch_size),
            "--source-offset",
            str(off),
            "--max-jobs-per-source",
            str(args.max_jobs_per_source),
            "--out",
            str(daily_jobs_run_out),
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

        if args.sleep_after_crawl > 0:
            time.sleep(float(args.sleep_after_crawl))

        ml_cmd = [py, "scripts/job_board_flow/job_ingest/process_job_ingest_ml.py", "--limit", str(args.ml_limit)]
        if args.no_strict_india:
            ml_cmd.append("--no-strict-india")
        r2 = subprocess.run(ml_cmd, cwd=ROOT)
        if r2.returncode != 0:
            return r2.returncode

        sync_limit = int(max(1, int(args.sync_limit)))
        print(f">>> Step: sync_verified_to_postgres (limit={sync_limit})")
        r3 = subprocess.run(
            [py, "scripts/job_board_flow/job_ingest/sync_verified_to_postgres.py", "--limit", str(sync_limit)],
            cwd=ROOT,
        )
        if r3.returncode != 0:
            return r3.returncode

        if not args.no_sheet:
            print(">>> Step: export_job_board_jobs_to_sheets (--from-postgres)")
            cmd = [
                py,
                "scripts/job_board_flow/export_job_board_jobs_to_sheets.py",
                "--from-postgres",
            ]
            if args.append_sheet:
                cmd.append("--append-jobs")
            r4 = subprocess.run(cmd, cwd=ROOT)
            if r4.returncode != 0:
                return r4.returncode

        new_off = off + int(args.batch_size)
        _save_state(
            {
                "source_offset": new_off,
                "total_crawl_ready_last": 0,
                "last_batch_id": batch_id,
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
        "scripts/job_board_flow/crawl_jobs_from_sources.py",
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
    if not args.no_jobs_json:
        crawl_cmd.extend(["--out", str(daily_jobs_run_out)])
    else:
        crawl_cmd.append("--no-write-json")
    if args.prefer_less_known_sources:
        crawl_cmd.append("--prefer-less-known-sources")
    if args.exclude_popular_sources:
        crawl_cmd.append("--exclude-popular-sources")
    if args.focus_digital_marketing:
        crawl_cmd.append("--focus-digital-marketing")
    if args.mongo_fallback_json:
        crawl_cmd.append("--mongo-fallback-json")
        crawl_cmd.extend(["--sources-file", str(resolved_sources_file)])
    if args.student_pipeline_only:
        crawl_cmd.append("--student-pipeline-only")

    r1 = subprocess.run(crawl_cmd, cwd=ROOT)
    if r1.returncode != 0:
        return r1.returncode

    if args.sleep_after_crawl > 0:
        time.sleep(float(args.sleep_after_crawl))

    ml_cmd = [py, "scripts/job_board_flow/job_ingest/process_job_ingest_ml.py", "--limit", str(args.ml_limit)]
    if args.no_strict_india:
        ml_cmd.append("--no-strict-india")
    r2 = subprocess.run(ml_cmd, cwd=ROOT)
    if r2.returncode != 0:
        return r2.returncode

    sync_limit = int(max(1, int(args.sync_limit)))
    print(f">>> Step: sync_verified_to_postgres (limit={sync_limit})")
    r3 = subprocess.run(
        [py, "scripts/job_board_flow/job_ingest/sync_verified_to_postgres.py", "--limit", str(sync_limit)],
        cwd=ROOT,
    )
    if r3.returncode != 0:
        return r3.returncode

    if not args.no_sheet:
        print(">>> Step: export_job_board_jobs_to_sheets (--from-postgres)")
        cmd = [
            py,
            "scripts/job_board_flow/export_job_board_jobs_to_sheets.py",
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
