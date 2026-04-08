#!/usr/bin/env python3
"""
Modular job-board pipeline (job boards only), strict 5 steps:
  1) Crawler
  2) Mongo ingest
  3) ML classify (direct, no Mongo ml_status transitions)
  4) Postgres upsert (fallback JSONL if DB unavailable)
  5) Google Sheet export from Postgres
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.pipeline.job_board.contracts import PipelineResult
from app.utils.timezone import now_ist
from app.pipeline.job_board.steps import (
    classify_jobs_direct,
    export_sheet_from_json,
    export_sheet_from_postgres,
    ingest_jobs_to_mongo,
    persist_verified_to_postgres,
    run_crawl_step,
    share_jobs_with_students,
)

STATE_PATH = ROOT / "app" / "data" / "pipeline" / "crawl_batch_state.json"


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"source_offset": 0}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def _save_state(data: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Modular 5-step job-board pipeline (crawler→mongo→ml→postgres→sheet)",
        epilog=(
            "High volume same-day sheet (example): "
            "%(prog)s --rounds 8 --batch-size 6 --popular-source-max-jobs 40 "
            "--prefer-less-known-sources --no-profile-filter --append-sheet --sheet-date 2026-04-08"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--rounds",
        type=int,
        default=1,
        help="Run full pipeline this many times (checkpoint advances each round). Default: 1.",
    )
    ap.add_argument("--batch-size", type=int, default=2)
    ap.add_argument("--max-jobs-per-source", type=int, default=60)
    ap.add_argument(
        "--popular-source-max-jobs",
        type=int,
        default=10,
        help="Cap per popular-domain source (AmbitionBox/Naukri/…); raise for more rows per round.",
    )
    ap.add_argument("--source-request-delay", type=float, default=0.0)
    ap.add_argument("--source-request-jitter", type=float, default=0.0)
    ap.add_argument("--prefer-less-known-sources", action="store_true")
    ap.add_argument("--exclude-popular-sources", action="store_true")
    ap.add_argument("--focus-digital-marketing", action="store_true")
    ap.add_argument("--student-pipeline-only", action="store_true")
    ap.add_argument("--no-profile-filter", action="store_true", help="Disable crawl-stage target profile filter")
    ap.add_argument("--reset-checkpoint", action="store_true")

    ap.add_argument("--min-confidence", type=float, default=0.5)
    ap.add_argument("--no-strict-india", action="store_true")
    ap.add_argument("--no-depth-profile", action="store_true")
    ap.add_argument("--no-require-remote", action="store_true")
    ap.add_argument("--no-require-role-track", action="store_true")

    ap.add_argument("--postgres-batch-size", type=int, default=40)
    ap.add_argument(
        "--fallback-jsonl",
        type=Path,
        default=Path("app/data/jobs/job_board_postgres_fallback.jsonl"),
    )
    ap.add_argument("--no-sheet", action="store_true")
    ap.add_argument("--append-sheet", action="store_true")
    ap.add_argument("--max-jobs-per-domain", type=int, default=0)
    ap.add_argument(
        "--sheet-date",
        type=str,
        default="",
        metavar="YYYY-MM-DD",
        help="IST date for Google Sheet tab names (sources + *_jobs). Default: script uses today's IST in exporter.",
    )
    ap.add_argument("--share-students", action="store_true", help="After verification, run student sharing workflow")
    ap.add_argument("--share-dry-run", action="store_true", help="With --share-students, compute matches only")
    ap.add_argument(
        "--strict-share-step",
        action="store_true",
        help="Fail pipeline if share step fails (default: warn and continue).",
    )
    ap.add_argument("--share-student-limit", type=int, default=0, help="With --share-students, limit students")
    ap.add_argument("--share-send-limit", type=int, default=0, help="With --share-students, cap sent emails")
    ap.add_argument("--share-max-jobs-per-student", type=int, default=5)
    ap.add_argument("--share-min-score", type=int, default=2)
    ap.add_argument(
        "--share-job-date",
        type=str,
        default="",
        help="Job date for sharing in IST YYYY-MM-DD (default: today IST).",
    )
    args = ap.parse_args()

    rounds = max(1, int(args.rounds))
    sheet_date = args.sheet_date.strip() or None
    sheet_exit = 0

    for round_idx in range(rounds):
        st = _load_state()
        off = (
            0
            if (args.reset_checkpoint and round_idx == 0)
            else int(st.get("source_offset") or 0)
        )
        print(
            f"\n=== Pipeline round {round_idx + 1}/{rounds} "
            f"(checkpoint source_offset={off}, batch_size={args.batch_size}) ===\n",
            flush=True,
        )

        print("\n[1/5] Crawl sources → jobs json ...\n", flush=True)
        crawl = run_crawl_step(
            root=ROOT,
            source_offset=off,
            batch_size=int(args.batch_size),
            max_jobs_per_source=int(args.max_jobs_per_source),
            popular_source_max_jobs=int(args.popular_source_max_jobs),
            source_request_delay=float(args.source_request_delay),
            source_request_jitter=float(args.source_request_jitter),
            prefer_less_known_sources=bool(args.prefer_less_known_sources),
            exclude_popular_sources=bool(args.exclude_popular_sources),
            focus_digital_marketing=bool(args.focus_digital_marketing),
            student_pipeline_only=bool(args.student_pipeline_only),
            no_profile_filter=bool(args.no_profile_filter),
        )
        print(f"Crawl output: {crawl.jobs_json_path} jobs={crawl.jobs_count}")

        print("\n[2/5] Ingest crawled rows to Mongo job_ingest ...\n", flush=True)
        mongo_stats = ingest_jobs_to_mongo(
            jobs_json_path=crawl.jobs_json_path,
            crawl_batch_id=crawl.batch_id,
            source_platform="job_board",
        )
        print(
            "mongo_ingest:",
            f"attempted={mongo_stats.attempted}",
            f"upserted={mongo_stats.upserted}",
            f"errors={mongo_stats.errors}",
        )

        print("\n[3/5] ML classify + profile gates (direct rows) ...\n", flush=True)
        verified_rows, ml_stats = classify_jobs_direct(
            jobs_json_path=crawl.jobs_json_path,
            min_confidence=float(args.min_confidence),
            strict_india=not bool(args.no_strict_india),
            depth_profile_enabled=False if args.no_depth_profile else None,
            require_remote_signal=False if args.no_require_remote else None,
            require_role_track=False if args.no_require_role_track else None,
        )
        print(
            "ml:",
            f"processed={ml_stats.processed}",
            f"verified={ml_stats.verified}",
            f"rejected={ml_stats.rejected}",
            f"depth={ml_stats.depth_gate}",
            f"spam={ml_stats.spam_prefilter}",
            f"quality={ml_stats.quality_gate}",
        )

        print("\n[4/5] Upsert verified rows to Postgres (fallback on DB failure) ...\n", flush=True)
        fallback_path = args.fallback_jsonl
        if not fallback_path.is_absolute():
            fallback_path = ROOT / fallback_path
        pg_stats = persist_verified_to_postgres(
            rows=verified_rows,
            batch_size=int(args.postgres_batch_size),
            fallback_jsonl=fallback_path,
        )
        print(
            "postgres:",
            f"inserted={pg_stats.inserted}",
            f"updated={pg_stats.updated}",
            f"skipped={pg_stats.skipped}",
            f"fallback_n={pg_stats.fallback_appended}",
        )

        fallback_jobs_json: Path | None = None
        if pg_stats.used_fallback:
            fallback_jobs_json = ROOT / "app" / "data" / "jobs" / f"jobs_verified_{crawl.batch_id}.json"
            fallback_jobs_json.parent.mkdir(parents=True, exist_ok=True)
            fallback_jobs_json.write_text(
                json.dumps(
                    {
                        "meta": {
                            "source": "job_board_pipeline_fallback",
                            "batch_id": crawl.batch_id,
                            "total_jobs": len(verified_rows),
                        },
                        "jobs": [
                            {k: v for k, v in row.items() if not str(k).startswith("_")}
                            for row in verified_rows
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        if not args.no_sheet:
            if fallback_jobs_json is not None:
                print("\n[5/5] Export fallback JSON batch to Google Sheet ...\n", flush=True)
                sheet_exit = export_sheet_from_json(
                    root=ROOT,
                    jobs_json_path=fallback_jobs_json,
                    append_jobs=bool(args.append_sheet),
                    sheet_date=sheet_date,
                )
            else:
                print("\n[5/5] Export Postgres rows to Google Sheet ...\n", flush=True)
                sheet_exit = export_sheet_from_postgres(
                    root=ROOT,
                    append_jobs=bool(args.append_sheet),
                    max_jobs_per_domain=int(args.max_jobs_per_domain),
                    sheet_date=sheet_date,
                )
                if sheet_exit != 0:
                    print(
                        "\n[5/5] Postgres sheet export failed; falling back to crawl JSON export ...\n",
                        flush=True,
                    )
                    sheet_exit = export_sheet_from_json(
                        root=ROOT,
                        jobs_json_path=crawl.jobs_json_path,
                        append_jobs=bool(args.append_sheet),
                        sheet_date=sheet_date,
                    )
            if sheet_exit != 0:
                return sheet_exit

        new_off = off + int(args.batch_size)
        _save_state({"source_offset": new_off, "last_batch_id": crawl.batch_id})
        print(f"Saved checkpoint: next source_offset={new_off}")

        _ = PipelineResult(
            crawl=crawl,
            mongo=mongo_stats,
            ml=ml_stats,
            postgres=pg_stats,
            sheet_export_ran=not args.no_sheet,
            sheet_exit_code=sheet_exit,
            verified_rows=verified_rows,
        )

    if args.share_students:
        print("\n[6/6] Share matched jobs with students ...\n", flush=True)
        share_exit = share_jobs_with_students(
            root=ROOT,
            dry_run=bool(args.share_dry_run),
            student_limit=int(args.share_student_limit),
            max_jobs_per_student=int(args.share_max_jobs_per_student),
            min_score=int(args.share_min_score),
            send_limit=int(args.share_send_limit),
            job_date=(args.share_job_date or now_ist().strftime("%Y-%m-%d")),
        )
        if share_exit != 0:
            if args.strict_share_step:
                return share_exit
            print(
                f"[WARN] Share step failed with exit={share_exit}; continuing pipeline "
                "(use --strict-share-step to fail hard).",
                flush=True,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
