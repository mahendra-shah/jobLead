"""Process Mongo `job_ingest` pending documents: quality → ML → India gate → enrich → Postgres (+ JSONL fallback)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.exc import OperationalError, ProgrammingError

from app.services.job_board_ml.classifier import JobBoardIngestClassifier
from app.services.job_board_ml.enrichment import enrich_job_board_payload
from app.services.job_board_ml.fallback import append_rows_jsonl
from app.services.job_board_ml.postgres_sync import build_sync_engine, sync_job_board_rows
from app.services.job_board_ml.quality import quality_check, resolve_apply_url
from app.services.job_board_ml.depth_profile import evaluate_depth_profile
from app.services.job_board_ml.telegram_aligned_gates import (
    board_requires_title_and_company,
    build_job_data_for_quality_scorer,
    experience_exceeds_fresher_cap,
    is_non_job_spam,
    payload_has_apply_contact,
)
from app.services.job_quality_scorer import get_quality_scorer
from app.services.mongodb_job_ingest_service import MongoJobIngestService
from app.config import settings
from app.utils.india_job_gate import passes_india_relevance
from app.utils.job_dedupe import build_text_for_ml


def _row_for_postgres(payload: dict[str, Any], dedupe_key: str, ml_scores: dict[str, Any]) -> dict[str, Any]:
    row = dict(payload)
    row["_dedupe_key"] = dedupe_key
    row["_ml_scores"] = dict(ml_scores)
    return row


def run_job_board_ingest_ml(
    *,
    limit: int = 500,
    min_confidence: float = 0.5,
    strict_india: bool = True,
    sync_postgres: bool = True,
    postgres_batch_size: int = 40,
    fallback_jsonl: Path | None = None,
    recover_stale_minutes: int = 0,
    depth_profile_enabled: bool | None = None,
    require_remote_signal: bool | None = None,
    require_role_track: bool | None = None,
) -> int:
    """
    Claim up to `limit` pending docs, classify, write outcomes to Mongo, optionally upsert Postgres.

    Returns process exit code (0 ok, 1 hard failure e.g. Mongo down).
    """
    use_depth = (
        settings.JOB_BOARD_DEPTH_PROFILE_ENABLED
        if depth_profile_enabled is None
        else depth_profile_enabled
    )
    use_remote = (
        settings.JOB_BOARD_REQUIRE_REMOTE_SIGNAL
        if require_remote_signal is None
        else require_remote_signal
    )
    use_track = (
        settings.JOB_BOARD_REQUIRE_ROLE_TRACK_MATCH
        if require_role_track is None
        else require_role_track
    )

    svc = MongoJobIngestService()
    clf = JobBoardIngestClassifier()
    quality_scorer = get_quality_scorer()

    if recover_stale_minutes > 0:
        n = svc.reset_stale_processing(max_age_minutes=recover_stale_minutes)
        if n:
            print(f"job_board_ml: reset_stale_processing n={n}", flush=True)

    engine = None
    if sync_postgres:
        try:
            engine = build_sync_engine()
        except Exception as e:
            print(f"job_board_ml: Postgres engine failed ({e}); continuing Mongo-only + fallback if any.", flush=True)
            sync_postgres = False

    verified_pending_pg: list[dict[str, Any]] = []
    stats = {
        "processed": 0,
        "verified": 0,
        "rejected": 0,
        "spam_prefilter": 0,
        "no_apply_contact": 0,
        "title_company": 0,
        "experience_cap": 0,
        "depth_gate": 0,
        "quality_gate": 0,
        "postgres_inserted": 0,
        "postgres_updated": 0,
        "postgres_skipped": 0,
        "fallback_appended": 0,
    }

    def flush_postgres_buffer(reason_if_fail: str = "") -> None:
        nonlocal verified_pending_pg
        if not verified_pending_pg or not sync_postgres or engine is None:
            verified_pending_pg = []
            return
        batch = verified_pending_pg
        verified_pending_pg = []
        try:
            s = sync_job_board_rows(engine, batch, batch_size=postgres_batch_size)
            stats["postgres_inserted"] += s.inserted
            stats["postgres_updated"] += s.updated
            stats["postgres_skipped"] += s.skipped
        except ProgrammingError as e:
            print(f"job_board_ml: Postgres schema/programming error ({e}); not using JSON fallback.", flush=True)
            raise
        except OperationalError as e:
            msg = reason_if_fail or str(e)
            print(f"job_board_ml: Postgres unavailable ({msg}); writing {len(batch)} rows to fallback", flush=True)
            if fallback_jsonl:
                append_rows_jsonl(fallback_jsonl, batch, reason=msg)
                stats["fallback_appended"] += len(batch)
            else:
                print("job_board_ml: no fallback_jsonl path set; rows are only in Mongo as verified.", flush=True)

    try:
        for _ in range(int(limit)):
            doc = svc.claim_next_pending()
            if not doc:
                break
            stats["processed"] += 1
            dedupe_key = str(doc.get("dedupe_key") or "")
            payload: dict[str, Any] = dict(doc.get("payload") or {})

            ok_q, reason_q = quality_check(payload)
            if not ok_q:
                svc.set_ml_outcome(
                    dedupe_key,
                    ml_status="rejected",
                    ml_scores={"reason": "quality_gate", "detail": reason_q},
                )
                stats["rejected"] += 1
                continue

            abs_url = resolve_apply_url(payload)
            payload["apply_url"] = abs_url
            if not str(payload.get("url") or "").startswith("http"):
                payload["url"] = abs_url

            enrich_patch = enrich_job_board_payload(payload)
            if enrich_patch:
                svc.patch_payload(dedupe_key, enrich_patch)
                payload.update(enrich_patch)

            text = build_text_for_ml(payload)
            spam_label = is_non_job_spam(text)
            if spam_label:
                svc.set_ml_outcome(
                    dedupe_key,
                    ml_status="rejected",
                    ml_scores={
                        "reason": "telegram_aligned_prefilter",
                        "detail": spam_label,
                        "classifier": "job_board_ingest_sklearn",
                    },
                )
                stats["rejected"] += 1
                stats["spam_prefilter"] += 1
                continue

            cr = clf.classify(text)
            ml_scores: dict[str, Any] = {
                "reason": cr.reason,
                "confidence": float(cr.confidence),
                "is_job": bool(cr.is_job),
                "classifier": "job_board_ingest_sklearn",
            }
            if cr.features_used is not None:
                ml_scores["features_used"] = cr.features_used

            if not cr.is_job or cr.confidence < float(min_confidence):
                ml_scores["reason_profile"] = "below_threshold_or_not_job"
                svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                stats["rejected"] += 1
                continue

            if not payload_has_apply_contact(payload, text):
                ml_scores["reason_profile"] = "no_apply_link_or_email"
                svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                stats["rejected"] += 1
                stats["no_apply_contact"] += 1
                continue

            if use_depth:
                ok_d, reason_d, depth_det = evaluate_depth_profile(
                    payload,
                    max_fresher_years=float(settings.MAX_FRESHER_EXPERIENCE_YEARS),
                    require_remote_signal=bool(use_remote),
                    require_role_track=bool(use_track),
                )
                ml_scores["depth_profile"] = depth_det
                if not ok_d:
                    ml_scores["reason_profile"] = f"depth_{reason_d}"
                    svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                    stats["rejected"] += 1
                    stats["depth_gate"] += 1
                    continue
            else:
                ok_tc, reason_tc = board_requires_title_and_company(payload)
                if not ok_tc:
                    ml_scores["reason_profile"] = reason_tc
                    svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                    stats["rejected"] += 1
                    stats["title_company"] += 1
                    continue

                if experience_exceeds_fresher_cap(payload, settings.MAX_FRESHER_EXPERIENCE_YEARS):
                    ml_scores["reason_profile"] = "experience_above_fresher_cap"
                    ml_scores["fresher_cap_years"] = settings.MAX_FRESHER_EXPERIENCE_YEARS
                    svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                    stats["rejected"] += 1
                    stats["experience_cap"] += 1
                    continue

            if strict_india and not passes_india_relevance(payload):
                ml_scores["reason_profile"] = "failed_india_relevance"
                svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                stats["rejected"] += 1
                continue

            job_data = build_job_data_for_quality_scorer(
                payload, settings.MAX_FRESHER_EXPERIENCE_YEARS
            )
            q = quality_scorer.score_job(job_data, float(cr.confidence))
            ml_scores["quality_score"] = float(q.quality_score)
            ml_scores["meets_criteria"] = bool(q.meets_criteria)
            if not q.meets_criteria:
                ml_scores["reason_profile"] = "quality_relevance_failed"
                ml_scores["quality_fail_reasons"] = q.reasons[:12]
                svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                stats["rejected"] += 1
                stats["quality_gate"] += 1
                continue
            if q.quality_score < float(settings.JOB_QUALITY_MIN_SCORE):
                ml_scores["reason_profile"] = "quality_score_below_min"
                ml_scores["quality_min"] = settings.JOB_QUALITY_MIN_SCORE
                svc.set_ml_outcome(dedupe_key, ml_status="rejected", ml_scores=ml_scores)
                stats["rejected"] += 1
                stats["quality_gate"] += 1
                continue

            svc.set_ml_outcome(dedupe_key, ml_status="verified", ml_scores=ml_scores)
            stats["verified"] += 1

            if sync_postgres and engine is not None:
                verified_pending_pg.append(_row_for_postgres(payload, dedupe_key, ml_scores))
                if len(verified_pending_pg) >= postgres_batch_size:
                    flush_postgres_buffer()

        flush_postgres_buffer()

    finally:
        if engine is not None:
            engine.dispose()

    print(
        "job_board_ml: done "
        f"processed={stats['processed']} verified={stats['verified']} rejected={stats['rejected']} "
        f"spam_pre={stats['spam_prefilter']} no_apply={stats['no_apply_contact']} "
        f"title_co={stats['title_company']} exp_cap={stats['experience_cap']} "
        f"depth={stats['depth_gate']} qual={stats['quality_gate']} "
        f"pg_ins={stats['postgres_inserted']} pg_upd={stats['postgres_updated']} pg_skip={stats['postgres_skipped']} "
        f"fallback_n={stats['fallback_appended']}",
        flush=True,
    )

    # Soft warning: model missing — still exit 0 if we processed nothing vs errored
    if not clf.is_loaded:
        print(
            "job_board_ml: WARNING job-board classifier not loaded; "
            f"add {settings.ML_JOB_BOARD_CLASSIFIER_BASENAME} "
            f"(or legacy {settings.ML_CLASSIFIER_LEGACY_BASENAME}) under app/ml/models/",
            flush=True,
        )

    return 0
