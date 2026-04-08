"""Step 3: direct ML classification (no Mongo status transitions)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.config import settings
from app.pipeline.job_board.contracts import MLClassifyStats
from app.services.job_board_ml.classifier import JobBoardIngestClassifier
from app.services.job_board_ml.depth_profile import evaluate_depth_profile
from app.services.job_board_ml.enrichment import enrich_job_board_payload
from app.services.job_board_ml.quality import quality_check, resolve_apply_url
from app.services.job_board_ml.telegram_aligned_gates import (
    board_requires_title_and_company,
    build_job_data_for_quality_scorer,
    experience_exceeds_fresher_cap,
    is_non_job_spam,
    payload_has_apply_contact,
)
from app.services.job_quality_scorer import get_quality_scorer
from app.utils.india_job_gate import passes_india_relevance
from app.utils.job_dedupe import build_text_for_ml, compute_dedupe_key


def _load_jobs(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("jobs") or []
    return [x for x in rows if isinstance(x, dict)]


def classify_jobs_direct(
    *,
    jobs_json_path: Path,
    min_confidence: float = 0.5,
    strict_india: bool = True,
    depth_profile_enabled: bool | None = None,
    require_remote_signal: bool | None = None,
    require_role_track: bool | None = None,
) -> tuple[list[dict[str, Any]], MLClassifyStats]:
    """
    Classify crawled jobs and return verified rows for Postgres.

    Returns:
      - verified rows with `_dedupe_key` and `_ml_scores`
      - step stats
    """
    stats = MLClassifyStats()
    verified_rows: list[dict[str, Any]] = []
    if not jobs_json_path.exists():
        return verified_rows, stats

    rows = _load_jobs(jobs_json_path)
    if not rows:
        return verified_rows, stats

    use_depth = settings.JOB_BOARD_DEPTH_PROFILE_ENABLED if depth_profile_enabled is None else depth_profile_enabled
    use_remote = settings.JOB_BOARD_REQUIRE_REMOTE_SIGNAL if require_remote_signal is None else require_remote_signal
    use_track = settings.JOB_BOARD_REQUIRE_ROLE_TRACK_MATCH if require_role_track is None else require_role_track

    clf = JobBoardIngestClassifier()
    quality_scorer = get_quality_scorer()
    for payload in rows:
        stats.processed += 1
        dedupe_key = compute_dedupe_key(payload)

        def reject(reason: str) -> None:
            stats.rejected += 1
            stats.reason_counts[reason] = stats.reason_counts.get(reason, 0) + 1

        ok_q, reason_q = quality_check(payload)
        if not ok_q:
            reject(f"quality_gate:{reason_q}")
            continue

        abs_url = resolve_apply_url(payload)
        payload["apply_url"] = abs_url
        if not str(payload.get("url") or "").startswith("http"):
            payload["url"] = abs_url

        enrich_patch = enrich_job_board_payload(payload)
        if enrich_patch:
            payload.update(enrich_patch)

        text = build_text_for_ml(payload)
        spam_label = is_non_job_spam(text)
        if spam_label:
            stats.spam_prefilter += 1
            reject(f"spam:{spam_label}")
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
            # Depth override mode: for fresher/remote/track-matched jobs, classifier can be
            # noisy on board HTML text. Allow only when depth profile is strong, then pass
            # through existing India + quality gates.
            allow_override = bool(settings.JOB_BOARD_ALLOW_DEPTH_OVERRIDE)
            if allow_override and use_depth:
                ok_d_ovr, reason_d_ovr, depth_det_ovr = evaluate_depth_profile(
                    payload,
                    max_fresher_years=float(settings.MAX_FRESHER_EXPERIENCE_YEARS),
                    require_remote_signal=bool(use_remote),
                    require_role_track=bool(use_track),
                )
                depth_chars = int(depth_det_ovr.get("depth_text_chars") or 0)
                if ok_d_ovr and depth_chars >= int(settings.JOB_BOARD_DEPTH_OVERRIDE_MIN_TEXT_CHARS):
                    ml_scores["depth_profile"] = depth_det_ovr
                    ml_scores["classifier_override"] = "depth_profile_pass"
                    ml_scores["is_job"] = True
                    ml_scores["confidence"] = max(float(cr.confidence), float(min_confidence))
                else:
                    ml_scores["depth_profile"] = depth_det_ovr
                    ml_scores["reason_profile"] = "below_threshold_or_not_job"
                    reject("below_threshold_or_not_job")
                    continue
            else:
                ml_scores["reason_profile"] = "below_threshold_or_not_job"
                reject("below_threshold_or_not_job")
                continue

        if not payload_has_apply_contact(payload, text):
            stats.no_apply_contact += 1
            ml_scores["reason_profile"] = "no_apply_link_or_email"
            reject("no_apply_link_or_email")
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
                stats.depth_gate += 1
                ml_scores["reason_profile"] = f"depth_{reason_d}"
                reject(f"depth_{reason_d}")
                continue
        else:
            ok_tc, reason_tc = board_requires_title_and_company(payload)
            if not ok_tc:
                stats.title_company += 1
                ml_scores["reason_profile"] = reason_tc
                reject(reason_tc)
                continue

            if experience_exceeds_fresher_cap(payload, settings.MAX_FRESHER_EXPERIENCE_YEARS):
                stats.experience_cap += 1
                ml_scores["reason_profile"] = "experience_above_fresher_cap"
                reject("experience_above_fresher_cap")
                continue

        if strict_india and not passes_india_relevance(payload):
            ml_scores["reason_profile"] = "failed_india_relevance"
            reject("failed_india_relevance")
            continue

        job_data = build_job_data_for_quality_scorer(payload, settings.MAX_FRESHER_EXPERIENCE_YEARS)
        q = quality_scorer.score_job(job_data, float(cr.confidence))
        ml_scores["quality_score"] = float(q.quality_score)
        ml_scores["meets_criteria"] = bool(q.meets_criteria)
        if not q.meets_criteria:
            stats.quality_gate += 1
            ml_scores["reason_profile"] = "quality_relevance_failed"
            reject("quality_relevance_failed")
            continue
        if q.quality_score < float(settings.JOB_QUALITY_MIN_SCORE):
            stats.quality_gate += 1
            ml_scores["reason_profile"] = "quality_score_below_min"
            reject("quality_score_below_min")
            continue

        row = dict(payload)
        row["_dedupe_key"] = dedupe_key
        row["_ml_scores"] = ml_scores
        verified_rows.append(row)
        stats.verified += 1

    return verified_rows, stats
