#!/usr/bin/env python3
"""
Drain Mongo job_ingest: pending → processing → verified | rejected.

Uses SklearnClassifier (if loaded) + same profile rules as merge_job_runs._profile_filters.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.sklearn_classifier import SklearnClassifier
from app.services.mongodb_job_ingest_service import MongoJobIngestService
from app.utils.india_job_gate import passes_india_relevance
from scripts.merge_job_runs import _profile_filters


def main() -> int:
    parser = argparse.ArgumentParser(description="ML + profile gate for Mongo job_ingest")
    parser.add_argument("--limit", type=int, default=200, help="Max documents to process this run")
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.55,
        help="Min classifier confidence when model is loaded (ignored if model missing)",
    )
    parser.add_argument(
        "--no-strict-india",
        action="store_true",
        help="Disable India-only gate (default: only India on-site or India-tied remote/hybrid)",
    )
    args = parser.parse_args()
    strict_india = not bool(args.no_strict_india)

    ingest = MongoJobIngestService()
    clf = SklearnClassifier()

    verified = 0
    rejected = 0
    errors = 0

    for _ in range(max(0, int(args.limit))):
        doc = ingest.claim_next_pending()
        if not doc:
            break
        dk = doc.get("dedupe_key") or ""
        text = (doc.get("text_for_ml") or "").strip()
        payload = doc.get("payload") or {}

        ml_scores: dict = {
            "model_loaded": bool(clf.is_loaded),
            "model_version": getattr(clf, "model_version", None),
        }

        try:
            if not text:
                ingest.set_ml_outcome(
                    dk,
                    ml_status="rejected",
                    ml_scores={**ml_scores, "reason": "empty_text_for_ml"},
                )
                rejected += 1
                continue

            if clf.is_loaded:
                result = clf.classify(text)
                ml_scores["is_job"] = result.is_job
                ml_scores["confidence"] = result.confidence
                ml_scores["reason"] = result.reason
                if not result.is_job or float(result.confidence or 0) < float(args.min_confidence):
                    ingest.set_ml_outcome(dk, ml_status="rejected", ml_scores=ml_scores)
                    rejected += 1
                    continue
            else:
                ml_scores["is_job"] = True
                ml_scores["confidence"] = None
                ml_scores["reason"] = "classifier_not_loaded_skip_to_profile"

            kept = _profile_filters([payload])
            if not kept:
                ingest.set_ml_outcome(
                    dk,
                    ml_status="rejected",
                    ml_scores={**ml_scores, "reason_profile": "failed_target_profile_rules"},
                )
                rejected += 1
            elif strict_india and not passes_india_relevance(payload):
                ingest.set_ml_outcome(
                    dk,
                    ml_status="rejected",
                    ml_scores={
                        **ml_scores,
                        "reason_profile": "failed_india_relevance",
                    },
                )
                rejected += 1
            else:
                ingest.set_ml_outcome(
                    dk,
                    ml_status="verified",
                    ml_scores={
                        **ml_scores,
                        "reason_profile": "passed",
                        "india_gate": "strict" if strict_india else "off",
                    },
                )
                verified += 1
        except Exception as e:
            errors += 1
            ingest.set_ml_outcome(
                dk,
                ml_status="error",
                ml_scores={**ml_scores, "error": str(e)[:500]},
            )

    counts = ingest.count_by_status()
    print(
        f"Processed batch: verified={verified} rejected={rejected} errors={errors} | "
        f"collection_counts={counts}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
