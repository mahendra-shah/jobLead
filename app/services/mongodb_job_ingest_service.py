"""MongoDB `job_ingest` — crawled web/board jobs before ML and verified JSON export."""

from __future__ import annotations

import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

from app.config import settings
from app.utils.job_dedupe import build_text_for_ml, compute_dedupe_key, normalize_url

logger = logging.getLogger(__name__)

JOB_INGEST_COLLECTION_DEFAULT = "job_ingest"


def _trim_payload(job: Dict[str, Any], max_desc: int = 12000) -> Dict[str, Any]:
    out = dict(job)
    d = out.get("description")
    if isinstance(d, str) and len(d) > max_desc:
        out["description"] = d[:max_desc]
    return out


class MongoJobIngestService:
    def __init__(self, collection_name: Optional[str] = None) -> None:
        self.collection_name = collection_name or getattr(
            settings, "JOB_INGEST_COLLECTION", JOB_INGEST_COLLECTION_DEFAULT
        )
        self._client: Optional[MongoClient] = None
        self._db = None
        self._col = None

    def _connect(self) -> None:
        if self._client is not None:
            return

        def _try_connect_with_retries(uri: str, *, label: str) -> Optional[MongoClient]:
            attempts = 4
            backoff_seconds = [1.5, 3.0, 5.0]
            last_exc: Optional[Exception] = None

            for i in range(attempts):
                try:
                    client = MongoClient(
                        uri,
                        serverSelectionTimeoutMS=8000,
                        connectTimeoutMS=8000,
                        socketTimeoutMS=25000,
                    )
                    client.admin.command("ping")
                    if i > 0:
                        logger.info("Mongo %s connection recovered on retry %d/%d", label, i + 1, attempts)
                    return client
                except Exception as exc:
                    last_exc = exc
                    if i < attempts - 1:
                        sleep_s = backoff_seconds[min(i, len(backoff_seconds) - 1)]
                        logger.warning(
                            "Mongo %s connect attempt %d/%d failed: %s; retrying in %.1fs",
                            label,
                            i + 1,
                            attempts,
                            str(exc),
                            sleep_s,
                        )
                        time.sleep(sleep_s)

            logger.error(
                "Mongo %s connect failed after %d attempts: %s",
                label,
                attempts,
                str(last_exc) if last_exc else "unknown error",
            )
            return None

        uri_primary = settings.MONGODB_URI
        client = _try_connect_with_retries(uri_primary, label="primary")

        if (
            client is None
            and uri_primary.startswith("mongodb://localhost")
            and settings.MONGODB_ATLAS_FALLBACK
        ):
            if settings.MONGODB_USERNAME and settings.MONGODB_PASSWORD:
                uri_atlas = (
                    f"mongodb+srv://{settings.MONGODB_USERNAME}:{settings.MONGODB_PASSWORD}@"
                    f"{settings.MONGODB_CLUSTER}/?retryWrites=true&w=majority"
                )
            else:
                uri_atlas = f"mongodb+srv://{settings.MONGODB_CLUSTER}/?retryWrites=true&w=majority"
            logger.warning("Local Mongo not reachable; falling back to Atlas URI.")
            client = _try_connect_with_retries(uri_atlas, label="atlas-fallback")

        if client is None:
            raise RuntimeError(f"MongoDB connection failed for uri: {uri_primary!r}")

        self._client = client
        self._db = self._client[settings.MONGODB_DATABASE]
        self._col = self._db[self.collection_name]

    def _ensure_indexes(self) -> None:
        self._connect()
        assert self._col is not None
        self._col.create_index("dedupe_key", unique=True, background=True)
        self._col.create_index([("ml_status", ASCENDING), ("created_at", ASCENDING)], background=True)
        self._col.create_index("url_norm", background=True)
        self._col.create_index("updated_at", background=True)

    def upsert_from_crawl(
        self,
        job: Dict[str, Any],
        *,
        crawl_batch_id: str,
        source_platform: str = "job_board",
    ) -> str:
        """
        Upsert one crawled job. New docs start as ml_status=pending.
        Increments seen_count and refreshes payload / last_seen_at on repeats.
        Returns dedupe_key.
        """
        self._ensure_indexes()
        assert self._col is not None

        url = str(job.get("apply_url") or job.get("url") or "").strip()
        url_norm = normalize_url(url)
        url_key = hashlib.sha256(url_norm.encode("utf-8")).hexdigest() if url_norm else ""
        dedupe_key = compute_dedupe_key(job)
        text_for_ml = build_text_for_ml(job) or url_norm or dedupe_key
        payload = _trim_payload(job)
        now = datetime.now(timezone.utc)

        source_ref = {
            "source_domain": job.get("source_domain"),
            "crawl_batch_id": crawl_batch_id,
        }

        set_doc: Dict[str, Any] = {
            "url_norm": url_norm,
            "dedupe_key": dedupe_key,
            "source_platform": source_platform,
            "source_ref": source_ref,
            "payload": payload,
            "text_for_ml": text_for_ml,
            "updated_at": now,
            "last_seen_at": now,
        }
        if url_key:
            set_doc["url_key"] = url_key
        update_doc: Dict[str, Any] = {
            "$set": set_doc,
            "$setOnInsert": {
                "ml_status": "pending",
                "first_seen_at": now,
                "created_at": now,
                "ml_scores": {},
            },
            "$inc": {"seen_count": 1},
        }
        if not url_key:
            update_doc["$unset"] = {"url_key": ""}

        def _touch_by_url_key() -> bool:
            """Update existing row by canonical URL hash; never rewrite dedupe_key here."""
            if not url_key:
                return False
            safe_set_doc = {k: v for k, v in set_doc.items() if k != "dedupe_key"}
            r_url = self._col.update_one(
                {"url_key": url_key},
                {"$set": safe_set_doc, "$inc": {"seen_count": 1}},
                upsert=False,
            )
            return bool(r_url.matched_count)

        # Avoid $or + upsert here: it can attempt insert and hit unique url_key duplicates.
        if _touch_by_url_key():
            return dedupe_key

        try:
            self._col.update_one({"dedupe_key": dedupe_key}, update_doc, upsert=True)
        except (OperationFailure, DuplicateKeyError) as exc:
            code = getattr(exc, "code", None)
            if code is None and hasattr(exc, "details") and isinstance(getattr(exc, "details", None), dict):
                code = exc.details.get("code")
            if code != 11000:
                raise
            if _touch_by_url_key():
                logger.debug(
                    "job_ingest upsert recovered after duplicate key (url_key=%s...)",
                    (url_key or "")[:16],
                )
                return dedupe_key
            r_dk = self._col.update_one(
                {"dedupe_key": dedupe_key},
                {"$set": set_doc, "$inc": {"seen_count": 1}},
                upsert=False,
            )
            if not r_dk.matched_count:
                raise
            logger.debug(
                "job_ingest upsert recovered after duplicate key (dedupe_key=%s...)",
                (dedupe_key or "")[:16],
            )
        return dedupe_key

    def claim_next_pending(self) -> Optional[Dict[str, Any]]:
        """Atomically move one doc from pending → processing. Returns doc or None."""
        self._ensure_indexes()
        assert self._col is not None
        now = datetime.now(timezone.utc)
        return self._col.find_one_and_update(
            {"ml_status": "pending"},
            {
                "$set": {
                    "ml_status": "processing",
                    "processing_started_at": now,
                }
            },
            sort=[("created_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    def set_ml_outcome(
        self,
        dedupe_key: str,
        *,
        ml_status: str,
        ml_scores: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._ensure_indexes()
        assert self._col is not None
        now = datetime.now(timezone.utc)
        update: Dict[str, Any] = {
            "$set": {
                "ml_status": ml_status,
                "updated_at": now,
            }
        }
        if ml_scores is not None:
            update["$set"]["ml_scores"] = ml_scores
        self._col.update_one({"dedupe_key": dedupe_key}, update)

    def list_verified_payloads(self, *, limit: int = 50000) -> List[Dict[str, Any]]:
        """Return payload + ml_scores for verified jobs (for JSON export)."""
        self._ensure_indexes()
        assert self._col is not None
        out: List[Dict[str, Any]] = []
        for doc in self._col.find({"ml_status": "verified"}).sort("updated_at", -1).limit(int(limit)):
            row = dict(doc.get("payload") or {})
            row["_ml_scores"] = doc.get("ml_scores") or {}
            row["_dedupe_key"] = doc.get("dedupe_key")
            row["_verified_at"] = doc.get("updated_at")
            out.append(row)
        return out

    def count_by_status(self) -> Dict[str, int]:
        self._ensure_indexes()
        assert self._col is not None
        pipeline = [{"$group": {"_id": "$ml_status", "n": {"$sum": 1}}}]
        agg = list(self._col.aggregate(pipeline))
        return {str(x["_id"] or "unknown"): int(x["n"]) for x in agg}
