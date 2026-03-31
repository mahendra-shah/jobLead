"""MongoDB storage for job-board sources (Phase 1 crawl-ready sources).

Collection: job_board_sources (configurable later if needed)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, MongoClient

from app.config import settings
from app.services.job_board_source_health import check_source_health
from app.utils.phase1_source_profile import (
    build_phase1_metadata_extra,
    classify_category,
    infer_region_label,
    student_pipeline_eligible,
)

logger = logging.getLogger(__name__)


JOB_BOARD_SOURCES_COLLECTION = "job_board_sources"


def _normalize_domain_key(doc: Dict[str, Any]) -> str:
    """Stable key so one batch does not crawl the same board twice via duplicate Mongo rows."""
    d = str(doc.get("domain") or "").strip().lower()
    if d:
        return f"domain:{d}"
    u = doc.get("url_norm") or _normalize_url(str(doc.get("url") or ""))
    if u:
        return f"url:{u}"
    return f"id:{doc.get('_id')}"


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip().lower()
    # Strip query params/fragments for stable uniqueness.
    # Keep path.
    u = u.split("#", 1)[0]
    u = u.split("?", 1)[0]
    if u.endswith("/"):
        u = u[:-1]
    return u


class MongoJobBoardSourcesService:
    def __init__(self, collection_name: str = JOB_BOARD_SOURCES_COLLECTION) -> None:
        self.collection_name = collection_name
        self._client: Optional[MongoClient] = None
        self._db = None
        self._col = None

    def _connect(self) -> None:
        if self._client is not None:
            return
        # Fail fast; if local Mongo isn't running, optionally fall back to Atlas.
        def _try_connect(uri: str) -> Optional[MongoClient]:
            try:
                client = MongoClient(
                    uri,
                    serverSelectionTimeoutMS=2000,
                    connectTimeoutMS=2000,
                    socketTimeoutMS=2000,
                )
                client.admin.command("ping")
                return client
            except Exception:
                return None

        uri_primary = settings.MONGODB_URI
        client = _try_connect(uri_primary)

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
            client = _try_connect(uri_atlas)

        if client is None:
            hint = (
                " Start mongod (e.g. sudo systemctl start mongod) or fix MONGODB_URI in .env. "
                "Optional: MONGODB_ATLAS_FALLBACK=true if you use Atlas when local is down."
            )
            raise RuntimeError(f"MongoDB connection failed for uri: {uri_primary!r}.{hint}")

        self._client = client
        self._db = self._client[settings.MONGODB_DATABASE]
        self._col = self._db[self.collection_name]

    def _ensure_indexes(self) -> None:
        self._connect()
        assert self._col is not None

        # Unique on normalized URL to avoid duplicates for the same source.
        # We store normalized URL in `url_norm`.
        self._col.create_index("url_norm", unique=True, background=True)

        # Query acceleration
        self._col.create_index([("crawl_ready", ASCENDING), ("status", ASCENDING)], background=True)
        self._col.create_index([("category", ASCENDING), ("region", ASCENDING)], background=True)
        self._col.create_index("student_pipeline_eligible", background=True)
        self._col.create_index("domain", background=True)
        self._col.create_index("last_health_check_at", background=True)

    def upsert_source(
        self,
        *,
        domain: str,
        url: str,
        name: str,
        source_type: str = "job_board",
        category: str = "tech",
        region: Optional[str] = None,
        city: Optional[str] = None,
        discovered_from: Optional[str] = None,
        discovered_at: Optional[datetime] = None,
        crawl_ready: bool,
        status: str = "active",
        last_crawled_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
        health_check: bool = False,
        student_pipeline_eligible: Optional[bool] = None,
    ) -> None:
        self._ensure_indexes()
        assert self._col is not None

        now = datetime.now(timezone.utc)
        url_norm = _normalize_url(url)
        metadata = dict(metadata or {})

        health_fields: Dict[str, Any] = {}
        if health_check:
            try:
                h = check_source_health(url, use_get=True)
                health_fields = {
                    "health_score": float(h.get("health_score") or 0.0),
                    "last_health_check_at": h.get("last_health_check_at") or now,
                }
            except Exception:
                health_fields = {"health_score": 0.0, "last_health_check_at": now}
        else:
            # Keep existing health if present; otherwise set defaults.
            health_fields = {"health_score": 100.0, "last_health_check_at": now}

        doc: Dict[str, Any] = {
            "domain": domain,
            "url": url,
            "url_norm": url_norm,
            "name": name,
            "source_type": source_type,
            "category": category,
            "region": region,
            "city": city,
            "discovered_from": discovered_from,
            "discovered_at": discovered_at,
            "crawl_ready": crawl_ready,
            "status": status,
            "last_crawled_at": last_crawled_at,
            "metadata": metadata,
            **health_fields,
            "updated_at": now,
        }
        if student_pipeline_eligible is not None:
            doc["student_pipeline_eligible"] = bool(student_pipeline_eligible)

        # created_at must not be overwritten on update.
        update_doc = {"$set": doc, "$setOnInsert": {"created_at": now}}

        self._col.update_one({"url_norm": url_norm}, update_doc, upsert=True)

    def import_crawl_ready_sources_from_json(
        self,
        crawl_ready_sources_path: str,
        *,
        health_check: bool = False,
        delete_non_crawl_ready: bool = True,
        limit: Optional[int] = None,
        student_pipeline_only: bool = True,
    ) -> Dict[str, int]:
        """
        Upsert crawl-ready sources from the existing JSON artifact.
        Optionally delete documents that are currently not crawl_ready anymore.
        """
        import json
        from pathlib import Path

        p = Path(crawl_ready_sources_path)
        data = json.loads(p.read_text(encoding="utf-8"))
        sources = data.get("sources") or []

        upserted = 0
        skipped = 0
        skipped_audience = 0

        ready_norms: List[str] = []

        for s in sources:
            if limit is not None and upserted >= int(limit):
                break
            m = dict(s.get("metadata") or {})
            strategy = m.get("crawl_strategy") or {}
            if not strategy.get("crawl_ready"):
                skipped += 1
                continue

            domain = (s.get("domain") or "").strip()
            url = (s.get("url") or "").strip()
            if not domain or not url:
                skipped += 1
                continue

            eligible = student_pipeline_eligible(s)
            if student_pipeline_only and not eligible:
                skipped_audience += 1
                continue

            url_norm = _normalize_url(url)
            ready_norms.append(url_norm)

            phase1_extra = build_phase1_metadata_extra(s)
            m.setdefault("phase1", {}).update(phase1_extra.get("phase1") or {})

            category = classify_category(s)
            region = infer_region_label(s)

            self.upsert_source(
                domain=domain,
                url=url,
                name=s.get("name") or domain,
                source_type="job_board",
                category=category,
                region=region,
                city=s.get("city"),
                discovered_from=(m.get("discovery_origin") or None),
                discovered_at=None,
                crawl_ready=True,
                status=s.get("status") or "active",
                last_crawled_at=None,
                metadata=m,
                health_check=health_check,
                student_pipeline_eligible=eligible,
            )
            upserted += 1

        # Remove sources that are no longer crawl-ready (optional).
        if delete_non_crawl_ready:
            assert self._col is not None
            # Keep only current crawl-ready sources from this import.
            # (This deletes stale sources that were crawl-ready earlier but are not in the latest JSON.)
            self._col.delete_many({"crawl_ready": True, "url_norm": {"$nin": ready_norms}})

        return {
            "upserted": upserted,
            "skipped": skipped,
            "skipped_audience": skipped_audience,
            "ready_total": len(ready_norms),
        }

    def get_crawl_ready_sources(
        self,
        *,
        limit: int = 30,
        region: Optional[str] = None,
        student_pipeline_only: bool = True,
    ) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        assert self._col is not None

        query: Dict[str, Any] = {"crawl_ready": True, "status": "active"}
        if student_pipeline_only:
            query["student_pipeline_eligible"] = True
        if region:
            query["region"] = region

        docs = (
            self._col.find(query)
            .sort([("health_score", -1)])
            .limit(int(limit))
        )
        return list(docs)

    @staticmethod
    def mongo_doc_to_phase2_source(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Shape expected by scripts/crawl_jobs_from_sources.py (matches JSON export)."""
        _id = doc.get("_id")
        sid = str(_id) if _id is not None else ""
        meta = dict(doc.get("metadata") or {})
        # Preserve discovery country if it lived only at top level in older imports
        if doc.get("city") and not meta.get("city"):
            meta = {**meta, "city": doc.get("city")}
        return {
            "id": sid,
            "domain": doc.get("domain") or "",
            "url": doc.get("url") or "",
            "name": doc.get("name") or "",
            "city": doc.get("city"),
            "country": meta.get("country") or ("India" if doc.get("region") == "India" else None),
            "metadata": meta,
            "status": doc.get("status") or "active",
            "student_pipeline_eligible": doc.get("student_pipeline_eligible"),
        }

    def _ordered_crawl_ready_docs(
        self,
        *,
        student_pipeline_priority: bool = True,
        student_pipeline_only: bool = False,
    ) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        assert self._col is not None

        q: Dict[str, Any] = {"crawl_ready": True, "status": "active"}
        if student_pipeline_only:
            q["student_pipeline_eligible"] = True

        cursor = self._col.find(q).sort([("health_score", -1)])
        all_docs = list(cursor)

        if not student_pipeline_priority or student_pipeline_only:
            merged = all_docs
        else:
            eligible = [d for d in all_docs if d.get("student_pipeline_eligible") is True]
            ineligible = [d for d in all_docs if d.get("student_pipeline_eligible") is not True]
            merged = eligible + ineligible

        seen: set[str] = set()
        out: List[Dict[str, Any]] = []
        for d in merged:
            key = _normalize_domain_key(d)
            if key in seen:
                continue
            seen.add(key)
            out.append(d)
        return out

    def get_phase2_crawl_queue(
        self,
        *,
        limit: int,
        student_pipeline_priority: bool = True,
        student_pipeline_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Sources for Phase 2 job crawl, ordered for your product defaults:
        - student_pipeline_priority=True: India/remote boards first (student_pipeline_eligible),
          then others, each bucket by health_score desc.
        - student_pipeline_only=True: only eligible rows (no fallback).
        """
        ordered = self._ordered_crawl_ready_docs(
            student_pipeline_priority=student_pipeline_priority,
            student_pipeline_only=student_pipeline_only,
        )
        return [self.mongo_doc_to_phase2_source(d) for d in ordered[: int(limit)]]

    def get_phase2_crawl_queue_slice(
        self,
        *,
        offset: int,
        limit: int,
        student_pipeline_priority: bool = True,
        student_pipeline_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Same order as get_phase2_crawl_queue but windowed for batched daily runs."""
        ordered = self._ordered_crawl_ready_docs(
            student_pipeline_priority=student_pipeline_priority,
            student_pipeline_only=student_pipeline_only,
        )
        off = max(0, int(offset))
        lim = max(0, int(limit))
        chunk = ordered[off : off + lim]
        return [self.mongo_doc_to_phase2_source(d) for d in chunk]

    def count_crawl_ready_active(
        self,
        *,
        student_pipeline_priority: bool = True,
        student_pipeline_only: bool = False,
    ) -> int:
        """Length of the Phase-2 queue (unique domain / URL key), not raw Mongo row count."""
        return len(
            self._ordered_crawl_ready_docs(
                student_pipeline_priority=student_pipeline_priority,
                student_pipeline_only=student_pipeline_only,
            )
        )

