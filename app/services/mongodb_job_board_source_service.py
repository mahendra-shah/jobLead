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

logger = logging.getLogger(__name__)


JOB_BOARD_SOURCES_COLLECTION = "job_board_sources"


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

        if client is None and uri_primary.startswith("mongodb://localhost"):
            # Atlas fallback (when local Mongo isn't started).
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
            raise RuntimeError(f"MongoDB connection failed for primary uri: {uri_primary}")

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

        doc = {
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

        ready_norms: List[str] = []

        for s in sources:
            if limit is not None and upserted >= int(limit):
                break
            m = s.get("metadata") or {}
            strategy = m.get("crawl_strategy") or {}
            if not strategy.get("crawl_ready"):
                skipped += 1
                continue

            domain = (s.get("domain") or "").strip()
            url = (s.get("url") or "").strip()
            if not domain or not url:
                skipped += 1
                continue

            url_norm = _normalize_url(url)
            ready_norms.append(url_norm)

            # Heuristic category/region defaults since crawler JSON doesn't always carry them.
            name_l = (s.get("name") or "").lower()
            domain_l = domain.lower()
            nontech_markers = ("marketing", "sales", "hr", "recruit", "recruiter", "data entry", "customer support", "customer care", "finance", "accountant")
            category = "non-tech" if any(x in name_l for x in nontech_markers) or any(x in domain_l for x in nontech_markers) else "tech"

            # Region: only India is explicit in your artifacts; everything else becomes Global.
            raw_country = (s.get("country") or "").strip()
            region = "India" if raw_country == "India" else "Global"

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
            )
            upserted += 1

        # Remove sources that are no longer crawl-ready (optional).
        if delete_non_crawl_ready:
            assert self._col is not None
            # Keep only current crawl-ready sources from this import.
            # (This deletes stale sources that were crawl-ready earlier but are not in the latest JSON.)
            self._col.delete_many({"crawl_ready": True, "url_norm": {"$nin": ready_norms}})

        return {"upserted": upserted, "skipped": skipped, "ready_total": len(ready_norms)}

    def get_crawl_ready_sources(
        self,
        *,
        limit: int = 30,
        region: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        assert self._col is not None

        query: Dict[str, Any] = {"crawl_ready": True, "status": "active"}
        if region:
            query["region"] = region

        docs = (
            self._col.find(query)
            .sort([("health_score", -1)])
            .limit(int(limit))
        )
        return list(docs)

