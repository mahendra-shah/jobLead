"""Mongo schema (Pydantic) for job-board sources used in Phase 1/2.

This is intentionally a lightweight schema:
- core identity fields (domain/url/name)
- crawl readiness + crawl strategy metadata
- minimal "Telegram-style" health check fields
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from bson import ObjectId
from pydantic import BaseModel, Field


class JobBoardSourceCategory(str):
    # Use simple string union for readability (avoid enum imports).
    TECH = "tech"
    NONTECH = "non-tech"


class JobBoardSourceBase(BaseModel):
    # Identity
    domain: str
    url: str
    name: str
    source_type: Literal["job_board"] = "job_board"

    # Classification / targeting
    category: str = JobBoardSourceCategory.TECH  # "tech" | "non-tech"
    region: Optional[str] = None  # e.g. "India" | "Global"
    city: Optional[str] = None

    # Discovery
    discovered_from: Optional[str] = None
    discovered_at: Optional[datetime] = None

    # Crawl state
    crawl_ready: bool = False
    # Phase 1: India or remote-audience boards only (see phase1_source_profile)
    student_pipeline_eligible: Optional[bool] = None
    status: str = "active"
    last_crawled_at: Optional[datetime] = None

    # Minimal health check (Telegram-style)
    health_score: float = Field(default=100.0, ge=0, le=100)
    last_health_check_at: Optional[datetime] = None

    # Analyzer-derived strategy. Stored under metadata for flexibility.
    metadata: Dict[str, Any] = Field(default_factory=dict)


class JobBoardSourceCreate(JobBoardSourceBase):
    pass


class JobBoardSourceInDB(JobBoardSourceBase):
    id: Optional[str] = Field(default=None)

    @classmethod
    def from_mongo(cls, doc: Dict[str, Any]) -> "JobBoardSourceInDB":
        doc = dict(doc or {})
        _id = doc.get("_id")
        if isinstance(_id, ObjectId):
            doc["id"] = str(_id)
        doc.pop("_id", None)
        return cls(**doc)

