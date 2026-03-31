"""Discovery sources and pilot config schemas."""
from datetime import datetime
from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel


class DiscoverySourceResponse(BaseModel):
    id: UUID
    name: str
    url: str
    source_type: str
    platform: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country_code: Optional[str] = None
    phase: int
    is_shortlisted: bool
    discovered_at: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class DiscoverySourceListResponse(BaseModel):
    sources: List[DiscoverySourceResponse]
    total: int
    page: int
    page_size: int


class ShortlistResponse(BaseModel):
    shortlisted_count: int
    total_sources: int
    message: str


class SyncTelegramResponse(BaseModel):
    synced: int
    skipped: int
    errors: List[str]


class PilotCitiesResponse(BaseModel):
    india: List[str]
    outside_india: List[str]


class FresherKeywordsResponse(BaseModel):
    keywords: List[str]


class DiscoverySummaryResponse(BaseModel):
    discovery_sources_total: int
    discovery_sources_shortlisted: int
    telegram_groups_from_discovery: int
    jobs_total: int
