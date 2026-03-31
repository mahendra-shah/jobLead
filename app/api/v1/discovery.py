"""Discovery API: pilot config, discovery sources, shortlist, sync to Telegram groups."""
from typing import Optional
from fastapi import APIRouter, Depends, Query

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models.discovery_source import DiscoverySource
from app.schemas.discovery import (
    DiscoverySourceResponse,
    DiscoverySourceListResponse,
    ShortlistResponse,
    SyncTelegramResponse,
    PilotCitiesResponse,
    FresherKeywordsResponse,
    DiscoverySummaryResponse,
)
from app.models.job import Job
from app.services.discovery_service import (
    run_shortlist_heuristics,
    sync_shortlisted_telegram_to_groups,
    load_pilot_cities,
    load_fresher_keywords,
)

router = APIRouter()


@router.get("/summary", response_model=DiscoverySummaryResponse)
async def discovery_summary(db: AsyncSession = Depends(get_db)):
    """Counts: discovery sources, shortlisted, telegram groups from discovery, and total jobs (fetched)."""
    total_sources = (
        await db.execute(
            select(func.count()).select_from(DiscoverySource)
        )
    ).scalar() or 0

    shortlisted = (
        await db.execute(
            select(func.count())
            .select_from(DiscoverySource)
            .where(DiscoverySource.is_shortlisted.is_(True))
        )
    ).scalar() or 0

    # New discovery functionality is source-agnostic; do not depend on telegram_groups.
    # Keep the field for now but always report 0 to avoid coupling.
    tg_from_disc = 0

    jobs_total = (
        await db.execute(
            select(func.count())
            .select_from(Job)
            .where(Job.is_active.is_(True))
        )
    ).scalar() or 0
    return DiscoverySummaryResponse(
        discovery_sources_total=total_sources,
        discovery_sources_shortlisted=shortlisted,
        telegram_groups_from_discovery=tg_from_disc,
        jobs_total=jobs_total,
    )


@router.get("/pilot-cities", response_model=PilotCitiesResponse)
async def get_pilot_cities():
    """Return 20 India + 20 outside India pilot cities."""
    data = load_pilot_cities()
    return PilotCitiesResponse(india=data["india"], outside_india=data["outside_india"])


@router.get("/fresher-keywords", response_model=FresherKeywordsResponse)
async def get_fresher_keywords():
    """Return fresher pilot keywords for discovery and filtering."""
    data = load_fresher_keywords()
    return FresherKeywordsResponse(keywords=data["keywords"])


@router.get("/sources", response_model=DiscoverySourceListResponse)
async def list_discovery_sources(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    shortlisted_only: bool = Query(False, description="Only shortlisted sources"),
    phase: Optional[int] = Query(None),
    city: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """List discovery sources with optional filters. Use for viewing fetched sources."""
    q = select(DiscoverySource)
    if shortlisted_only:
        q = q.where(DiscoverySource.is_shortlisted.is_(True))
    if phase is not None:
        q = q.where(DiscoverySource.phase == phase)
    if city:
        q = q.where(DiscoverySource.city.ilike(f"%{city}%"))
    if source_type:
        q = q.where(DiscoverySource.source_type == source_type)

    total = (await db.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0

    q = q.order_by(DiscoverySource.phase, DiscoverySource.name)
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    sources = result.scalars().all()

    return DiscoverySourceListResponse(
        sources=[DiscoverySourceResponse.model_validate(s) for s in sources],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/sources/shortlisted", response_model=DiscoverySourceListResponse)
async def list_shortlisted_sources(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """List only shortlisted discovery sources (up to 200).

    NOTE: This endpoint re-implements the query instead of calling
    list_discovery_sources() directly, to avoid passing FastAPI's
    Query(...) objects into the SQL layer.
    """
    q = select(DiscoverySource).where(DiscoverySource.is_shortlisted.is_(True))

    total = (
        await db.execute(
            select(func.count()).select_from(q.subquery())
        )
    ).scalar() or 0

    q = q.order_by(DiscoverySource.phase, DiscoverySource.name)
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    sources = result.scalars().all()

    return DiscoverySourceListResponse(
        sources=[DiscoverySourceResponse.model_validate(s) for s in sources],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/shortlist", response_model=ShortlistResponse)
async def run_shortlist(
    max_shortlist: int = Query(200, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Run heuristics to mark up to max_shortlist sources as shortlisted (name/url keywords)."""
    count, total = await run_shortlist_heuristics(db, max_shortlist=max_shortlist)
    await db.commit()
    return ShortlistResponse(
        shortlisted_count=count,
        total_sources=total,
        message=f"Shortlisted {count} of {total} sources (heuristics + optional manual review).",
    )


@router.post("/sync-telegram", response_model=SyncTelegramResponse)
async def sync_telegram_from_discovery(db: AsyncSession = Depends(get_db)):
    """Sync shortlisted Telegram discovery sources into telegram_groups so scraper can fetch jobs."""
    synced, skipped, errors = await sync_shortlisted_telegram_to_groups(db)
    await db.commit()
    return SyncTelegramResponse(synced=synced, skipped=skipped, errors=errors[:20])
