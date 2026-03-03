"""
Visibility API - Real-time monitoring for Admin Panel

Provides detailed insights into:
- Account health and failures  
- Channel scraping status
- Error analysis (ban vs rate limit vs connectivity)
- System metrics

NO CloudWatch costs - pure API-based monitoring.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, Query
import asyncio
import json
import pytz  # kept for tz-aware MongoDB datetime construction
import redis as _redis_mod
from app.utils.timezone import IST, ist_today_utc_window
from sqlalchemy import select, func, desc, text, Integer, cast
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from pymongo import MongoClient
from pydantic import BaseModel
import structlog

from app.api.deps import get_db
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.models.telegram_group import TelegramGroup
from app.models.job import Job
from app.services.telegram_scraper_service import get_scraper_service
from app.core.scheduler import get_scheduler_status
from app.config import settings

logger = structlog.get_logger(__name__)
router = APIRouter()


class AccountHealthResponse(BaseModel):
    """Health status and live statistics for a single Telegram account."""

    phone: str
    health_status: str
    is_active: bool
    consecutive_errors: int
    last_successful_fetch_at: Optional[datetime]
    last_error_message: Optional[str]
    last_error_at: Optional[datetime]
    # Live count computed from telegram_groups — replaces stale
    # groups_joined_count column on TelegramAccount.
    channels_joined: int
    last_join_at: Optional[datetime]
    last_used_at: Optional[datetime]


class ChannelStatsResponse(BaseModel):
    """Scraping statistics and metadata for a single Telegram channel."""

    username: str
    title: Optional[str]
    category: Optional[str]
    is_active: bool
    is_joined: bool
    joined_by_account: Optional[str]   # phone of account that joined
    joined_at: Optional[datetime]
    last_scraped_at: Optional[datetime]
    last_scraped_by_account: Optional[str]  # phone via relationship
    hours_since_last_scrape: Optional[float]
    total_messages_scraped: int
    job_messages_found: int
    health_score: float


class VisibilityDashboardResponse(BaseModel):
    """Complete dashboard snapshot grouped by domain."""

    timestamp: datetime
    accounts: Dict
    channels: Dict
    messages: Dict
    jobs: Dict
    system: Dict
    sheets: Optional[Dict] = None


@router.get("/accounts/health", response_model=List[AccountHealthResponse])
async def get_accounts_health(db: AsyncSession = Depends(get_db)):
    """
    Get health status of all Telegram accounts.

    **Quick Visibility:**
    - Which accounts are healthy/degraded/banned
    - Error counts and last errors
    - Last successful fetch times
    - ``channels_joined``: live count from telegram_groups (not the
      stale denormalized column on TelegramAccount)

    Returns:
        List of account health details
    """
    result = await db.execute(select(TelegramAccount))
    accounts = result.scalars().all()

    # Build a live joined-channels count per account from telegram_groups.
    # The `groups_joined_count` column on TelegramAccount is stale and
    # unreliable, so we compute it on the fly.
    live_count_stmt = (
        select(
            TelegramGroup.telegram_account_id,
            func.count().label("cnt"),
        )
        .where(
            TelegramGroup.is_joined == True,  # noqa: E712
            TelegramGroup.is_active == True,  # noqa: E712
            TelegramGroup.telegram_account_id.isnot(None),
        )
        .group_by(TelegramGroup.telegram_account_id)
    )
    live_count_result = await db.execute(live_count_stmt)
    # Map UUID → count for O(1) lookups below.
    joined_counts: Dict = {
        str(row.telegram_account_id): row.cnt
        for row in live_count_result.all()
    }

    return [
        AccountHealthResponse(
            phone=acc.phone,
            health_status=acc.health_status.value,
            is_active=acc.is_active,
            consecutive_errors=acc.consecutive_errors,
            last_successful_fetch_at=acc.last_successful_fetch_at,
            last_error_message=acc.last_error_message,
            last_error_at=acc.last_error_at,
            channels_joined=joined_counts.get(str(acc.id), 0),
            last_join_at=acc.last_join_at,
            last_used_at=acc.last_used_at,
        )
        for acc in accounts
    ]


@router.get("/accounts/{phone}/errors")
async def get_account_error_history(
    phone: str,
    db: AsyncSession = Depends(get_db),
    limit: int = Query(50, le=200)
):
    """
    Get error history for a specific account.
    
    **Use Cases:**
    - Investigate why account was banned
    - Identify patterns (rate limiting vs connectivity)
    - Historical error trends
    
    Args:
        phone: Phone number of account (e.g., +1234567890)
        limit: Max number of errors to return
        
    Returns:
        Error history with timestamps and types
    """
    result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.phone == phone)
    )
    account = result.scalar_one_or_none()
    
    if not account:
        return {"error": "Account not found"}
    
    return {
        "account": phone,
        "health_status": account.health_status.value,
        "current_error": account.last_error_message,
        "last_error_at": account.last_error_at.isoformat() if account.last_error_at else None,
        "consecutive_errors": account.consecutive_errors,
        "last_successful_fetch": account.last_successful_fetch_at.isoformat() if account.last_successful_fetch_at else None
    }


@router.get("/channels/stats", response_model=List[ChannelStatsResponse])
async def get_channels_scraping_stats(
    active_only: bool = Query(True),
    assigned: Optional[bool] = Query(
        None,
        description="Filter by assignment status: true=assigned to an account, "
                    "false=unassigned (no telegram_account_id)",
    ),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
):
    """
    Get scraping statistics for all channels.

    **Visibility:**
    - Which channels are being scraped successfully
    - When was last scrape, which account last scraped
    - Which account joined the channel (``joined_by_account``)
    - Total messages and job messages collected
    - Health scores, join status, category

    Args:
        active_only: Show only active channels (default True).
        assigned: Optional filter — ``true`` returns only channels that
            have a ``telegram_account_id`` assigned; ``false`` returns
            only unassigned channels.  Omit for all.
        limit: Max channels to return.

    Returns:
        Channel scraping statistics
    """
    # Eagerly load the last_scraper_account relationship so we can read
    # its phone number without triggering lazy-load errors.
    stmt = select(TelegramGroup).options(
        selectinload(TelegramGroup.last_scraper_account)
    )

    if active_only:
        stmt = stmt.where(TelegramGroup.is_active == True)  # noqa: E712

    if assigned is True:
        stmt = stmt.where(TelegramGroup.telegram_account_id.isnot(None))
    elif assigned is False:
        stmt = stmt.where(TelegramGroup.telegram_account_id.is_(None))

    # Nulls-last ordering: channels with a recent scrape appear first;
    # channels never scraped sink to the bottom.
    stmt = stmt.order_by(
        TelegramGroup.last_scraped_at.desc().nullslast()
    ).limit(limit)

    result = await db.execute(stmt)
    channels = result.scalars().all()

    results = []
    for ch in channels:
        hours_since = None
        if ch.last_scraped_at:
            delta = datetime.now(timezone.utc) - ch.last_scraped_at
            hours_since = delta.total_seconds() / 3600

        # Phone of the account that last actively scraped this channel
        # (via the relationship), not just the joining account.
        last_scraper_phone: Optional[str] = (
            ch.last_scraper_account.phone
            if ch.last_scraper_account
            else None
        )

        results.append(
            ChannelStatsResponse(
                username=ch.username,
                title=ch.title,
                category=ch.category,
                is_active=ch.is_active,
                is_joined=ch.is_joined,
                joined_by_account=ch.joined_by_phone,
                joined_at=ch.joined_at,
                last_scraped_at=ch.last_scraped_at,
                last_scraped_by_account=last_scraper_phone,
                hours_since_last_scrape=(
                    round(hours_since, 1) if hours_since is not None else None
                ),
                total_messages_scraped=ch.total_messages_scraped,
                job_messages_found=ch.job_messages_found or 0,
                health_score=ch.health_score,
            )
        )

    return results


@router.get("/errors/analysis")
async def get_error_analysis(
    hours: int = Query(24, description="Look back N hours"),
    db: AsyncSession = Depends(get_db)
):
    """
    Analyze errors across all accounts and channels.
    
    **Key Insights:**
    - Most common error types
    - Which accounts are affected
    - Error frequency trends
    - Ban vs Rate Limit vs Connectivity breakdown
    
    Args:
        hours: Analysis window (default 24 hours)
        
    Returns:
        Error analysis with categorization
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    # Get errors from PostgreSQL
    stmt = select(TelegramAccount).where(TelegramAccount.last_error_at >= since)
    result = await db.execute(stmt)
    accounts = result.scalars().all()
    
    # Categorize errors
    error_categories = {
        'ban': [],
        'rate_limit': [],
        'connectivity': [],
        'session': [],
        'other': []
    }
    
    for acc in accounts:
        error_msg = acc.last_error_message or ""
        error_info = {
            'phone': acc.phone,
            'error': error_msg,
            'timestamp': acc.last_error_at.isoformat() if acc.last_error_at else None
        }
        
        if 'AuthKeyError' in error_msg or 'banned' in error_msg.lower():
            error_categories['ban'].append(error_info)
        elif 'FloodWait' in error_msg or 'rate limit' in error_msg.lower():
            error_categories['rate_limit'].append(error_info)
        elif 'connect' in error_msg.lower() or 'timeout' in error_msg.lower():
            error_categories['connectivity'].append(error_info)
        elif 'session' in error_msg.lower():
            error_categories['session'].append(error_info)
        else:
            error_categories['other'].append(error_info)
    
    total_errors = sum(len(v) for v in error_categories.values())
    
    return {
        'analysis_period_hours': hours,
        'total_errors': total_errors,
        'breakdown': {
            'bans': len(error_categories['ban']),
            'rate_limits': len(error_categories['rate_limit']),
            'connectivity_issues': len(error_categories['connectivity']),
            'session_issues': len(error_categories['session']),
            'other': len(error_categories['other'])
        },
        'details': error_categories,
        'summary': f"{total_errors} errors in last {hours} hours"
    }


def _mongodb_stats_sync(uri: str, db_name: str, today_start_utc: datetime) -> dict:
    """Sync helper — runs in a thread executor, never blocks the async event loop."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["raw_messages"]
        total      = col.count_documents({})
        unprocessed = col.count_documents({"is_processed": False})
        fetched_today = col.count_documents({"fetched_at": {"$gte": today_start_utc}})
        client.close()
        return {"total": total, "unprocessed": unprocessed, "fetched_today": fetched_today}
    except Exception as exc:
        logger.warning("failed_to_get_mongodb_stats", error=str(exc))
        return {"total": 0, "unprocessed": 0, "fetched_today": 0}


def _sheets_redis_sync(redis_url: str) -> dict:
    """Read last sheets-export status from Redis in a thread executor."""
    try:
        _r = _redis_mod.from_url(redis_url, decode_responses=True, socket_timeout=3)
        raw = _r.get("sheets:last_export")
        _r.close()
        if raw:
            return json.loads(raw)
        return {"status": "never_run", "note": "No export recorded yet. Will run after the next ML cycle."}
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}


@router.get("/dashboard", response_model=VisibilityDashboardResponse)
async def get_visibility_dashboard(db: AsyncSession = Depends(get_db)):
    """
    **Main Dashboard API** - Complete system visibility in one call.

    Uses IST midnight as the "today" boundary so counts match the Indian
    calendar day shown to users.  Response is cached in Redis for 30 s.

    Response is grouped into logical domains:
    - ``accounts``: health counts and recent errors
    - ``channels``: scraping coverage and join statistics
    - ``messages``: MongoDB raw message counts
    - ``jobs``: PostgreSQL job classification counts
    - ``system``: scheduler and scraper status
    """
    logger.info("visibility_dashboard_requested")

    # ── 30-second Redis cache ─────────────────────────────────────────────
    _CACHE_KEY = "visibility:dashboard:v1"
    try:
        _rc = _redis_mod.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=1)
        _cached = _rc.get(_CACHE_KEY)
        _rc.close()
        if _cached:
            logger.info("visibility_dashboard_cache_hit")
            return VisibilityDashboardResponse(**json.loads(_cached))
    except Exception:
        pass  # cache miss or Redis down — continue normally

    # ── IST-based “today” boundary (via central utility) ─────────────────────
    # today_start_pg  = naive UTC for Postgres TIMESTAMP WITHOUT TIME ZONE
    # today_start_utc = tz-aware UTC for MongoDB datetime comparisons
    today_start_pg, _end_pg, _ist_date = ist_today_utc_window()
    today_start_utc = today_start_pg.replace(tzinfo=timezone.utc)
    yesterday_utc   = today_start_utc - timedelta(days=1)

    # ── Fire all Postgres queries in parallel ────────────────────────────
    (
        r_all_accounts,
        r_channels_agg,
        r_latest_ch,
        r_jobs_today,
        r_recent_errors,
    ) = await asyncio.gather(
        db.execute(select(TelegramAccount)),
        # Single aggregation query replacing 5 separate COUNT queries
        db.execute(
            select(
                func.count().label("total_active"),
                func.sum(cast(TelegramGroup.is_joined == True,  Integer)).label("joined"),   # noqa: E712
                func.sum(cast(TelegramGroup.telegram_account_id.is_(None), Integer)).label("unassigned"),
                func.sum(cast(TelegramGroup.joined_at >= today_start_utc, Integer)).label("joined_today"),
                func.sum(cast(TelegramGroup.last_scraped_at >= yesterday_utc, Integer)).label("scraped_last_24h"),
            ).select_from(TelegramGroup).where(TelegramGroup.is_active == True)  # noqa: E712
        ),
        db.execute(
            select(TelegramGroup)
            .where(TelegramGroup.last_scraped_at.isnot(None))
            .order_by(desc(TelegramGroup.last_scraped_at))
            .limit(1)
        ),
        db.execute(
            select(func.count()).select_from(Job).where(Job.created_at >= today_start_pg)
        ),
        db.execute(
            select(TelegramAccount)
            .where(TelegramAccount.last_error_at.isnot(None))
            .order_by(desc(TelegramAccount.last_error_at))
            .limit(20)
        ),
    )

    # ── MongoDB + Redis sheets status — both in thread executors ─────────
    loop = asyncio.get_event_loop()
    mongo_stats, sheets_section = await asyncio.gather(
        loop.run_in_executor(None, _mongodb_stats_sync,
                             settings.MONGODB_URI, settings.MONGODB_DATABASE, today_start_utc),
        loop.run_in_executor(None, _sheets_redis_sync, settings.REDIS_URL),
    )

    # ── Assemble accounts section ─────────────────────────────────────────
    accounts = r_all_accounts.scalars().all()
    recent_errors = [
        {
            "phone": a.phone,
            "error": a.last_error_message,
            "timestamp": a.last_error_at.isoformat() if a.last_error_at else None,
            "health_status": a.health_status.value,
        }
        for a in r_recent_errors.scalars().all()
    ]
    accounts_section: Dict = {
        "total": len(accounts),
        "active": sum(1 for a in accounts if a.is_active),
        "healthy": sum(1 for a in accounts if a.health_status == HealthStatus.HEALTHY),
        "degraded": sum(1 for a in accounts if a.health_status == HealthStatus.DEGRADED),
        "banned": sum(1 for a in accounts if a.health_status == HealthStatus.BANNED),
        "recent_errors": recent_errors,
    }

    # ── Assemble channels section ─────────────────────────────────────────
    ch = r_channels_agg.first()
    latest_ch = r_latest_ch.scalars().first()
    last_scrape_at     = None
    hours_since_scrape = None
    if latest_ch and latest_ch.last_scraped_at:
        last_scrape_at = latest_ch.last_scraped_at.isoformat()
        hours_since_scrape = round(
            (datetime.now(timezone.utc) - latest_ch.last_scraped_at).total_seconds() / 3600, 1
        )
    channels_section: Dict = {
        "total_active":       ch.total_active      if ch else 0,
        "joined":             ch.joined            if ch else 0,
        "unassigned":         ch.unassigned        if ch else 0,
        "joined_today":       ch.joined_today      if ch else 0,
        "scraped_last_24h":   ch.scraped_last_24h  if ch else 0,
        "last_scrape_at":     last_scrape_at,
        "hours_since_last_scrape": hours_since_scrape,
    }

    # ── Assemble messages section ─────────────────────────────────────────
    messages_section: Dict = {
        "total":        mongo_stats["total"],
        "unprocessed":  mongo_stats["unprocessed"],
        "fetched_today": mongo_stats["fetched_today"],
    }

    # ── Assemble jobs section ─────────────────────────────────────────────
    # classified_today uses IST midnight, so it reflects jobs created
    # on the Indian calendar day — matches what the admin panel shows.
    jobs_section: Dict = {
        "classified_today": r_jobs_today.scalar(),
        "messages_fetched_today": mongo_stats["fetched_today"],
        "scraper_status": "operational" if accounts_section["healthy"] > 0 else "degraded",
    }

    # ── System section ────────────────────────────────────────────────────
    scraper = get_scraper_service()
    sched_status = get_scheduler_status()
    system_section: Dict = {
        "scheduler": sched_status,
        "scraper_initialized": scraper._initialized,
        "connected_clients": len(scraper.clients),
    }

    logger.info(
        "visibility_dashboard_generated",
        total_accounts=accounts_section["total"],
        healthy=accounts_section["healthy"],
    )

    response = VisibilityDashboardResponse(
        timestamp=datetime.now(timezone.utc),
        accounts=accounts_section,
        channels=channels_section,
        messages=messages_section,
        jobs=jobs_section,
        system=system_section,
        sheets=sheets_section,
    )

    # ── Cache for 30 seconds ──────────────────────────────────────────────
    try:
        _rc2 = _redis_mod.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=1)
        _rc2.setex(_CACHE_KEY, 30, response.model_dump_json())
        _rc2.close()
    except Exception:
        pass

    return response


@router.get("/system/status")
async def get_system_status():
    """
    Quick system status check — lightweight endpoint.

    **Use for:**
    - Status badges in admin panel
    - Quick health checks
    - Mobile app status display

    Returns:
        System status including scheduler job details with IST times.
    """
    scraper = get_scraper_service()
    sched_status = get_scheduler_status()

    return {
        "status": "operational",
        "scraper_initialized": scraper._initialized,
        "connected_clients": len(scraper.clients),
        "scheduler": sched_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
