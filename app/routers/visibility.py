"""
Visibility API - Real-time monitoring for Admin Panel

Provides detailed insights into:
- Account health and failures  
- Channel scraping status
- Error analysis (ban vs rate limit vs connectivity)
- System metrics

NO CloudWatch costs - pure API-based monitoring.
"""

from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
import structlog

from app.api.deps import get_db
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.models.telegram_group import TelegramGroup
from app.services.telegram_scraper_service import get_scraper_service
from app.config import settings

logger = structlog.get_logger(__name__)
router = APIRouter()


class AccountHealthResponse(BaseModel):
    phone: str
    health_status: str
    is_active: bool
    consecutive_errors: int
    last_successful_fetch_at: Optional[datetime]
    last_error_message: Optional[str]
    last_error_at: Optional[datetime]
    groups_joined_count: int


class ChannelStatsResponse(BaseModel):
    username: str
    is_active: bool
    last_scraped_at: Optional[datetime]
    total_messages_scraped: int
    last_scraped_by_account: Optional[str]
    hours_since_last_scrape: Optional[float]
    health_score: float


class VisibilityDashboardResponse(BaseModel):
    timestamp: datetime
    accounts_summary: dict
    recent_errors: List[dict]
    channel_stats_summary: dict
    scraping_performance: dict


@router.get("/accounts/health", response_model=List[AccountHealthResponse])
async def get_accounts_health(db: AsyncSession = Depends(get_db)):
    """
    Get health status of all Telegram accounts.
    
    **Quick Visibility:**
    - Which accounts are healthy/degraded/banned
    - Error counts and last errors  
    - Last successful fetch times
    
    Returns:
        List of account health details
    """
    result = await db.execute(select(TelegramAccount))
    accounts = result.scalars().all()
    
    return [
        AccountHealthResponse(
            phone=acc.phone,
            health_status=acc.health_status.value,
            is_active=acc.is_active,
            consecutive_errors=acc.consecutive_errors,
            last_successful_fetch_at=acc.last_successful_fetch_at,
            last_error_message=acc.last_error_message,
            last_error_at=acc.last_error_at,
            groups_joined_count=acc.groups_joined_count
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
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db)
):
    """
    Get scraping statistics for all channels.
    
    **Visibility:**
    - Which channels are being scraped successfully
    - When was last scrape
    - Which account is assigned
    - Total messages collected
    - Health scores
    
    Args:
        active_only: Show only active channels
        limit: Max channels to return
        
    Returns:
        Channel scraping statistics
    """
    stmt = select(TelegramGroup)
    
    if active_only:
        stmt = stmt.where(TelegramGroup.is_active == True)
    
    stmt = stmt.order_by(TelegramGroup.last_scraped_at.desc()).limit(limit)
    result = await db.execute(stmt)
    channels = result.scalars().all()
    
    results = []
    for ch in channels:
        hours_since = None
        if ch.last_scraped_at:
            hours_since = (datetime.utcnow() - ch.last_scraped_at).total_seconds() / 3600
        
        # Get account phone if joined_by_account_id exists
        account_phone = None
        if ch.joined_by_account_id:
            account_result = await db.execute(
                select(TelegramAccount).where(TelegramAccount.id == ch.joined_by_account_id)
            )
            account = account_result.scalar_one_or_none()
            if account:
                account_phone = account.phone
        
        results.append(ChannelStatsResponse(
            username=ch.username,
            is_active=ch.is_active,
            last_scraped_at=ch.last_scraped_at,
            total_messages_scraped=ch.total_messages_scraped,
            last_scraped_by_account=account_phone,
            hours_since_last_scrape=round(hours_since, 1) if hours_since else None,
            health_score=ch.health_score
        ))
    
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
    since = datetime.utcnow() - timedelta(hours=hours)
    
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


@router.get("/dashboard", response_model=VisibilityDashboardResponse)
async def get_visibility_dashboard(db: AsyncSession = Depends(get_db)):
    """
    **Main Dashboard API** - Complete system visibility in one call.
    
    **Perfect for Admin Panel:**
    - Account health summary
    - Recent errors (last 20)
    - Channel scraping performance
    - System-wide metrics
    
    Returns:
        Complete dashboard data
    """
    logger.info("visibility_dashboard_requested")
    
    # Account summary
    result = await db.execute(select(TelegramAccount))
    accounts = result.scalars().all()
    accounts_summary = {
        'total': len(accounts),
        'healthy': sum(1 for a in accounts if a.health_status == HealthStatus.HEALTHY),
        'degraded': sum(1 for a in accounts if a.health_status == HealthStatus.DEGRADED),
        'banned': sum(1 for a in accounts if a.health_status == HealthStatus.BANNED),
        'active': sum(1 for a in accounts if a.is_active)
    }
    
    # Recent errors
    stmt = select(TelegramAccount).where(
        TelegramAccount.last_error_at.isnot(None)
    ).order_by(desc(TelegramAccount.last_error_at)).limit(20)
    result = await db.execute(stmt)
    recent_errors_accounts = result.scalars().all()
    
    recent_errors = [
        {
            'phone': acc.phone,
            'error': acc.last_error_message,
            'timestamp': acc.last_error_at.isoformat() if acc.last_error_at else None,
            'health_status': acc.health_status.value
        }
        for acc in recent_errors_accounts
    ]
    
    # Channel stats from PostgreSQL
    stmt = select(func.count()).select_from(TelegramGroup).where(TelegramGroup.is_active == True)
    result = await db.execute(stmt)
    total_channels = result.scalar()
    
    # Last scrape time
    stmt = select(TelegramGroup).where(
        TelegramGroup.last_scraped_at.isnot(None)
    ).order_by(desc(TelegramGroup.last_scraped_at))
    result = await db.execute(stmt)
    latest_channel = result.scalars().first()
    
    last_scrape_time = None
    hours_since_scrape = None
    if latest_channel and latest_channel.last_scraped_at:
        last_scrape_time = latest_channel.last_scraped_at.isoformat()
        hours_since_scrape = (datetime.utcnow() - latest_channel.last_scraped_at).total_seconds() / 3600
    
    # Get MongoDB stats if available
    total_messages = 0
    unprocessed_messages = 0
    try:
        scraper = get_scraper_service()
        if scraper._initialized:
            mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
            total_messages = mongo_db.raw_messages.count_documents({})
            unprocessed_messages = mongo_db.raw_messages.count_documents({'is_processed': False})
    except Exception as e:
        logger.warning("failed_to_get_mongodb_stats", error=str(e))
    
    channel_stats_summary = {
        'total_active_channels': total_channels,
        'total_messages_collected': total_messages,
        'unprocessed_messages': unprocessed_messages,
        'last_scrape_at': last_scrape_time,
        'hours_since_last_scrape': round(hours_since_scrape, 1) if hours_since_scrape else None
    }
    
    # Performance metrics (last 24 hours) from PostgreSQL
    yesterday = datetime.utcnow() - timedelta(days=1)
    
    # Count channels scraped in last 24h
    stmt = select(func.count()).select_from(TelegramGroup).where(
        TelegramGroup.last_scraped_at >= yesterday
    )
    result = await db.execute(stmt)
    channels_scraped_24h = result.scalar()
    
    scraping_performance = {
        'channels_scraped_24h': channels_scraped_24h,
        'messages_last_24h': 0,  # Would need MongoDB for this
        'scraper_status': 'operational' if accounts_summary['healthy'] > 0 else 'degraded'
    }
    
    logger.info("visibility_dashboard_generated", **accounts_summary)
    
    return VisibilityDashboardResponse(
        timestamp=datetime.utcnow(),
        accounts_summary=accounts_summary,
        recent_errors=recent_errors,
        channel_stats_summary=channel_stats_summary,
        scraping_performance=scraping_performance
    )


@router.get("/system/status")
async def get_system_status():
    """
    Quick system status check - lightweight endpoint.
    
    **Use for:**
    - Status badges in admin panel
    - Quick health checks
    - Mobile app status display
    
    Returns:
        System status summary
    """
    scraper = get_scraper_service()
    
    return {
        'status': 'operational',
        'scraper_initialized': scraper._initialized,
        'connected_clients': len(scraper.clients),
        'timestamp': datetime.utcnow().isoformat()
    }
