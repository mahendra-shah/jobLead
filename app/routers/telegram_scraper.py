"""
Telegram Scraper API Router

Provides endpoints for managing and monitoring the Telegram scraper service.

Endpoints:
- POST /api/telegram-scraper/scrape/trigger - Manually trigger scraping
- GET /api/telegram-scraper/scrape/status - Get scraper status
- GET /api/telegram-scraper/scrape/channels - List all channels
- POST /api/telegram-scraper/scrape/channels/{channel_username} - Scrape single channel
- GET /api/telegram-scraper/scheduler/status - Get scheduler status
- POST /api/telegram-scraper/scheduler/trigger/{job_id} - Manually trigger scheduled job

Author: Backend Team
Date: 2026-02-10
"""

import logging
import os
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Path, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.services.telegram_scraper_service import get_scraper_service
from app.core.scheduler import get_scheduler_status, trigger_job_now
from app.config import settings
from app.api.deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/telegram-scraper",
    tags=["Telegram Scraper"]
)


# Response Models
class ScrapeResult(BaseModel):
    """Result from a scraping operation."""
    total_channels: int
    successful: int
    failed: int
    total_messages: int
    duration_seconds: float
    started_at: datetime
    completed_at: datetime
    account_stats: Dict[int, Dict[str, int]]
    results: Optional[list] = None  # Detailed results per channel


class ChannelScrapeResult(BaseModel):
    """Result from scraping a single channel."""
    channel: str
    account_id: int
    messages_fetched: int
    success: bool
    error: Optional[str] = None


class ScraperStatus(BaseModel):
    """Current status of the scraper."""
    service_initialized: bool
    total_clients: int
    account_stats: Dict[int, Dict[str, int]]


class ChannelInfo(BaseModel):
    """Information about a channel."""
    username: str
    account_id: int
    last_scraped_at: Optional[datetime] = None
    last_message_id: Optional[int] = None
    total_messages_scraped: int = 0
    is_active: bool = True


# Endpoints

@router.post("/scrape/trigger", response_model=ScrapeResult)
async def trigger_scraping():
    """
    Manually trigger Telegram scraping for all active channels.
    
    This endpoint initiates an immediate scraping of all active channels
    using the multi-account scraper service. Useful for on-demand scraping
    outside of the scheduled times.
    
    Returns:
        ScrapeResult: Statistics about the scraping operation
    
    Raises:
        HTTPException: If scraping fails
    """
    logger.info("📡 Manual scrape triggered via API")
    
    try:
        scraper = get_scraper_service()
        await scraper.initialize()
        
        result = await scraper.scrape_all_channels()
        
        logger.info(f"✅ Manual scrape completed: {result['total_messages']} messages")
        
        return ScrapeResult(**result)
    
    except Exception as e:
        logger.error(f"❌ Manual scrape failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Scraping failed: {str(e)}"
        )


@router.get("/scrape/status", response_model=ScraperStatus)
async def get_scraper_status():
    """
    Get current status of the Telegram scraper service.
    
    Returns information about initialized clients, account statistics,
    and overall service health.
    
    Returns:
        ScraperStatus: Current scraper status
    """
    try:
        scraper = get_scraper_service()
        stats = scraper.get_stats()
        
        return ScraperStatus(
            service_initialized=scraper._initialized,
            total_clients=stats['total_clients'],
            account_stats=stats['accounts']
        )
    
    except Exception as e:
        logger.error(f"❌ Failed to get scraper status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get status: {str(e)}"
        )


@router.get("/scrape/channels")
async def list_channels(
    active_only: bool = Query(True, description="Only show active channels")
):
    """
    List all Telegram channels in the database.
    
    Args:
        active_only: If True, only return active channels
    
    Returns:
        List of channels with their scraping status
    """
    try:
        scraper = get_scraper_service()
        await scraper.initialize()
        
        mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
        channels_collection = mongo_db['channels']
        
        # Build query
        query = {}
        if active_only:
            query['is_active'] = True
        
        # Get channels
        channels = list(channels_collection.find(
            query,
            {
                'username': 1,
                'joined_by_account_id': 1,
                'last_scraped_at': 1,
                'last_message_id': 1,
                'total_messages_scraped': 1,
                'is_active': 1
            }
        ))
        
        # Format response
        result = []
        for channel in channels:
            result.append({
                'username': channel.get('username'),
                'account_id': channel.get('joined_by_account_id', 1),
                'last_scraped_at': channel.get('last_scraped_at'),
                'last_message_id': channel.get('last_message_id'),
                'total_messages_scraped': channel.get('total_messages_scraped', 0),
                'is_active': channel.get('is_active', True)
            })
        
        return {
            'total': len(result),
            'channels': result
        }
    
    except Exception as e:
        logger.error(f"❌ Failed to list channels: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list channels: {str(e)}"
        )


@router.post("/scrape/channels/{channel_username}", response_model=ChannelScrapeResult)
async def scrape_single_channel(
    channel_username: str = Path(..., description="Channel username (with or without @)")
):
    """
    Manually scrape a single channel.
    
    Useful for testing or immediately fetching messages from a specific channel
    without waiting for the scheduled scrape.
    
    Args:
        channel_username: Channel username (e.g., 'bangalore_jobs' or '@bangalore_jobs')
    
    Returns:
        ChannelScrapeResult: Result of the scraping operation
    
    Raises:
        HTTPException: If channel not found or scraping fails
    """
    logger.info(f"📡 Manual scrape requested for channel: {channel_username}")
    
    try:
        scraper = get_scraper_service()
        await scraper.initialize()
        
        result = await scraper.scrape_single_channel(channel_username)
        
        logger.info(f"✅ Channel scrape completed: @{channel_username}")
        
        return ChannelScrapeResult(**result)
    
    except ValueError as e:
        logger.warning(f"⚠️  Channel not found: {channel_username}")
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    
    except Exception as e:
        logger.error(f"❌ Channel scrape failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Scraping failed: {str(e)}"
        )


@router.post("/scheduler/trigger/{job_id}")
async def trigger_scheduled_job(
    job_id: str = Path(..., description="Job ID to trigger (e.g., 'telegram_scraper_daily')")
):
    """
    Manually trigger a scheduled job immediately.
    
    Useful for testing or running jobs on-demand without waiting for
    the scheduled time.
    
    Args:
        job_id: ID of the job to trigger
    
    Returns:
        Result from the job execution
    
    Raises:
        HTTPException: If job not found or execution fails
    """
    logger.info(f"📡 Manual trigger requested for job: {job_id}")
    
    try:
        result = await trigger_job_now(job_id)
        
        logger.info(f"✅ Job triggered successfully: {job_id}")
        
        return {
            'success': True,
            'job_id': job_id,
            'triggered_at': datetime.utcnow(),
            'result': result
        }
    
    except ValueError as e:
        logger.warning(f"⚠️  Job not found: {job_id}")
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    
    except Exception as e:
        logger.error(f"❌ Job trigger failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Job execution failed: {str(e)}"
        )

async def scraper_health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint for the scraper service.
    
    Checks:
    - Scraper service availability
    - MongoDB connection and latency
    - PostgreSQL connection and latency
    - Session files existence and validity
    - Account health status
    - Last successful scrape time
    
    Returns HTTP 503 if any critical component is unhealthy.
    
    Returns:
        Health status with detailed sub-checks
    """
    import time
    import structlog
    from app.models.telegram_account import TelegramAccount
    from fastapi import status
    from fastapi.responses import JSONResponse
    
    logger = structlog.get_logger(__name__)
    
    try:
        scraper = get_scraper_service()
        is_healthy = True
        critical_issues = []
        
        # Check session files
        session_files_check = []
        for account_id in range(1, scraper.ACCOUNTS_AVAILABLE + 1):
            session_file_path = scraper.session_dir / f"session_account{account_id}.session"
            exists = session_file_path.exists()
            
            file_info = {
                'account_id': account_id,
                'exists': exists
            }
            
            if exists:
                stat = session_file_path.stat()
                file_info['size_bytes'] = stat.st_size
                file_info['modified_days_ago'] = round((time.time() - stat.st_mtime) / 86400, 1)
                
                if stat.st_size == 0:
                    file_info['warning'] = 'Empty file'
                    is_healthy = False
                    critical_issues.append(f"Account {account_id} session file is empty")
            else:
                is_healthy = False
                critical_issues.append(f"Account {account_id} session file missing")
            
            session_files_check.append(file_info)
        
        # Check MongoDB connection and latency
        mongodb_check = {"status": "not_initialized", "latency_ms": None}
        if scraper._initialized and scraper.mongo_client:
            try:
                start = time.time()
                scraper.mongo_client.admin.command('ping')
                latency_ms = round((time.time() - start) * 1000, 2)
                mongodb_check = {
                    "status": "connected",
                    "latency_ms": latency_ms
                }
                
                if latency_ms > 1000:
                    mongodb_check['warning'] = 'High latency'
            except Exception as e:
                mongodb_check = {
                    "status": "error",
                    "error": str(e)
                }
                is_healthy = False
                critical_issues.append(f"MongoDB connection failed: {str(e)}")
        
        # Check PostgreSQL connection and latency
        postgres_check = {}
        try:
            start = time.time()
            db.execute("SELECT 1")
            latency_ms = round((time.time() - start) * 1000, 2)
            postgres_check = {
                "status": "connected",
                "latency_ms": latency_ms
            }
            
            if latency_ms > 500:
                postgres_check['warning'] = 'High latency'
        except Exception as e:
            postgres_check = {
                "status": "error",
                "error": str(e)
            }
            is_healthy = False
            critical_issues.append(f"PostgreSQL connection failed: {str(e)}")
        
        # Check account health status
        accounts_check = []
        try:
            accounts = db.query(TelegramAccount).all()
            
            for account in accounts:
                accounts_check.append({
                    "phone": account.phone,
                    "health_status": account.health_status.value,
                    "is_active": account.is_active,
                    "consecutive_errors": account.consecutive_errors,
                    "last_successful_fetch_at": account.last_successful_fetch_at.isoformat() if account.last_successful_fetch_at else None,
                    "last_error": account.last_error_message
                })
            
            active_count = sum(1 for a in accounts if a.is_healthy())
            if active_count == 0:
                is_healthy = False
                critical_issues.append("All accounts are unhealthy")
            elif active_count <= 2:
                critical_issues.append(f"Only {active_count} accounts healthy (degraded capacity)")
        except Exception as e:
            accounts_check = {"error": str(e)}
            logger.error("failed_to_check_account_health", error=str(e))
        
        # Get last successful scrape time (from MongoDB)
        last_scrape_check = {}
        if scraper._initialized and scraper.mongo_client:
            try:
                mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
                channels_collection = mongo_db['channels']
                
                # Find most recent scrape
                latest_channel = channels_collection.find_one(
                    {'last_scraped_at': {'$exists': True}},
                    sort=[('last_scraped_at', -1)]
                )
                
                if latest_channel and latest_channel.get('last_scraped_at'):
                    last_scrape_time = latest_channel['last_scraped_at']
                    hours_ago = (datetime.utcnow() - last_scrape_time).total_seconds() / 3600
                    
                    last_scrape_check = {
                        "last_scrape_at": last_scrape_time.isoformat(),
                        "hours_ago": round(hours_ago, 1)
                    }
                    
                    if hours_ago > 24:
                        last_scrape_check['warning'] = 'No scrape in last 24 hours'
                        is_healthy = False
                        critical_issues.append(f"No successful scrape in {round(hours_ago, 1)} hours")
                else:
                    last_scrape_check = {"status": "never_scraped"}
            except Exception as e:
                last_scrape_check = {"error": str(e)}
        
        response_data = {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'service_initialized': scraper._initialized,
            'session_files': session_files_check,
            'mongodb': mongodb_check,
            'postgres': postgres_check,
            'accounts': accounts_check,
            'last_scrape': last_scrape_check,
            'total_clients_connected': len(scraper.clients),
            'critical_issues': critical_issues if critical_issues else None
        }
        
        # Return 503 if unhealthy
        if not is_healthy:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=response_data
            )
        
        return response_data
    
    except Exception as e:
        logger.error("health_check_failed", error=str(e), exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                'status': 'unhealthy',
                'error': str(e)
            }
        )

