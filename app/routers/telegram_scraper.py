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
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field

from app.services.telegram_scraper_service import get_scraper_service
from app.core.scheduler import get_scheduler_status, trigger_job_now
from app.config import settings

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


@router.get("/scheduler/status")
async def get_scheduler_status_endpoint():
    """
    Get status of the scheduler and all scheduled jobs.
    
    Returns information about:
    - Scheduler running status
    - All scheduled jobs
    - Next run times
    
    Returns:
        Dict with scheduler status and job information
    """
    try:
        status = get_scheduler_status()
        return status
    
    except Exception as e:
        logger.error(f"❌ Failed to get scheduler status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scheduler status: {str(e)}"
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


@router.get("/health")
async def scraper_health_check():
    """
    Health check endpoint for the scraper service.
    
    Checks:
    - Scraper service availability
    - MongoDB connection
    - Session files existence
    
    Returns:
        Health status
    """
    try:
        scraper = get_scraper_service()
        
        # Check session files
        session_files_exist = []
        for account_id in range(1, scraper.ACCOUNTS_AVAILABLE + 1):
            session_path = scraper.get_session_path(account_id)
            exists = (scraper.session_dir / f"session_account{account_id}.session").exists()
            session_files_exist.append({
                'account_id': account_id,
                'exists': exists
            })
        
        # Check MongoDB if initialized
        mongodb_status = "not_initialized"
        if scraper._initialized:
            try:
                scraper.mongo_client.admin.command('ping')
                mongodb_status = "connected"
            except Exception:
                mongodb_status = "error"
        
        return {
            'status': 'healthy',
            'service_initialized': scraper._initialized,
            'mongodb_status': mongodb_status,
            'session_files': session_files_exist,
            'total_clients_connected': len(scraper.clients)
        }
    
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }
