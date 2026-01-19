"""Admin API endpoints for Telegram scraping management."""
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db, get_current_active_superuser
from app.models.user import User
from app.models.scraping_log import ScrapingLog
from app.models.telegram_account import TelegramAccount
from app.models.telegram_group import TelegramGroup
from app.models.raw_telegram_message import RawTelegramMessage
from app.models.job import Job
from app.models.job_scraping_preferences import JobScrapingPreferences
from app.schemas.admin import (
    ScrapingLogResponse,
    ScrapingLogListResponse,
    TelegramAccountCreate,
    TelegramAccountUpdate,
    TelegramAccountResponse,
    TelegramAccountListResponse,
    TelegramGroupUpdate,
    TelegramGroupResponse,
    TelegramGroupListResponse,
    TelegramGroupHealthHistoryResponse,
    HealthScoreHistory,
    DashboardStats,
    TriggerScrapeRequest,
    TriggerScrapeResponse,
)
from app.schemas.job_preferences import (
    JobPreferencesResponse,
    JobPreferencesUpdate,
    ProcessingStatsResponse,
    FilteringStats
)
from app.services.storage_factory import get_storage_service
import os

router = APIRouter()


# ==================== Scraping Logs ====================

@router.get("/scraping-logs", response_model=ScrapingLogListResponse)
async def get_scraping_logs(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
    lambda_function: Optional[str] = Query(None, description="Filter by lambda function"),
    status: Optional[str] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Get paginated list of scraping logs with filters."""
    # Build query
    query = select(ScrapingLog)
    
    # Apply filters
    if lambda_function:
        query = query.where(ScrapingLog.lambda_function == lambda_function)
    if status:
        query = query.where(ScrapingLog.status == status)
    
    # Get total count
    count_query = select(func.count()).select_from(ScrapingLog)
    if lambda_function:
        count_query = count_query.where(ScrapingLog.lambda_function == lambda_function)
    if status:
        count_query = count_query.where(ScrapingLog.status == status)
    
    result = await db.execute(count_query)
    total = result.scalar()
    
    # Apply pagination and ordering
    query = query.order_by(desc(ScrapingLog.started_at))
    query = query.offset((page - 1) * page_size).limit(page_size)
    
    result = await db.execute(query)
    logs = result.scalars().all()
    
    return ScrapingLogListResponse(
        logs=logs,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/scraping-logs/{log_id}", response_model=ScrapingLogResponse)
async def get_scraping_log(
    log_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
):
    """Get detailed scraping log by ID."""
    result = await db.execute(
        select(ScrapingLog).where(ScrapingLog.id == log_id)
    )
    log = result.scalar_one_or_none()
    
    if not log:
        raise HTTPException(status_code=404, detail="Scraping log not found")
    
    return log


# ==================== Telegram Accounts ====================

@router.get("/telegram-accounts", response_model=TelegramAccountListResponse)
async def get_telegram_accounts(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
    is_active: Optional[bool] = Query(None),
    is_banned: Optional[bool] = Query(None),
):
    """Get list of Telegram accounts with usage stats."""
    query = select(TelegramAccount)
    
    if is_active is not None:
        query = query.where(TelegramAccount.is_active == is_active)
    if is_banned is not None:
        query = query.where(TelegramAccount.is_banned == is_banned)
    
    query = query.order_by(desc(TelegramAccount.last_used_at))
    
    result = await db.execute(query)
    accounts = result.scalars().all()
    
    # Add usage stats
    today = datetime.utcnow().date()
    accounts_with_stats = []
    
    for account in accounts:
        # Calculate groups joined today
        groups_today = 0
        if account.last_join_at and account.last_join_at.date() == today:
            # Query actual joins today
            result = await db.execute(
                select(func.count())
                .select_from(TelegramGroup)
                .where(
                    and_(
                        TelegramGroup.joined_by_account_id == account.id,
                        func.date(TelegramGroup.joined_at) == today
                    )
                )
            )
            groups_today = result.scalar() or 0
        
        # Create response object
        account_dict = {
            "id": account.id,
            "phone": account.phone,
            "api_id": account.api_id,
            "is_active": account.is_active,
            "is_banned": account.is_banned,
            "groups_joined_count": account.groups_joined_count,
            "last_used_at": account.last_used_at,
            "last_join_at": account.last_join_at,
            "created_at": account.created_at,
            "can_join_today": account.can_join_today(),
            "groups_joined_today": groups_today,
        }
        accounts_with_stats.append(TelegramAccountResponse(**account_dict))
    
    return TelegramAccountListResponse(
        accounts=accounts_with_stats,
        total=len(accounts_with_stats),
    )


@router.post("/telegram-accounts", response_model=TelegramAccountResponse)
async def create_telegram_account(
    account_data: TelegramAccountCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
):
    """Add new Telegram account."""
    # Check if account already exists
    result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.phone == account_data.phone)
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(status_code=400, detail="Account with this phone already exists")
    
    # Create new account
    account = TelegramAccount(
        phone=account_data.phone,
        api_id=account_data.api_id,
        api_hash=account_data.api_hash,
        session_string=account_data.session_string,
        is_active=True,
        is_banned=False,
        groups_joined_count=0,
    )
    
    db.add(account)
    await db.commit()
    await db.refresh(account)
    
    return TelegramAccountResponse(
        id=account.id,
        phone=account.phone,
        api_id=account.api_id,
        is_active=account.is_active,
        is_banned=account.is_banned,
        groups_joined_count=account.groups_joined_count,
        last_used_at=account.last_used_at,
        last_join_at=account.last_join_at,
        created_at=account.created_at,
        can_join_today=account.can_join_today(),
        groups_joined_today=0,
    )


@router.patch("/telegram-accounts/{account_id}", response_model=TelegramAccountResponse)
async def update_telegram_account(
    account_id: int,
    account_data: TelegramAccountUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
):
    """Update Telegram account (activate/deactivate, mark banned)."""
    result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.id == account_id)
    )
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    # Update fields
    if account_data.is_active is not None:
        account.is_active = account_data.is_active
    if account_data.is_banned is not None:
        account.is_banned = account_data.is_banned
        if account_data.is_banned:
            account.is_active = False  # Auto-deactivate banned accounts
    
    await db.commit()
    await db.refresh(account)
    
    return TelegramAccountResponse(
        id=account.id,
        phone=account.phone,
        api_id=account.api_id,
        is_active=account.is_active,
        is_banned=account.is_banned,
        groups_joined_count=account.groups_joined_count,
        last_used_at=account.last_used_at,
        last_join_at=account.last_join_at,
        created_at=account.created_at,
        can_join_today=account.can_join_today(),
        groups_joined_today=0,
    )


# ==================== Telegram Groups ====================

@router.get("/telegram-groups", response_model=TelegramGroupListResponse)
async def get_telegram_groups(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
    is_active: Optional[bool] = Query(None),
    is_joined: Optional[bool] = Query(None),
    category: Optional[str] = Query(None),
    min_health_score: Optional[float] = Query(None, ge=0, le=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Get list of Telegram groups with health scores."""
    query = select(TelegramGroup)
    
    # Apply filters
    if is_active is not None:
        query = query.where(TelegramGroup.is_active == is_active)
    if is_joined is not None:
        query = query.where(TelegramGroup.is_joined == is_joined)
    if category:
        query = query.where(TelegramGroup.category == category)
    if min_health_score is not None:
        query = query.where(TelegramGroup.health_score >= min_health_score)
    
    # Get total count
    count_query = select(func.count()).select_from(TelegramGroup)
    if is_active is not None:
        count_query = count_query.where(TelegramGroup.is_active == is_active)
    if is_joined is not None:
        count_query = count_query.where(TelegramGroup.is_joined == is_joined)
    if category:
        count_query = count_query.where(TelegramGroup.category == category)
    if min_health_score is not None:
        count_query = count_query.where(TelegramGroup.health_score >= min_health_score)
    
    result = await db.execute(count_query)
    total = result.scalar()
    
    # Apply pagination and ordering
    query = query.order_by(desc(TelegramGroup.health_score))
    query = query.offset((page - 1) * page_size).limit(page_size)
    
    result = await db.execute(query)
    groups = result.scalars().all()
    
    return TelegramGroupListResponse(
        groups=groups,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.patch("/telegram-groups/{group_id}", response_model=TelegramGroupResponse)
async def update_telegram_group(
    group_id: int,
    group_data: TelegramGroupUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
):
    """Update Telegram group (activate/deactivate, change category)."""
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.id == group_id)
    )
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    # Update fields
    if group_data.is_active is not None:
        group.is_active = group_data.is_active
        if not group_data.is_active:
            group.deactivated_at = datetime.utcnow()
        else:
            group.deactivated_at = None
    
    if group_data.category is not None:
        group.category = group_data.category
    
    await db.commit()
    await db.refresh(group)
    
    return group


@router.get("/telegram-groups/{group_id}/health-history", response_model=TelegramGroupHealthHistoryResponse)
async def get_group_health_history(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
    days: int = Query(30, ge=1, le=90),
):
    """Get health score trend for a group."""
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.id == group_id)
    )
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    # Query scraping logs to get historical data
    since_date = datetime.utcnow() - timedelta(days=days)
    
    result = await db.execute(
        select(ScrapingLog)
        .where(
            and_(
                ScrapingLog.lambda_function == "message_scraper",
                ScrapingLog.started_at >= since_date,
                ScrapingLog.status == "success"
            )
        )
        .order_by(ScrapingLog.started_at)
    )
    logs = result.scalars().all()
    
    # Build history from logs (simplified - in production, store snapshots)
    history = []
    for log in logs:
        if log.metrics and group.username in log.metrics.get("groups_processed", []):
            history.append(HealthScoreHistory(
                date=log.completed_at or log.started_at,
                health_score=group.health_score or 0.0,
                total_messages=group.total_messages_scraped,
                job_messages=group.job_messages_found,
                quality_jobs=group.quality_jobs_found,
            ))
    
    return TelegramGroupHealthHistoryResponse(
        group_id=group.id,
        username=group.username,
        current_health_score=group.health_score,
        history=history,
    )


# ==================== Dashboard Stats ====================

@router.get("/stats/dashboard", response_model=DashboardStats)
async def get_dashboard_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
):
    """Get overall system statistics and metrics."""
    today = datetime.utcnow().date()
    
    # Total jobs
    result = await db.execute(select(func.count()).select_from(Job))
    total_jobs = result.scalar() or 0
    
    # Jobs created today
    result = await db.execute(
        select(func.count())
        .select_from(Job)
        .where(func.date(Job.created_at) == today)
    )
    total_jobs_today = result.scalar() or 0
    
    # Total messages processed
    result = await db.execute(
        select(func.count())
        .select_from(RawTelegramMessage)
        .where(RawTelegramMessage.processed.is_(True))
    )
    total_messages_processed = result.scalar() or 0
    
    # Messages processed today
    result = await db.execute(
        select(func.count())
        .select_from(RawTelegramMessage)
        .where(
            and_(
                RawTelegramMessage.processed.is_(True),
                func.date(RawTelegramMessage.created_at) == today
            )
        )
    )
    total_messages_today = result.scalar() or 0
    
    # Account stats
    result = await db.execute(select(func.count()).select_from(TelegramAccount))
    total_accounts = result.scalar() or 0
    
    result = await db.execute(
        select(func.count())
        .select_from(TelegramAccount)
        .where(TelegramAccount.is_active.is_(True))
    )
    active_accounts = result.scalar() or 0
    
    # Group stats
    result = await db.execute(select(func.count()).select_from(TelegramGroup))
    total_groups = result.scalar() or 0
    
    result = await db.execute(
        select(func.count())
        .select_from(TelegramGroup)
        .where(TelegramGroup.is_active.is_(True))
    )
    active_groups = result.scalar() or 0
    
    result = await db.execute(
        select(func.count())
        .select_from(TelegramGroup)
        .where(TelegramGroup.is_joined.is_(True))
    )
    joined_groups = result.scalar() or 0
    
    # Average health score
    result = await db.execute(
        select(func.avg(TelegramGroup.health_score))
        .select_from(TelegramGroup)
        .where(
            and_(
                TelegramGroup.is_joined.is_(True),
                TelegramGroup.health_score.isnot(None)
            )
        )
    )
    average_health_score = result.scalar() or 0.0
    
    # Recent activity
    result = await db.execute(
        select(func.max(TelegramGroup.joined_at))
        .select_from(TelegramGroup)
    )
    last_group_join = result.scalar()
    
    result = await db.execute(
        select(func.max(TelegramGroup.last_scraped_at))
        .select_from(TelegramGroup)
    )
    last_message_scrape = result.scalar()
    
    result = await db.execute(
        select(func.max(ScrapingLog.completed_at))
        .select_from(ScrapingLog)
        .where(ScrapingLog.lambda_function == "job_processor")
    )
    last_job_extraction = result.scalar()
    
    # Last 24h metrics
    since_24h = datetime.utcnow() - timedelta(hours=24)
    
    result = await db.execute(
        select(func.count())
        .select_from(Job)
        .where(Job.created_at >= since_24h)
    )
    jobs_extracted_last_24h = result.scalar() or 0
    
    result = await db.execute(
        select(func.count())
        .select_from(RawTelegramMessage)
        .where(
            and_(
                RawTelegramMessage.processing_status == "duplicate",
                RawTelegramMessage.created_at >= since_24h
            )
        )
    )
    duplicates_found_last_24h = result.scalar() or 0
    
    result = await db.execute(
        select(func.count())
        .select_from(RawTelegramMessage)
        .where(RawTelegramMessage.created_at >= since_24h)
    )
    messages_scraped_last_24h = result.scalar() or 0
    
    # Cost estimates (simplified - based on AI API calls)
    result = await db.execute(
        select(func.sum(ScrapingLog.cost_estimate))
        .select_from(ScrapingLog)
        .where(func.date(ScrapingLog.started_at) == today)
    )
    estimated_cost_today = result.scalar() or 0.0
    
    # Estimate monthly cost (today * 30)
    estimated_cost_month = estimated_cost_today * 30
    
    return DashboardStats(
        total_jobs=total_jobs,
        total_jobs_today=total_jobs_today,
        total_messages_processed=total_messages_processed,
        total_messages_today=total_messages_today,
        total_accounts=total_accounts,
        active_accounts=active_accounts,
        total_groups=total_groups,
        active_groups=active_groups,
        joined_groups=joined_groups,
        average_health_score=average_health_score,
        last_group_join=last_group_join,
        last_message_scrape=last_message_scrape,
        last_job_extraction=last_job_extraction,
        jobs_extracted_last_24h=jobs_extracted_last_24h,
        duplicates_found_last_24h=duplicates_found_last_24h,
        messages_scraped_last_24h=messages_scraped_last_24h,
        estimated_cost_today=estimated_cost_today,
        estimated_cost_month=estimated_cost_month,
    )


# ==================== Manual Trigger ====================

@router.post("/trigger-scrape", response_model=TriggerScrapeResponse)
async def trigger_scrape(
    request: TriggerScrapeRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser),
):
    """Manually invoke a Lambda function."""
    import boto3
    import json
    import uuid
    from app.config import settings
    
    # Validate lambda function name
    valid_functions = ["group_joiner", "message_scraper", "job_processor"]
    if request.lambda_function not in valid_functions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid lambda function. Must be one of: {', '.join(valid_functions)}"
        )
    
    # Check working hours if not forced
    if not request.force and request.lambda_function == "group_joiner":
        from datetime import datetime
        import pytz
        
        ist = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist)
        hour = now_ist.hour
        
        if hour < 10 or hour >= 20:
            return TriggerScrapeResponse(
                success=False,
                message="Group joiner can only run between 10 AM - 8 PM IST. Use force=true to override.",
                execution_id=None,
            )
    
    try:
        # Initialize boto3 client
        lambda_client = boto3.client('lambda', region_name=settings.AWS_REGION)
        
        # Generate execution ID
        execution_id = str(uuid.uuid4())
        
        # Prepare payload
        payload = {
            "execution_id": execution_id,
            "triggered_by": "admin",
            "force": request.force,
        }
        
        # Invoke Lambda function
        function_name = f"placement-{request.lambda_function.replace('_', '-')}"
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(payload),
        )
        
        if response['StatusCode'] == 202:
            return TriggerScrapeResponse(
                success=True,
                message=f"Successfully triggered {request.lambda_function}",
                execution_id=execution_id,
            )
        else:
            return TriggerScrapeResponse(
                success=False,
                message=f"Failed to trigger Lambda. Status: {response['StatusCode']}",
                execution_id=None,
            )
    
    except Exception as e:
        return TriggerScrapeResponse(
            success=False,
            message=f"Error triggering Lambda: {str(e)}",
            execution_id=None,
        )


# ==================== Job Scraping Preferences ====================

@router.get(
    "/job-preferences",
    response_model=JobPreferencesResponse,
    summary="Get active job scraping preferences"
)
async def get_active_preferences(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """
    Get the currently active job scraping preferences.
    
    Only admins can access this endpoint.
    """
    # Query for active preferences
    result = await db.execute(
        select(JobScrapingPreferences).where(
            JobScrapingPreferences.is_active.is_(True)
        )
    )
    preferences = result.scalar_one_or_none()
    
    if not preferences:
        raise HTTPException(
            status_code=404,
            detail="No active job scraping preferences found. Please create default preferences."
        )
    
    return preferences


@router.put(
    "/job-preferences",
    response_model=JobPreferencesResponse,
    summary="Update job scraping preferences"
)
async def update_preferences(
    prefs_update: JobPreferencesUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """
    Update the active job scraping preferences.
    
    This will update the existing active preferences with new values.
    Only admins can access this endpoint.
    """
    # Get current active preferences
    result = await db.execute(
        select(JobScrapingPreferences).where(
            JobScrapingPreferences.is_active.is_(True)
        )
    )
    preferences = result.scalar_one_or_none()
    
    if not preferences:
        raise HTTPException(
            status_code=404,
            detail="No active preferences found. Cannot update."
        )
    
    # Update fields
    update_data = prefs_update.model_dump(exclude_unset=True)
    
    for field, value in update_data.items():
        setattr(preferences, field, value)
    
    # Update metadata
    preferences.updated_by = current_user.id
    preferences.updated_at = datetime.utcnow()
    
    # Commit changes
    await db.commit()
    await db.refresh(preferences)
    
    return preferences


@router.get(
    "/job-preferences/stats",
    response_model=ProcessingStatsResponse,
    summary="Get processing statistics"
)
async def get_processing_stats(
    days: int = Query(7, description="Number of days to look back"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_superuser)
):
    """
    Get statistics about message processing and filtering.
    
    Parameters:
    - days: Number of days to look back (default: 7)
    
    Returns statistics from the storage service (local or DynamoDB)
    and preferences status.
    """
    # Get storage service
    storage = get_storage_service()
    
    # Get stats from storage
    stats = await storage.get_processing_stats(days=days)
    
    # Check if preferences are active
    result = await db.execute(
        select(JobScrapingPreferences).where(
            JobScrapingPreferences.is_active.is_(True)
        )
    )
    preferences = result.scalar_one_or_none()
    
    # Determine storage type
    storage_type = "dynamodb" if os.getenv('USE_DYNAMODB', 'false').lower() == 'true' else "local"
    
    # Get storage size (only for local)
    storage_size = None
    if storage_type == "local" and hasattr(storage, 'get_file_size'):
        storage_size = storage.get_file_size()
    
    return ProcessingStatsResponse(
        storage_stats=FilteringStats(**stats),
        storage_type=storage_type,
        storage_size=storage_size,
        preferences_active=preferences is not None,
        last_updated=datetime.utcnow()
    )
