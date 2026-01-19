"""
Admin endpoints for managing job scraping preferences
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from datetime import datetime
import os

from app.db.session import get_db
from app.models.job_scraping_preferences import JobScrapingPreferences
from app.schemas.job_preferences import (
    JobPreferencesResponse,
    JobPreferencesUpdate,
    ProcessingStatsResponse,
    FilteringStats
)
from app.core.security import get_current_user
from app.models.user import User
from app.services.storage_factory import get_storage_service

router = APIRouter()


@router.get(
    "/job-preferences",
    response_model=JobPreferencesResponse,
    summary="Get active job scraping preferences"
)
async def get_active_preferences(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
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
            status_code=status.HTTP_404_NOT_FOUND,
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
    current_user: User = Depends(get_current_user)
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
            status_code=status.HTTP_404_NOT_FOUND,
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
    days: int = 7,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
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


@router.post(
    "/job-preferences/activate/{preference_id}",
    response_model=JobPreferencesResponse,
    summary="Activate specific preferences"
)
async def activate_preferences(
    preference_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Activate a specific preference set and deactivate all others.
    
    This allows switching between different preference configurations.
    Only admins can access this endpoint.
    """
    # Deactivate all preferences
    await db.execute(
        update(JobScrapingPreferences).values(is_active=False)
    )
    
    # Get and activate the specified preference
    result = await db.execute(
        select(JobScrapingPreferences).where(
            JobScrapingPreferences.id == preference_id
        )
    )
    preferences = result.scalar_one_or_none()
    
    if not preferences:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Preferences with id {preference_id} not found"
        )
    
    preferences.is_active = True
    preferences.updated_by = current_user.id
    preferences.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(preferences)
    
    return preferences


@router.get(
    "/job-preferences/history",
    response_model=list[JobPreferencesResponse],
    summary="Get all preference configurations"
)
async def get_all_preferences(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all job scraping preference configurations (active and inactive).
    
    This is useful for viewing history and switching between configurations.
    Only admins can access this endpoint.
    """
    result = await db.execute(
        select(JobScrapingPreferences).order_by(
            JobScrapingPreferences.created_at.desc()
        )
    )
    preferences = result.scalars().all()
    
    return preferences
