"""
Saved Jobs API
Users can bookmark jobs with folders and notes
"""

from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import joinedload

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.job import Job
from app.models.student_interactions import SavedJob
from app.schemas.student import (
    SavedJobCreate,
    SavedJobUpdate,
    SavedJobResponse,
    SavedJobsResponse,
    FolderResponse
)

router = APIRouter()


@router.post("", response_model=SavedJobResponse, status_code=status.HTTP_201_CREATED)
async def save_job(
    saved_job_in: SavedJobCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Save/bookmark a job
    
    **Auth**: User (JWT required)
    
    Users can organize saved jobs into folders and add notes.
    """
    # Check if job exists
    try:
        job_uuid = UUID(saved_job_in.job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid job_id format. Must be a valid UUID."
        )
    
    result = await db.execute(
        select(Job)
        .options(joinedload(Job.company))
        .where(Job.id == job_uuid)
    )
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with id {saved_job_in.job_id} not found"
        )
    
    # Check if already saved
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.user_id == current_user.id,
            SavedJob.job_id == job_uuid
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job already saved"
        )
    
    # Create saved job
    db_saved_job = SavedJob(
        user_id=current_user.id,
        job_id=job_uuid,
        folder=saved_job_in.folder,
        notes=saved_job_in.notes
    )
    
    db.add(db_saved_job)
    await db.commit()
    await db.refresh(db_saved_job)
    
    # Format job data for response
    job_data = {
        "id": str(job.id),
        "title": job.title,
        "company_id": str(job.company_id) if job.company_id else None,
        "company_name": job.company.name if job.company else "Unknown",
        "description": job.description,
        "skills_required": job.skills_required or [],
        "experience_required": job.experience_required,
        "salary_range": job.salary_range or {},
        "is_fresher": job.is_fresher,
        "work_type": job.work_type,
        "experience_min": job.experience_min,
        "experience_max": job.experience_max,
        "salary_min": float(job.salary_min) if job.salary_min else None,
        "salary_max": float(job.salary_max) if job.salary_max else None,
        "location": job.location,
        "job_type": job.job_type,
        "employment_type": job.employment_type,
        "source": job.source,
        "source_url": job.source_url,
        "is_active": job.is_active,
        "view_count": job.view_count,
        "application_count": job.application_count,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }
    
    return SavedJobResponse(
        id=str(db_saved_job.id),
        user_id=str(db_saved_job.user_id),
        job_id=str(db_saved_job.job_id),
        job=job_data,
        folder=db_saved_job.folder,
        notes=db_saved_job.notes,
        saved_at=db_saved_job.saved_at,
        created_at=db_saved_job.created_at,
        updated_at=db_saved_job.updated_at
    )


@router.get("", response_model=SavedJobsResponse)
async def list_saved_jobs(
    folder: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    List all saved jobs
    
    **Auth**: User (JWT required)
    
    **Query Parameters**:
    - `folder`: Filter by folder name (optional)
    """
    # Build query
    query = select(SavedJob).where(SavedJob.user_id == current_user.id)
    
    if folder:
        query = query.where(SavedJob.folder == folder)
    
    query = query.order_by(SavedJob.saved_at.desc())
    
    # Execute
    result = await db.execute(query)
    saved_jobs = result.scalars().all()
    
    # Get unique folders
    folders_result = await db.execute(
        select(SavedJob.folder).where(
            SavedJob.user_id == current_user.id,
            SavedJob.folder.isnot(None)
        ).distinct()
    )
    folders = [row[0] for row in folders_result.fetchall()]
    
    # Format response with job details
    saved_jobs_response = []
    for saved_job in saved_jobs:
        # Get job details with company
        job_result = await db.execute(
            select(Job)
            .options(joinedload(Job.company))
            .where(Job.id == saved_job.job_id)
        )
        job = job_result.scalar_one_or_none()
        
        if job:
            job_data = {
                "id": str(job.id),
                "title": job.title,
                "company_id": str(job.company_id) if job.company_id else None,
                "company_name": job.company.name if job.company else "Unknown",
                "description": job.description,
                "skills_required": job.skills_required or [],
                "experience_required": job.experience_required,
                "salary_range": job.salary_range or {},
                "is_fresher": job.is_fresher,
                "work_type": job.work_type,
                "experience_min": job.experience_min,
                "experience_max": job.experience_max,
                "salary_min": float(job.salary_min) if job.salary_min else None,
                "salary_max": float(job.salary_max) if job.salary_max else None,
                "location": job.location,
                "job_type": job.job_type,
                "employment_type": job.employment_type,
                "source": job.source,
                "source_url": job.source_url,
                "is_active": job.is_active,
                "view_count": job.view_count,
                "application_count": job.application_count,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "updated_at": job.updated_at.isoformat() if job.updated_at else None,
            }
        else:
            job_data = None
        
        saved_jobs_response.append(SavedJobResponse(
            id=str(saved_job.id),
            user_id=str(saved_job.user_id),
            job_id=str(saved_job.job_id),
            job=job_data,
            folder=saved_job.folder,
            notes=saved_job.notes,
            saved_at=saved_job.saved_at,
            created_at=saved_job.created_at,
            updated_at=saved_job.updated_at
        ))
    
    return SavedJobsResponse(
        total=len(saved_jobs_response),
        saved_jobs=saved_jobs_response,
        folders=folders
    )


@router.patch("/{saved_job_id}", response_model=SavedJobResponse)
async def update_saved_job(
    saved_job_id: str,
    saved_job_update: SavedJobUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update saved job folder or notes
    
    **Auth**: User (JWT required)
    """
    try:
        saved_job_uuid = UUID(saved_job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid saved_job_id format. Must be a valid UUID."
        )
    
    # Get saved job
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.id == saved_job_uuid,
            SavedJob.user_id == current_user.id
        )
    )
    saved_job = result.scalar_one_or_none()
    
    if not saved_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Saved job not found"
        )
    
    # Update fields
    update_data = saved_job_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(saved_job, field, value)
    
    await db.commit()
    await db.refresh(saved_job)
    
    # Get job details
    job_result = await db.execute(
        select(Job)
        .options(joinedload(Job.company))
        .where(Job.id == saved_job.job_id)
    )
    job = job_result.scalar_one_or_none()
    
    if job:
        job_data = {
            "id": str(job.id),
            "title": job.title,
            "company_id": str(job.company_id) if job.company_id else None,
            "company_name": job.company.name if job.company else "Unknown",
            "description": job.description,
            "skills_required": job.skills_required or [],
            "experience_required": job.experience_required,
            "salary_range": job.salary_range or {},
            "is_fresher": job.is_fresher,
            "work_type": job.work_type,
            "experience_min": job.experience_min,
            "experience_max": job.experience_max,
            "salary_min": float(job.salary_min) if job.salary_min else None,
            "salary_max": float(job.salary_max) if job.salary_max else None,
            "location": job.location,
            "job_type": job.job_type,
            "employment_type": job.employment_type,
            "source": job.source,
            "source_url": job.source_url,
            "is_active": job.is_active,
            "view_count": job.view_count,
            "application_count": job.application_count,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        }
    else:
        job_data = None
    
    return SavedJobResponse(
        id=str(saved_job.id),
        user_id=str(saved_job.user_id),
        job_id=str(saved_job.job_id),
        job=job_data,
        folder=saved_job.folder,
        notes=saved_job.notes,
        saved_at=saved_job.saved_at,
        created_at=saved_job.created_at,
        updated_at=saved_job.updated_at
    )


@router.delete("/{saved_job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_saved_job(
    saved_job_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Remove a saved job
    
    **Auth**: User (JWT required)
    """
    try:
        saved_job_uuid = UUID(saved_job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid saved_job_id format. Must be a valid UUID."
        )
    
    # Get saved job
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.id == saved_job_uuid,
            SavedJob.user_id == current_user.id
        )
    )
    saved_job = result.scalar_one_or_none()
    
    if not saved_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Saved job not found"
        )
    
    # Delete
    await db.delete(saved_job)
    await db.commit()
    
    return None


@router.get("/folders", response_model=list[FolderResponse])
async def list_folders(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    List all folders with job counts
    
    **Auth**: User (JWT required)
    """
    # Get folders with counts
    result = await db.execute(
        select(
            SavedJob.folder,
            func.count(SavedJob.id).label("count")
        )
        .where(
            SavedJob.user_id == current_user.id,
            SavedJob.folder.isnot(None)
        )
        .group_by(SavedJob.folder)
    )
    
    folders = []
    for row in result.fetchall():
        folders.append(FolderResponse(
            name=row[0],
            count=row[1]
        ))
    
    # Add "No Folder" count
    no_folder_result = await db.execute(
        select(func.count(SavedJob.id))
        .where(
            SavedJob.user_id == current_user.id,
            SavedJob.folder.is_(None)
        )
    )
    no_folder_count = no_folder_result.scalar()
    
    if no_folder_count > 0:
        folders.append(FolderResponse(
            name="No Folder",
            count=no_folder_count
        ))
    
    return folders


@router.get("/check/{job_id}")
async def check_if_saved(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Check if a job is already saved
    
    **Auth**: User (JWT required)
    
    Useful for UI to show "Saved" vs "Save" button
    """
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid job_id format. Must be a valid UUID."
        )
    
    # Check if saved
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.user_id == current_user.id,
            SavedJob.job_id == job_uuid
        )
    )
    saved_job = result.scalar_one_or_none()
    
    return {
        "job_id": job_id,
        "is_saved": saved_job is not None,
        "saved_job_id": str(saved_job.id) if saved_job else None,
        "folder": saved_job.folder if saved_job else None
    }
