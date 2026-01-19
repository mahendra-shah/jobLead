"""
Saved Jobs API
Students can bookmark jobs with folders and notes
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.student import Student
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


@router.post("/students/me/saved-jobs", response_model=SavedJobResponse, status_code=status.HTTP_201_CREATED)
async def save_job(
    saved_job_in: SavedJobCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Save/bookmark a job
    
    **Auth**: Student (JWT required)
    
    Students can organize saved jobs into folders and add notes.
    """
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Check if job exists
    result = await db.execute(
        select(Job).where(Job.id == saved_job_in.job_id)
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
            SavedJob.student_id == student.id,
            SavedJob.job_id == saved_job_in.job_id
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
        student_id=student.id,
        job_id=saved_job_in.job_id,
        folder=saved_job_in.folder,
        notes=saved_job_in.notes
    )
    
    db.add(db_saved_job)
    await db.commit()
    await db.refresh(db_saved_job)
    
    # Load job details
    await db.refresh(db_saved_job, ["job"])
    
    return SavedJobResponse(
        id=db_saved_job.id,
        job_id=db_saved_job.job_id,
        job={
            "id": job.id,
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "job_type": job.job_type,
            "skills": job.skills or [],
            "apply_link": job.apply_link,
            "posted_at": job.posted_at,
            "deadline": job.deadline
        },
        folder=db_saved_job.folder,
        notes=db_saved_job.notes,
        saved_at=db_saved_job.saved_at
    )


@router.get("/students/me/saved-jobs", response_model=SavedJobsResponse)
async def list_saved_jobs(
    folder: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    List all saved jobs
    
    **Auth**: Student (JWT required)
    
    **Query Parameters**:
    - `folder`: Filter by folder name (optional)
    """
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Build query
    query = select(SavedJob).where(SavedJob.student_id == student.id)
    
    if folder:
        query = query.where(SavedJob.folder == folder)
    
    query = query.order_by(SavedJob.saved_at.desc())
    
    # Execute
    result = await db.execute(query)
    saved_jobs = result.scalars().all()
    
    # Get unique folders
    folders_result = await db.execute(
        select(SavedJob.folder).where(
            SavedJob.student_id == student.id,
            SavedJob.folder.isnot(None)
        ).distinct()
    )
    folders = [row[0] for row in folders_result.fetchall()]
    
    # Format response with job details
    saved_jobs_response = []
    for saved_job in saved_jobs:
        # Get job details
        job_result = await db.execute(
            select(Job).where(Job.id == saved_job.job_id)
        )
        job = job_result.scalar_one_or_none()
        
        saved_jobs_response.append(SavedJobResponse(
            id=saved_job.id,
            job_id=saved_job.job_id,
            job={
                "id": job.id,
                "title": job.title,
                "company": job.company,
                "location": job.location,
                "job_type": job.job_type,
                "skills": job.skills or [],
                "apply_link": job.apply_link,
                "posted_at": job.posted_at,
                "deadline": job.deadline
            } if job else None,
            folder=saved_job.folder,
            notes=saved_job.notes,
            saved_at=saved_job.saved_at
        ))
    
    return SavedJobsResponse(
        total=len(saved_jobs_response),
        saved_jobs=saved_jobs_response,
        folders=folders
    )


@router.patch("/students/me/saved-jobs/{saved_job_id}", response_model=SavedJobResponse)
async def update_saved_job(
    saved_job_id: int,
    saved_job_update: SavedJobUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update saved job folder or notes
    
    **Auth**: Student (JWT required)
    """
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Get saved job
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.id == saved_job_id,
            SavedJob.student_id == student.id
        )
    )
    saved_job = result.scalar_one_or_none()
    
    if not saved_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Saved job not found"
        )
    
    # Update fields
    update_data = saved_job_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(saved_job, field, value)
    
    await db.commit()
    await db.refresh(saved_job)
    
    # Get job details
    job_result = await db.execute(
        select(Job).where(Job.id == saved_job.job_id)
    )
    job = job_result.scalar_one_or_none()
    
    return SavedJobResponse(
        id=saved_job.id,
        job_id=saved_job.job_id,
        job={
            "id": job.id,
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "job_type": job.job_type,
            "skills": job.skills or [],
            "apply_link": job.apply_link,
            "posted_at": job.posted_at,
            "deadline": job.deadline
        } if job else None,
        folder=saved_job.folder,
        notes=saved_job.notes,
        saved_at=saved_job.saved_at
    )


@router.delete("/students/me/saved-jobs/{saved_job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_saved_job(
    saved_job_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Remove a saved job
    
    **Auth**: Student (JWT required)
    """
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Get saved job
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.id == saved_job_id,
            SavedJob.student_id == student.id
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


@router.get("/students/me/saved-jobs/folders", response_model=list[FolderResponse])
async def list_folders(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    List all folders with job counts
    
    **Auth**: Student (JWT required)
    """
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Get folders with counts
    result = await db.execute(
        select(
            SavedJob.folder,
            func.count(SavedJob.id).label("count")
        )
        .where(
            SavedJob.student_id == student.id,
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
            SavedJob.student_id == student.id,
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


@router.get("/students/me/saved-jobs/check/{job_id}")
async def check_if_saved(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Check if a job is already saved
    
    **Auth**: Student (JWT required)
    
    Useful for UI to show "Saved" vs "Save" button
    """
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Check if saved
    result = await db.execute(
        select(SavedJob).where(
            SavedJob.student_id == student.id,
            SavedJob.job_id == job_id
        )
    )
    saved_job = result.scalar_one_or_none()
    
    return {
        "job_id": job_id,
        "is_saved": saved_job is not None,
        "saved_job_id": saved_job.id if saved_job else None,
        "folder": saved_job.folder if saved_job else None
    }
