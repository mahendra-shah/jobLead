"""
Student Dashboard API
Overview of student activity and statistics
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.student import Student
from app.models.job import Job
from app.models.student_interactions import SavedJob, JobView, StudentNotification
from app.schemas.student import (
    StudentDashboardResponse,
    JobViewCreate
)

router = APIRouter()


@router.get("/dashboard", response_model=StudentDashboardResponse)
async def get_dashboard(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get student dashboard overview
    
    **Auth**: Student (JWT required)
    
    Returns:
    - Student profile
    - Profile completeness
    - Saved jobs count
    - Recent job views
    - Unread notifications count
    - Recommendations available count
    """
    # Get student by user_id
    result = await db.execute(
        select(Student).where(Student.user_id == current_user.id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Calculate profile completeness
    total_fields = 15
    completed_fields = sum([
        bool(student.first_name),
        bool(student.last_name),
        bool(student.email),
        bool(student.phone),
        bool(student.college_id),
        bool(student.degree),
        bool(student.branch),
        bool(student.passing_year),
        bool(student.cgpa),
        bool(student.resume_url),
        bool(student.skills and len(student.skills) > 0),
        bool(student.preferred_locations and len(student.preferred_locations) > 0),
        bool(student.preferred_job_types and len(student.preferred_job_types) > 0),
        bool(student.min_salary),
        bool(student.max_salary)
    ])
    profile_completeness = int((completed_fields / total_fields) * 100)
    
    # Get saved jobs count
    saved_count_result = await db.execute(
        select(func.count(SavedJob.id)).where(SavedJob.student_id == student.id)
    )
    saved_jobs_count = saved_count_result.scalar()
    
    # Get unread notifications count
    unread_result = await db.execute(
        select(func.count(StudentNotification.id)).where(
            StudentNotification.student_id == student.id,
            StudentNotification.read.is_(False)
        )
    )
    notifications_unread = unread_result.scalar()
    
    # Get recent job views (last 5)
    recent_views_result = await db.execute(
        select(JobView)
        .where(JobView.student_id == student.id)
        .order_by(JobView.viewed_at.desc())
        .limit(5)
    )
    recent_views = recent_views_result.scalars().all()
    
    # Get job details for recent views
    recent_jobs = []
    for view in recent_views:
        job_result = await db.execute(
            select(Job).where(Job.id == view.job_id)
        )
        job = job_result.scalar_one_or_none()
        if job:
            recent_jobs.append({
                "id": job.id,
                "title": job.title,
                "company": job.company,
                "location": job.location,
                "viewed_at": view.viewed_at,
                "duration_seconds": view.duration_seconds
            })
    
    # Get active jobs count (recommendations available)
    active_jobs_result = await db.execute(
        select(func.count(Job.id)).where(Job.is_active.is_(True))
    )
    recommendations_available = active_jobs_result.scalar()
    
    # Build stats
    stats = {
        "profile_completeness": profile_completeness,
        "saved_jobs": saved_jobs_count,
        "unread_notifications": notifications_unread,
        "total_job_views": len(recent_views),
        "recommendations_available": recommendations_available
    }
    
    # Simple student response matching actual model fields
    return StudentDashboardResponse(
        student={
            "id": student.id,
            "user_id": str(student.user_id),
            "full_name": student.full_name,
            "phone": student.phone,
            "skills": student.skills or [],
            "preferences": student.preferences or {},
            "profile_score": student.profile_score,
            "status": student.status,
            "email_notifications": student.email_notifications,
            "created_at": student.created_at,
            "updated_at": student.updated_at
        },
        stats=stats,
        recent_jobs=recent_jobs,
        saved_jobs_count=saved_jobs_count,
        notifications_unread=notifications_unread,
        profile_completeness=profile_completeness,
        recommendations_available=recommendations_available
    )


@router.post("/jobs/{job_id}/view", status_code=status.HTTP_201_CREATED)
async def track_job_view(
    job_id: int,
    view_data: JobViewCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Track when a student views a job
    
    **Auth**: Student (JWT required)
    
    Used for analytics and improving recommendations.
    
    **Request Body**:
    - `job_id`: ID of the viewed job (in path)
    - `duration_seconds`: How long the job was viewed (optional)
    - `source`: Where the job was viewed from (feed, search, notification, bookmark)
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
        select(Job).where(Job.id == job_id)
    )
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with id {job_id} not found"
        )
    
    # Create job view record
    db_view = JobView(
        student_id=student.id,
        job_id=job_id,
        duration_seconds=view_data.duration_seconds,
        source=view_data.source
    )
    
    db.add(db_view)
    await db.commit()
    
    return {
        "message": "Job view tracked successfully",
        "job_id": job_id,
        "student_id": student.id
    }


@router.get("/students/me/activity")
async def get_activity(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get student activity statistics
    
    **Auth**: Student (JWT required)
    
    Returns detailed analytics about student engagement.
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
    
    # Total job views
    total_views_result = await db.execute(
        select(func.count(JobView.id)).where(JobView.student_id == student.id)
    )
    total_views = total_views_result.scalar()
    
    # Unique jobs viewed
    unique_jobs_result = await db.execute(
        select(func.count(func.distinct(JobView.job_id))).where(JobView.student_id == student.id)
    )
    unique_jobs_viewed = unique_jobs_result.scalar()
    
    # Average view duration
    avg_duration_result = await db.execute(
        select(func.avg(JobView.duration_seconds)).where(
            JobView.student_id == student.id,
            JobView.duration_seconds.isnot(None)
        )
    )
    avg_duration = avg_duration_result.scalar() or 0
    
    # Views by source
    source_result = await db.execute(
        select(
            JobView.source,
            func.count(JobView.id).label("count")
        )
        .where(JobView.student_id == student.id)
        .group_by(JobView.source)
    )
    views_by_source = {row[0]: row[1] for row in source_result.all()}
    
    # Saved jobs count
    saved_count_result = await db.execute(
        select(func.count(SavedJob.id)).where(SavedJob.student_id == student.id)
    )
    saved_jobs = saved_count_result.scalar()
    
    return {
        "total_views": total_views,
        "unique_jobs_viewed": unique_jobs_viewed,
        "average_view_duration_seconds": round(avg_duration, 2),
        "views_by_source": views_by_source,
        "saved_jobs": saved_jobs,
        "engagement_score": min(100, (total_views * 2) + (saved_jobs * 5))  # Simple engagement metric
    }
