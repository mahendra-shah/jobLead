"""
Student Dashboard API
Overview of student activity and statistics
"""

from uuid import UUID

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

def _has_value(v) -> bool:
    """Generic truthy check that handles lists/dicts/strings consistently."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (list, dict)):
        return len(v) > 0
    if isinstance(v, str):
        return v.strip() != ""
    return True


def _calculate_profile_completeness(student: Student) -> int:
    """
    Compute profile completeness based on the current Student model fields.
    Returns an int percentage (0-100).
    """
    checks = [
        student.first_name,
        student.last_name,
        student.phone,
        student.date_of_birth,
        student.gender,
        student.current_address,
        student.highest_qualification,
        student.college_name,
        student.course,
        student.branch,
        student.passing_year,
        student.technical_skills,
        student.soft_skills,
        student.resume_url,
    ]
    
    # Check preference JSONB for job preferences
    if student.preference and isinstance(student.preference, dict):
        # Check if any preference field is filled
        has_job_prefs = any([
            student.preference.get('job_type') and len(student.preference.get('job_type', [])) > 0,
            student.preference.get('work_mode') and len(student.preference.get('work_mode', [])) > 0,
            student.preference.get('preferred_job_role') and len(student.preference.get('preferred_job_role', [])) > 0,
            student.preference.get('preferred_location') and len(student.preference.get('preferred_location', [])) > 0,
        ])
        if has_job_prefs:
            checks.append(True)  # At least one preference is set
    
    total = len(checks)
    filled = sum(1 for v in checks if _has_value(v))
    return int((filled / total) * 100) if total else 0


def _student_profile_payload(student: Student, current_user: User) -> dict:
    """
    Build a StudentProfile-like dict used by the frontend.
    (Most keys are optional; frontend types are permissive.)
    """
    # Serialize nested JSON fields safely
    internship_details = student.internship_details or []
    projects = student.projects or []
    languages = student.languages or []

    # date_of_birth -> ISO string
    dob = None
    if student.date_of_birth:
        dob = student.date_of_birth.isoformat() if hasattr(student.date_of_birth, "isoformat") else str(student.date_of_birth)

    return {
        "id": str(student.id) if getattr(student, "id", None) else None,
        "is_active": getattr(current_user, "is_active", None),
        "created_at": getattr(student, "created_at", None),
        "updated_at": getattr(student, "updated_at", None),

        # Personal
        "first_name": student.first_name,
        "last_name": student.last_name,
        "full_name": student.full_name,
        "phone": student.phone,
        "email": getattr(current_user, "email", None),
        "date_of_birth": dob,
        "gender": student.gender,
        "current_address": student.current_address,

        # Education
        "highest_qualification": student.highest_qualification,
        "college_name": student.college_name,
        "college_id": student.college_id,
        "course": student.course,
        "branch": student.branch,
        "passing_year": student.passing_year,
        "percentage": student.percentage,
        "cgpa": student.cgpa,

        # Skills
        "technical_skills": student.technical_skills or [],
        "soft_skills": student.soft_skills or [],

        # Experience
        "experience_type": student.experience_type,
        "internship_details": internship_details,
        "projects": projects,

        # Languages
        "languages": languages,

        # Preferences (from JSONB preference column)
        "preference": student.preference or {},

        # Links
        "github_profile": student.github_profile,
        "linkedin_profile": student.linkedin_profile,
        "portfolio_url": student.portfolio_url,
        "coding_platforms": student.coding_platforms or {},

        # Resume
        "resume_url": student.resume_url,
    }


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
    
    # If profile doesn't exist yet, still return a valid dashboard with 0% completion.
    if not student:
        saved_jobs_count = 0
        notifications_unread = 0
        recommendations_available = 0
        profile_completeness = 0
        stats = {
            "profile_completeness": profile_completeness,
            "saved_jobs": saved_jobs_count,
            "unread_notifications": notifications_unread,
            "total_job_views": 0,
            "recommendations_available": recommendations_available,
        }
        return StudentDashboardResponse(
            student={"email": getattr(current_user, "email", None)},
            stats=stats,
            recent_jobs=[],
            saved_jobs_count=saved_jobs_count,
            notifications_unread=notifications_unread,
            profile_completeness=profile_completeness,
            recommendations_available=recommendations_available,
        )

    # Calculate profile completeness (updated to match current Student model)
    profile_completeness = _calculate_profile_completeness(student)
    
    # Get saved jobs count
    saved_count_result = await db.execute(
        select(func.count(SavedJob.id)).where(SavedJob.user_id == current_user.id)
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
    
    return StudentDashboardResponse(
        student=_student_profile_payload(student, current_user),
        stats=stats,
        recent_jobs=recent_jobs,
        saved_jobs_count=saved_jobs_count,
        notifications_unread=notifications_unread,
        profile_completeness=profile_completeness,
        recommendations_available=recommendations_available
    )


@router.post("/jobs/{job_id}/view", status_code=status.HTTP_201_CREATED)
async def track_job_view(
    job_id: str,
    view_data: JobViewCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Track when a student views a job
    
    **Auth**: Student (JWT required)
    
    Used for analytics and improving recommendations.
    
    **Request Body**:
    - `job_id`: ID of the viewed job (UUID in path)
    - `duration_seconds`: How long the job was viewed (optional)
    - `source`: Where the job was viewed from (feed, search, notification, bookmark)
    """
    # Validate and convert job_id to UUID (API receives string from frontend)
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid job_id format. Must be a valid UUID."
        )

    # Get student
    result = await db.execute(
        select(Student).where(Student.user_id == current_user.id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Check if job exists
    result = await db.execute(
        select(Job).where(Job.id == job_uuid)
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
        job_id=job_uuid,
        duration_seconds=view_data.duration_seconds,
        source=view_data.source
    )
    
    db.add(db_view)
    await db.commit()
    
    return {
        "message": "Job view tracked successfully",
        "job_id": str(job_uuid),
        "student_id": student.id
    }


@router.get("/activity")
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
        select(Student).where(Student.user_id == current_user.id)
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
        select(func.count(SavedJob.id)).where(SavedJob.user_id == current_user.id)
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
