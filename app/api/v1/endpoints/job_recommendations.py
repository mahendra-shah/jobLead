"""
Job Recommendations API
Personalized job feed for students
"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.student import Student
from app.models.job import Job
from app.services.job_recommendation_service import JobRecommendationService
from app.schemas.student import RecommendedJobsResponse
from app.schemas.job import CompanyBrief

router = APIRouter()


def _as_string_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if item is not None and str(item).strip()]
    if isinstance(value, str) and value.strip():
        if "," in value:
            return [part.strip() for part in value.split(",") if part.strip()]
        return [value.strip()]
    return []


def get_cache_manager():
    """Get cache manager without circular import."""
    from app.main import cache_manager
    return cache_manager


async def _get_student_for_current_user(db: AsyncSession, current_user: User):
    if not current_user or not current_user.email:
        return None
    result = await db.execute(
        select(Student).where(func.lower(Student.email) == current_user.email.strip().lower())
    )
    return result.scalar_one_or_none()


@router.get("/recommended-jobs", response_model=RecommendedJobsResponse)
async def get_recommended_jobs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    min_score: float = Query(50.0, ge=0, le=100),
    exclude_saved: bool = Query(False),
    exclude_viewed: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get personalized job recommendations for current student
    
    **Auth**: Student (JWT required)
    
    **Scoring Algorithm** (0-100):
    - Skill Match: 45%
    - Location Match: 20%
    - Experience Match: 15%
    - Job Type Match: 10%
    - Freshness: 10%
    
    **Query Parameters**:
    - `limit`: Max recommendations to return (1-100, default: 20)
    - `offset`: Pagination offset (default: 0)
    - `min_score`: Minimum recommendation score (0-100, default: 50)
    - `exclude_saved`: Skip saved jobs (default: false)
    - `exclude_viewed`: Skip viewed jobs (default: false)
    
    **Response**:
    Each recommendation includes:
    - Full job details (title, company, location, skills, apply_link)
    - Recommendation score (0-100)
    - Match reasons with emojis
    - Missing skills
    - Score breakdown by factor
    - Is saved status
    
    **Example**:
    ```
    GET /api/v1/students/me/recommended-jobs?limit=10&min_score=60&exclude_saved=true
    ```
    """
    student = await _get_student_for_current_user(db, current_user)
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found. Please complete your profile first."
        )
    
    # Normalize possible legacy/malformed skill shapes from DB
    student.technical_skills = _as_string_list(getattr(student, "technical_skills", None))
    student.soft_skills = _as_string_list(getattr(student, "soft_skills", None))

    # Check if student has set skills (technical_skills or soft_skills)
    has_technical_skills = len(student.technical_skills) > 0
    has_soft_skills = len(student.soft_skills) > 0
    
    if not (has_technical_skills or has_soft_skills):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please add your technical skills or soft skills in profile to get personalized recommendations."
        )
    
    # Get recommendations using service
    recommendation_service = JobRecommendationService(db, get_cache_manager())
    recommendations = await recommendation_service.get_recommendations(
        student=student,
        limit=limit,
        offset=offset,
        min_score=min_score,
        exclude_saved=exclude_saved,
        exclude_viewed=exclude_viewed
    )
    
    # Count total matching jobs (for pagination)
    # For simplicity, we'll use the length of recommendations
    # In production, you might want to count separately
    total = len(recommendations) + offset
    
    return RecommendedJobsResponse(
        total=total,
        limit=limit,
        offset=offset,
        recommendations=recommendations,
        filters_applied={
            "min_score": min_score,
            "exclude_saved": exclude_saved,
            "exclude_viewed": exclude_viewed
        }
    )


@router.get("/jobs/{job_id}/similar")
async def get_similar_jobs(
    job_id: str,
    limit: int = Query(5, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get similar jobs based on skills and company
    
    **Auth**: Student (JWT required)
    
    Returns jobs from the same company or with similar skills.
    Useful for "You might also like" sections.
    
    **Query Parameters**:
    - `limit`: Max similar jobs to return (1-20, default: 5)
    """
    # Validate and convert job_id to UUID
    try:
        job_uuid = UUID(job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid job_id format. Must be a valid UUID."
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
    
    # Get similar jobs
    recommendation_service = JobRecommendationService(db, get_cache_manager())
    similar_jobs = await recommendation_service.get_similar_jobs(
        job_id=job_uuid,
        limit=limit
    )
    
    # Format response
    return {
        "reference_job": {
            "id": str(job.id),
            "title": job.title,
            "company": CompanyBrief.model_validate(job.company).model_dump() if job.company else None
        },
        "similar_jobs": [
            {
                "id": str(j.id),
                "title": j.title,
                "company": CompanyBrief.model_validate(j.company).model_dump() if j.company else None,
                "location": j.location,
                "job_type": j.job_type,
                "skills": j.skills_required or [],
                "apply_link": j.source_url,
                "posted_at": j.created_at
            }
            for j in similar_jobs
        ],
        "total": len(similar_jobs)
    }


@router.get("/recommendation-stats")
async def get_recommendation_stats(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get recommendation statistics for current student.

    Uses lightweight DB count queries and the already-warmed
    recommendation cache where available.  Never triggers a full
    re-score of 500 jobs.

    **Auth**: Student (JWT required)

    Returns:
    - total_jobs_available: active high-quality jobs in last 7 days
    - match_distribution: high/medium/low score bands (from cache)
    - criteria_matches: skill-match count, fresher-friendly count
    - top_recommendations: first 5 items from the warmed cache
    """
    result = await db.execute(
        select(Student).where(Student.user_id == current_user.id)
    )
    student = result.scalar_one_or_none()

    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )

    recommendation_service = JobRecommendationService(db, get_cache_manager())
    return await recommendation_service.get_recommendation_counts(student)
