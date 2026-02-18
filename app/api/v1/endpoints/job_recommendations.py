"""
Job Recommendations API
Personalized job feed for students
"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.student import Student
from app.models.job import Job
from app.services.job_recommendation_service import JobRecommendationService
from app.schemas.student import RecommendedJobsResponse

router = APIRouter()


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
    - Skill Match: 40%
    - Location Match: 20%
    - Experience Match: 15%
    - Job Type Match: 10%
    - Company Preference: 10%
    - Freshness: 5%
    
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
    # Get student profile by user_id
    result = await db.execute(
        select(Student).where(Student.user_id == current_user.id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found. Please complete your profile first."
        )
    
    # Check if student has set skills (technical_skills or soft_skills)
    has_technical_skills = student.technical_skills and len(student.technical_skills) > 0
    has_soft_skills = student.soft_skills and len(student.soft_skills) > 0
    has_legacy_skills = student.skills and len(student.skills) > 0
    
    if not (has_technical_skills or has_soft_skills or has_legacy_skills):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please add your technical skills or soft skills in profile to get personalized recommendations."
        )
    
    # Get recommendations using service
    recommendation_service = JobRecommendationService(db)
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
    recommendation_service = JobRecommendationService(db)
    similar_jobs = await recommendation_service.get_similar_jobs(
        job_id=job_uuid,
        limit=limit
    )
    
    # Format response
    return {
        "reference_job": {
            "id": str(job.id),
            "title": job.title,
            "company": job.company
        },
        "similar_jobs": [
            {
                "id": str(j.id),
                "title": j.title,
                "company": j.company,
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


@router.get("/students/me/recommendation-stats")
async def get_recommendation_stats(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get recommendation statistics for current student
    
    **Auth**: Student (JWT required)
    
    Returns:
    - Total jobs available
    - High match jobs (score >= 80)
    - Medium match jobs (score 60-79)
    - Low match jobs (score 50-59)
    - Jobs matching skills
    - Jobs matching location
    """
    # Get student profile
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Get all recommendations with low min_score to calculate stats
    recommendation_service = JobRecommendationService(db)
    all_recommendations = await recommendation_service.get_recommendations(
        student=student,
        limit=1000,  # Get all
        offset=0,
        min_score=0.0,  # Get all jobs
        exclude_saved=False,
        exclude_viewed=False
    )
    
    # Calculate statistics
    total_jobs = len(all_recommendations)
    high_match = sum(1 for r in all_recommendations if r["recommendation_score"] >= 80)
    medium_match = sum(1 for r in all_recommendations if 60 <= r["recommendation_score"] < 80)
    low_match = sum(1 for r in all_recommendations if 50 <= r["recommendation_score"] < 60)
    
    # Count jobs matching specific criteria
    skill_matches = sum(1 for r in all_recommendations if r["score_breakdown"]["skill_score"] >= 20)
    location_matches = sum(1 for r in all_recommendations if r["score_breakdown"]["location_score"] >= 10)
    fresher_friendly = sum(1 for r in all_recommendations if r["score_breakdown"]["experience_score"] >= 10)
    
    return {
        "total_jobs_available": total_jobs,
        "match_distribution": {
            "high_match": high_match,  # 80+
            "medium_match": medium_match,  # 60-79
            "low_match": low_match  # 50-59
        },
        "criteria_matches": {
            "skill_matches": skill_matches,
            "location_matches": location_matches,
            "fresher_friendly": fresher_friendly
        },
        "top_recommendations": all_recommendations[:5] if all_recommendations else []
    }
