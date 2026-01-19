"""Job endpoints - Browse and search jobs."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_, and_, desc
from sqlalchemy.orm import joinedload

from app.api.deps import get_db
from app.models.job import Job
from app.schemas.job import JobListResponse, JobDetailResponse

router = APIRouter()


@router.get("/", response_model=JobListResponse)
async def list_jobs(
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(20, ge=1, le=100, description="Items per page"),
    location: Optional[str] = Query(None, description="Filter by location (e.g., 'Pune', 'Remote')"),
    skills: Optional[str] = Query(None, description="Filter by skills (comma-separated, e.g., 'Python,Django')"),
    job_type: Optional[str] = Query(None, description="Filter by job type (remote, office, hybrid)"),
    employment_type: Optional[str] = Query(None, description="Filter by employment type (fulltime, parttime, contract, internship)"),
    experience: Optional[str] = Query(None, description="Filter by experience (e.g., '0-2', '2-5')"),
    company_id: Optional[str] = Query(None, description="Filter by company ID"),
    is_active: bool = Query(True, description="Show only active jobs"),
    sort_by: str = Query("created_at", description="Sort by field (created_at, title, location)"),
    sort_order: str = Query("desc", description="Sort order (asc, desc)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get paginated list of jobs with filters
    
    **Filters:**
    - `location`: City name or 'Remote' (case-insensitive, partial match)
    - `skills`: Comma-separated skills (matches any)
    - `job_type`: remote, office, hybrid
    - `employment_type`: fulltime, parttime, contract, internship
    - `experience`: Experience range (e.g., '0-2 years', '2-5 years')
    - `company_id`: Filter by specific company
    - `is_active`: Show only active jobs (default: true)
    
    **Sorting:**
    - `sort_by`: created_at, title, location, view_count
    - `sort_order`: asc, desc (default: desc)
    
    **Pagination:**
    - `page`: Page number (starts at 1)
    - `size`: Items per page (max: 100)
    
    **Example:**
    ```
    GET /api/v1/jobs/?location=Pune&skills=Python,Django&job_type=remote&page=1&size=20
    ```
    """
    # Build base query
    query = select(Job).options(joinedload(Job.company))
    
    # Apply filters
    filters = []
    
    if is_active:
        filters.append(Job.is_active.is_(True))
    
    if location:
        # Case-insensitive partial match
        filters.append(Job.location.ilike(f"%{location}%"))
    
    if skills:
        # Split skills and match any
        skill_list = [s.strip() for s in skills.split(",")]
        # Check if any of the skills exist in the job's skills_required array
        skill_filters = [
            func.jsonb_exists(Job.skills_required, skill) for skill in skill_list
        ]
        filters.append(or_(*skill_filters))
    
    if job_type:
        filters.append(Job.job_type.ilike(f"%{job_type}%"))
    
    if employment_type:
        filters.append(Job.employment_type.ilike(f"%{employment_type}%"))
    
    if experience:
        filters.append(Job.experience_required.ilike(f"%{experience}%"))
    
    if company_id:
        filters.append(Job.company_id == company_id)
    
    if filters:
        query = query.where(and_(*filters))
    
    # Get total count
    count_query = select(func.count()).select_from(Job)
    if filters:
        count_query = count_query.where(and_(*filters))
    
    result = await db.execute(count_query)
    total = result.scalar()
    
    # Apply sorting
    sort_column = getattr(Job, sort_by, Job.created_at)
    if sort_order.lower() == "desc":
        query = query.order_by(desc(sort_column))
    else:
        query = query.order_by(sort_column)
    
    # Apply pagination
    query = query.offset((page - 1) * size).limit(size)
    
    # Execute query
    result = await db.execute(query)
    jobs = result.scalars().unique().all()
    
    # Format response
    items = []
    for job in jobs:
        items.append({
            "id": str(job.id),
            "title": job.title,
            "company_id": str(job.company_id) if job.company_id else None,
            "company_name": job.company.name if job.company else "Unknown",
            "description": job.description,
            "skills_required": job.skills_required or [],
            "experience_required": job.experience_required,
            "salary_range": job.salary_range or {},
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
        })
    
    return JobListResponse(
        items=items,
        total=total,
        page=page,
        size=size,
        pages=(total + size - 1) // size  # Ceiling division
    )


@router.get("/{job_id}", response_model=JobDetailResponse)
async def get_job(
    job_id: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed information about a specific job
    
    **Parameters:**
    - `job_id`: UUID of the job
    
    **Returns:**
    - Complete job details including company information
    """
    # Query job with company relationship
    result = await db.execute(
        select(Job)
        .options(joinedload(Job.company))
        .where(Job.id == job_id)
    )
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobDetailResponse(
        id=str(job.id),
        title=job.title,
        company_id=str(job.company_id) if job.company_id else None,
        company_name=job.company.name if job.company else "Unknown",
        company=job.company,
        description=job.description,
        skills_required=job.skills_required or [],
        experience_required=job.experience_required,
        salary_range=job.salary_range or {},
        location=job.location,
        job_type=job.job_type,
        employment_type=job.employment_type,
        source=job.source,
        source_url=job.source_url,
        raw_text=job.raw_text,
        is_active=job.is_active,
        is_verified=job.is_verified,
        view_count=job.view_count,
        application_count=job.application_count,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )
