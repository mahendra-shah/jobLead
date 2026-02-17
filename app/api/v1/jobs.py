"""Job endpoints - Browse and search jobs."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_, and_, desc, case
from sqlalchemy.orm import joinedload

from app.api.deps import get_db
from app.models.job import Job
from app.models.company import Company
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
    experience: Optional[str] = Query(None, description="Filter by experience (e.g., '0-2', '2-5') [LEGACY]"),
    
    # New filter parameters
    is_fresher: Optional[bool] = Query(None, description="Filter for fresher jobs (true/false)"),
    work_type: Optional[str] = Query(None, description="Filter by work type (remote, on-site, hybrid)"),
    min_experience: Optional[int] = Query(None, ge=0, description="Minimum years of experience"),
    max_experience: Optional[int] = Query(None, ge=0, description="Maximum years of experience"),
    min_salary: Optional[float] = Query(None, ge=0, description="Minimum salary"),
    max_salary: Optional[float] = Query(None, ge=0, description="Maximum salary"),
    
    company_id: Optional[str] = Query(None, description="Filter by company ID"),
    is_active: bool = Query(True, description="Show only active jobs"),
    sort_by: str = Query("created_at", description="Sort by field (created_at, title, location)"),
    sort_order: str = Query("desc", description="Sort order (asc, desc)"),
    include_total: bool = Query(False, description="Include total count (slower for large datasets)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get paginated list of jobs with filters - OPTIMIZED FOR SPEED
    
    **Performance Notes:**
    - Set `include_total=false` (default) for fastest response
    - Total count only computed when explicitly requested
    - Indexes optimized for common filter combinations
    
    **Filters:**
    - `location`: City name or 'Remote' (case-insensitive, partial match)
    - `skills`: Comma-separated skills (matches any)
    - `job_type`: remote, office, hybrid
    - `employment_type`: fulltime, parttime, contract, internship
    - `experience`: Experience range (e.g., '0-2 years', '2-5 years') [LEGACY]
    - `is_fresher`: Filter for fresher jobs (true/false)
    - `work_type`: remote, on-site, hybrid
    - `min_experience`: Minimum years of experience required
    - `max_experience`: Maximum years of experience allowed
    - `min_salary`: Minimum salary in INR
    - `max_salary`: Maximum salary in INR
    - `company_id`: Filter by specific company
    - `is_active`: Show only active jobs (default: true)
    
    **Sorting:**
    - `sort_by`: created_at, title, location, view_count
    - `sort_order`: asc, desc (default: desc)
    
    **Pagination:**
    - `page`: Page number (starts at 1)
    - `size`: Items per page (max: 100)
    - `include_total`: Include total count (default: false for speed)
    
    **Examples:**
    ```
    # Fast query without total count
    GET /api/v1/jobs/?page=1&size=20
    
    # Get fresher jobs with total
    GET /api/v1/jobs/?is_fresher=true&page=1&size=20&include_total=true
    
    # Get remote jobs with 2-5 years experience
    GET /api/v1/jobs/?work_type=remote&min_experience=2&max_experience=5
    
    # Get jobs with salary range 5L-10L
    GET /api/v1/jobs/?min_salary=500000&max_salary=1000000
    
    # Combined filters
    GET /api/v1/jobs/?location=Pune&skills=Python,Django&is_fresher=true&work_type=remote
    ```
    """
    # Build filters list
    filters = []
    
    if is_active:
        filters.append(Job.is_active.is_(True))
    
    if location:
        # Case-insensitive partial match (uses GIN index with pg_trgm)
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
    
    # New structured field filters
    if is_fresher is not None:
        filters.append(Job.is_fresher.is_(is_fresher))
    
    if work_type:
        filters.append(Job.work_type.ilike(f"%{work_type}%"))
    
    if min_experience is not None:
        # Show jobs where max_experience >= min_experience (or no experience requirement)
        filters.append(
            or_(
                Job.experience_max >= min_experience,
                Job.experience_max.is_(None)
            )
        )
    
    if max_experience is not None:
        # Show jobs where min_experience <= max_experience (or is fresher)
        filters.append(
            or_(
                Job.experience_min <= max_experience,
                Job.experience_min.is_(None),
                Job.is_fresher.is_(True)
            )
        )
    
    if min_salary is not None:
        # Show jobs where salary_max >= min_salary
        filters.append(
            and_(
                Job.salary_max.isnot(None),
                Job.salary_max >= min_salary
            )
        )
    
    if max_salary is not None:
        # Show jobs where salary_min <= max_salary
        filters.append(
            and_(
                Job.salary_min.isnot(None),
                Job.salary_min <= max_salary
            )
        )
    
    if company_id:
        filters.append(Job.company_id == company_id)
    
    # Combine filters
    where_clause = and_(*filters) if filters else True
    
    # Determine sort column and order
    sort_column = getattr(Job, sort_by, Job.created_at)
    order_clause = desc(sort_column) if sort_order.lower() == "desc" else sort_column
    
    # Build optimized query - select specific columns + company name
    # Use window function to get total count in same query (if requested)
    if include_total:
        # Single query with COUNT(*) OVER() window function
        query = (
            select(
                Job.id,
                Job.title,
                Job.company_id,
                Company.name.label('company_name'),
                Job.description,
                Job.skills_required,
                Job.experience_required,
                Job.salary_range,
                Job.is_fresher,
                Job.work_type,
                Job.experience_min,
                Job.experience_max,
                Job.salary_min,
                Job.salary_max,
                Job.location,
                Job.job_type,
                Job.employment_type,
                Job.source,
                Job.source_url,
                Job.is_active,
                Job.view_count,
                Job.application_count,
                Job.created_at,
                Job.updated_at,
                func.count().over().label('total_count')  # Window function for total
            )
            .outerjoin(Company, Job.company_id == Company.id)
            .where(where_clause)
            .order_by(order_clause)
            .offset((page - 1) * size)
            .limit(size)
        )
        
        # Execute query
        result = await db.execute(query)
        rows = result.all()
        
        # Get total from first row (same for all rows due to window function)
        total = rows[0].total_count if rows else 0
    else:
        # Fast query without count - just fetch data
        query = (
            select(
                Job.id,
                Job.title,
                Job.company_id,
                Company.name.label('company_name'),
                Job.description,
                Job.skills_required,
                Job.experience_required,
                Job.salary_range,
                Job.is_fresher,
                Job.work_type,
                Job.experience_min,
                Job.experience_max,
                Job.salary_min,
                Job.salary_max,
                Job.location,
                Job.job_type,
                Job.employment_type,
                Job.source,
                Job.source_url,
                Job.is_active,
                Job.view_count,
                Job.application_count,
                Job.created_at,
                Job.updated_at
            )
            .outerjoin(Company, Job.company_id == Company.id)
            .where(where_clause)
            .order_by(order_clause)
            .offset((page - 1) * size)
            .limit(size)
        )
        
        # Execute query
        result = await db.execute(query)
        rows = result.all()
        
        # No total count in fast mode
        total = None
    
    # Format response (fast list comprehension)
    items = [
        {
            "id": str(row.id),
            "title": row.title,
            "company_id": str(row.company_id) if row.company_id else None,
            "company_name": row.company_name or "Unknown",
            "description": row.description,
            "skills_required": row.skills_required or [],
            "experience_required": row.experience_required,
            "salary_range": row.salary_range or {},
            "is_fresher": row.is_fresher,
            "work_type": row.work_type,
            "experience_min": row.experience_min,
            "experience_max": row.experience_max,
            "salary_min": float(row.salary_min) if row.salary_min else None,
            "salary_max": float(row.salary_max) if row.salary_max else None,
            "location": row.location,
            "job_type": row.job_type,
            "employment_type": row.employment_type,
            "source": row.source,
            "source_url": row.source_url,
            "is_active": row.is_active,
            "view_count": row.view_count,
            "application_count": row.application_count,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]
    
    return JobListResponse(
        items=items,
        total=total if include_total else len(items),  # Return items length in fast mode
        page=page,
        size=size,
        pages=(total + size - 1) // size if (total and total > 0) else 1
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
        
        # Legacy fields
        experience_required=job.experience_required,
        salary_range=job.salary_range or {},
        
        # New structured fields
        is_fresher=job.is_fresher,
        work_type=job.work_type,
        experience_min=job.experience_min,
        experience_max=job.experience_max,
        salary_min=float(job.salary_min) if job.salary_min else None,
        salary_max=float(job.salary_max) if job.salary_max else None,
        
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
