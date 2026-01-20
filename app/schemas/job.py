"""Job schemas for API responses."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class CompanyBrief(BaseModel):
    """Brief company information."""
    id: str
    name: str
    domain: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    
    class Config:
        from_attributes = True


class JobBase(BaseModel):
    """Base job model with common fields."""
    id: str
    title: str
    company_id: Optional[str] = None
    company_name: str
    description: Optional[str] = None
    skills_required: List[str] = Field(default_factory=list)
    
    # Legacy fields (kept for backward compatibility)
    experience_required: Optional[str] = None
    salary_range: Dict[str, Any] = Field(default_factory=dict)
    
    # New structured fields
    is_fresher: Optional[bool] = None
    work_type: Optional[str] = None  # remote, on-site, hybrid
    experience_min: Optional[int] = None
    experience_max: Optional[int] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    
    location: Optional[str] = None
    job_type: Optional[str] = None
    employment_type: Optional[str] = None
    source: Optional[str] = None
    source_url: Optional[str] = None
    is_active: bool = True
    view_count: int = 0
    application_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class JobListResponse(BaseModel):
    """Response for paginated job list."""
    items: List[JobBase]
    total: int
    page: int
    size: int
    pages: int


class JobDetailResponse(JobBase):
    """Detailed job response with additional fields."""
    company: Optional[CompanyBrief] = None
    raw_text: Optional[str] = None
    is_verified: bool = False
    
    class Config:
        from_attributes = True
