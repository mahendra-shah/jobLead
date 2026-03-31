"""Job schemas for API responses."""

from datetime import datetime
from typing import Optional, List, Any
from uuid import UUID
from pydantic import BaseModel, Field, model_validator


class CompanyBrief(BaseModel):
    """Brief company information."""
    id: str
    name: str
    domain: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    
    @model_validator(mode='before')
    @classmethod
    def convert_uuid_fields(cls, data: Any) -> Any:
        """Convert UUID objects to strings before validation.
        
        This handles both SQLAlchemy ORM objects (from from_attributes=True)
        and dict inputs, ensuring UUIDs are always converted to strings.
        """
        if hasattr(data, 'id'):  # SQLAlchemy object
            return {
                'id': str(data.id) if data.id else None,
                'name': data.name,
                'domain': getattr(data, 'domain', None),
                'logo_url': getattr(data, 'logo_url', None),
                'website': getattr(data, 'website', None),
            }
        elif isinstance(data, dict):
            if 'id' in data and isinstance(data['id'], UUID):
                data = data.copy()
                data['id'] = str(data['id'])
        return data
    
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
    
    @model_validator(mode='before')
    @classmethod
    def convert_uuid_fields(cls, data: Any) -> Any:
        """Convert UUID objects to strings before validation.
        
        Handles SQLAlchemy ORM objects and dicts, converting id and company_id
        UUID fields to strings for proper Pydantic validation.
        """
        if hasattr(data, 'id'):  # SQLAlchemy Job object
            # Convert ORM object to dict with UUID fields as strings
            result = {
                'id': str(data.id) if data.id else None,
                'title': data.title,
                'company_id': str(data.company_id) if data.company_id else None,
                'company_name': getattr(data, 'company_name', 'Unknown'),
                'description': getattr(data, 'description', None),
                'skills_required': getattr(data, 'skills_required', []) or [],
                'experience': getattr(data, 'experience', None),
                'salary': getattr(data, 'salary', None),
                'is_fresher': getattr(data, 'is_fresher', None),
                'work_type': getattr(data, 'work_type', None),
                'location': getattr(data, 'location', None),
                'job_type': getattr(data, 'job_type', None),
                'employment_type': getattr(data, 'employment_type', None),
                'source': getattr(data, 'source', None),
                'source_url': getattr(data, 'source_url', None),
                'is_active': getattr(data, 'is_active', True),
                'shared_count': getattr(data, 'shared_count', 0),
                'created_at': getattr(data, 'created_at', None),
                'updated_at': getattr(data, 'updated_at', None),
            }
            return result
        elif isinstance(data, dict):
            # Handle dict input - convert UUID fields if present
            data = data.copy()
            if 'id' in data and isinstance(data['id'], UUID):
                data['id'] = str(data['id'])
            if 'company_id' in data and isinstance(data['company_id'], UUID):
                data['company_id'] = str(data['company_id'])
        return data
    
    experience: Optional[str] = None
    salary: Optional[str] = None
    is_fresher: Optional[bool] = None
    work_type: Optional[str] = None  # remote, on-site, hybrid
    
    location: Optional[str] = None
    job_type: Optional[str] = None
    employment_type: Optional[str] = None
    source: Optional[str] = None
    source_url: Optional[str] = None
    is_active: bool = True
    shared_count: int = 0
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
