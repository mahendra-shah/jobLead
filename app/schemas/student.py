"""
Pydantic schemas for Student APIs
Request/Response models
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr, validator


# ==================== Student Management Schemas ====================

class StudentBase(BaseModel):
    """Base student fields"""
    email: EmailStr
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    college_id: Optional[int] = None
    degree: Optional[str] = Field(None, max_length=100)
    branch: Optional[str] = Field(None, max_length=100)
    passing_year: Optional[int] = None
    cgpa: Optional[float] = Field(None, ge=0, le=10)


class StudentCreate(StudentBase):
    """Create student (admin/placement)"""
    password: str = Field(..., min_length=8)
    
    @validator('passing_year')
    def validate_passing_year(cls, v):
        if v and (v < 2020 or v > 2030):
            raise ValueError('Passing year must be between 2020 and 2030')
        return v


class StudentUpdate(BaseModel):
    """Update student (admin/placement)"""
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    college_id: Optional[int] = None
    degree: Optional[str] = Field(None, max_length=100)
    branch: Optional[str] = Field(None, max_length=100)
    passing_year: Optional[int] = None
    cgpa: Optional[float] = Field(None, ge=0, le=10)
    is_active: Optional[bool] = None


class StudentResponse(StudentBase):
    """Student response"""
    id: int
    full_name: str
    resume_url: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    # Computed fields
    profile_completeness: Optional[int] = None  # 0-100
    saved_jobs_count: Optional[int] = None
    
    class Config:
        from_attributes = True


class StudentListResponse(BaseModel):
    """Paginated student list"""
    total: int
    limit: int
    offset: int
    students: List[StudentResponse]


# ==================== Student Self-Service Schemas ====================

class StudentProfileUpdate(BaseModel):
    """Student updates their own profile"""
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    degree: Optional[str] = Field(None, max_length=100)
    branch: Optional[str] = Field(None, max_length=100)
    passing_year: Optional[int] = None
    cgpa: Optional[float] = Field(None, ge=0, le=10)


class StudentPasswordChange(BaseModel):
    """Change password"""
    current_password: str
    new_password: str = Field(..., min_length=8)


class StudentPreferencesUpdate(BaseModel):
    """
    Update job preferences
    
    **Location Preferences** - Supports multiple formats:
    - Specific cities: "Bangalore", "Mumbai", "Pune", "Delhi"
    - Remote work: "Remote", "Work from Home", "WFH"
    - Pan India: "Pan India", "Anywhere in India", "India"
    - International: "International", "Global", "Worldwide"
    - Specific countries: "USA", "UK", "Canada", "Singapore", "Dubai"
    - Multiple: ["Bangalore", "Remote", "USA"]
    
    **Examples**:
    - City-specific: ["Bangalore", "Pune"]
    - Remote only: ["Remote"]
    - Pan India: ["Pan India"]
    - International: ["International"]
    - Flexible: ["Bangalore", "Remote", "Pan India"]
    - Country-specific: ["USA", "Canada", "UK"]
    - Mixed: ["Bangalore", "USA", "Remote"]
    """
    skills: Optional[List[str]] = None
    preferred_locations: Optional[List[str]] = Field(
        None,
        description="Job locations: cities, 'Remote', 'Pan India', 'International', or countries"
    )
    preferred_job_types: Optional[List[str]] = None  # ['full_time', 'internship', 'contract']
    excluded_companies: Optional[List[str]] = None
    min_salary: Optional[int] = None
    max_salary: Optional[int] = None


class StudentPreferencesResponse(BaseModel):
    """Student preferences"""
    skills: List[str]
    preferred_locations: List[str]
    preferred_job_types: List[str]
    excluded_companies: List[str]
    min_salary: Optional[int]
    max_salary: Optional[int]


# ==================== Job Recommendation Schemas ====================

class JobMatchReason(BaseModel):
    """Why a job was recommended"""
    emoji: str
    reason: str


class RecommendedJobResponse(BaseModel):
    """A recommended job with score and reasons"""
    job: Dict[str, Any]  # Full job object
    recommendation_score: float = Field(..., ge=0, le=100)
    match_reasons: List[str]
    missing_skills: List[str]
    is_saved: bool
    view_count: int
    similar_jobs_count: int


class RecommendedJobsResponse(BaseModel):
    """Paginated recommended jobs"""
    total: int
    limit: int
    offset: int
    recommendations: List[RecommendedJobResponse]
    filters_applied: Dict[str, Any]


# ==================== Saved Jobs Schemas ====================

class SavedJobCreate(BaseModel):
    """Save/bookmark a job"""
    job_id: int
    folder: Optional[str] = Field(None, max_length=100)
    notes: Optional[str] = None


class SavedJobUpdate(BaseModel):
    """Update saved job"""
    folder: Optional[str] = Field(None, max_length=100)
    notes: Optional[str] = None


class SavedJobResponse(BaseModel):
    """Saved job response"""
    id: int
    job_id: int
    job: Optional[Dict[str, Any]]  # Full job object
    folder: Optional[str]
    notes: Optional[str]
    saved_at: datetime
    
    class Config:
        from_attributes = True


class SavedJobsResponse(BaseModel):
    """List of saved jobs"""
    total: int
    saved_jobs: List[SavedJobResponse]
    folders: List[str]  # Unique folders


class FolderResponse(BaseModel):
    """Folder with count"""
    name: str
    count: int


# ==================== Job View Schemas ====================

class JobViewCreate(BaseModel):
    """Track job view"""
    job_id: int
    duration_seconds: Optional[int] = None
    source: Optional[str] = Field(None, max_length=50)


# ==================== Notification Schemas ====================

class NotificationResponse(BaseModel):
    """Notification response"""
    id: int
    type: str
    title: str
    message: str
    link: Optional[str]
    job_id: Optional[int]
    data: Optional[Dict[str, Any]]
    read: bool
    read_at: Optional[datetime]
    created_at: datetime
    
    class Config:
        from_attributes = True


class NotificationsResponse(BaseModel):
    """List of notifications"""
    total: int
    unread_count: int
    notifications: List[NotificationResponse]


# ==================== Dashboard Schemas ====================

class StudentDashboardResponse(BaseModel):
    """Student dashboard summary"""
    student: StudentResponse
    stats: Dict[str, Any]
    recent_jobs: List[Dict[str, Any]]
    saved_jobs_count: int
    notifications_unread: int
    profile_completeness: int
    recommendations_available: int


class StudentStatsResponse(BaseModel):
    """Student statistics"""
    total_students: int
    active_students: int
    by_branch: Dict[str, int]
    by_passing_year: Dict[str, int]
    avg_cgpa: float
    total_saved_jobs: int
    total_job_views: int


# ==================== Profile Completeness ====================

class ProfileCompletenessResponse(BaseModel):
    """Profile completeness check"""
    percentage: int = Field(..., ge=0, le=100)
    missing_fields: List[str]
    suggestions: List[str]
    is_complete: bool


# ==================== Bulk Operations ====================

class BulkStudentCreate(BaseModel):
    """Bulk create students"""
    students: List[StudentCreate]


class BulkUploadResponse(BaseModel):
    """Bulk upload result"""
    success: int
    failed: int
    total: int
    errors: List[Dict[str, Any]]


# ==================== Filters & Search ====================

class StudentFilters(BaseModel):
    """Student list filters"""
    college_id: Optional[int] = None
    branch: Optional[str] = None
    passing_year: Optional[int] = None
    cgpa_min: Optional[float] = None
    cgpa_max: Optional[float] = None
    is_active: Optional[bool] = None
    search: Optional[str] = None  # Search in name, email
    sort_by: Optional[str] = Field(default="created_at", pattern="^(name|cgpa|created_at|updated_at)$")
    sort_order: Optional[str] = Field(default="desc", pattern="^(asc|desc)$")
