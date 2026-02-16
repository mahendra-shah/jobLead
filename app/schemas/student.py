"""
Pydantic schemas for Student APIs
Request/Response models - Normalized and organized
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, EmailStr, validator


# ==================== Shared Field Definitions ====================

class PersonalDetailsMixin(BaseModel):
    """Shared personal details fields"""
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(None, max_length=50)
    current_address: Optional[str] = Field(None, max_length=500)


class EducationDetailsMixin(BaseModel):
    """Shared education details fields"""
    highest_qualification: Optional[str] = Field(None, max_length=100)
    college_name: Optional[str] = Field(None, max_length=200)
    college_id: Optional[int] = None
    course: Optional[str] = Field(None, max_length=100)
    branch: Optional[str] = Field(None, max_length=100)
    degree: Optional[str] = Field(None, max_length=100)  # For backward compatibility
    passing_year: Optional[int] = Field(None, ge=2000, le=2030)
    percentage: Optional[float] = Field(None, ge=0, le=100)
    cgpa: Optional[float] = Field(None, ge=0, le=10)


# ==================== Nested Object Schemas ====================

class LanguageProficiency(BaseModel):
    """Language and proficiency level"""
    language: str = Field(..., min_length=1, max_length=100)
    proficiency_level: str = Field(..., description="beginner, proficient, fluent, native")
    
    @validator('proficiency_level')
    def validate_proficiency(cls, v):
        allowed = ['beginner', 'proficient', 'fluent', 'native']
        if v and v.lower() not in allowed:
            raise ValueError(f'Proficiency level must be one of: {", ".join(allowed)}')
        return v.lower() if v else v


class InternshipDetail(BaseModel):
    """Internship details"""
    company_name: str = Field(..., min_length=1, max_length=200)
    duration: str = Field(..., min_length=1, max_length=100)
    role: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None


class ProjectDetail(BaseModel):
    """Project details"""
    title: str = Field(..., min_length=1, max_length=200)
    description: str = Field(..., min_length=1)
    technologies: Optional[List[str]] = None
    github_url: Optional[str] = None
    live_url: Optional[str] = None


# ==================== Student Management Schemas (Admin/Placement) ====================

class StudentBase(BaseModel):
    """Base student fields for admin operations"""
    email: EmailStr
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    college_id: Optional[int] = None
    degree: Optional[str] = Field(None, max_length=100)
    branch: Optional[str] = Field(None, max_length=100)
    passing_year: Optional[int] = Field(None, ge=2000, le=2030)
    cgpa: Optional[float] = Field(None, ge=0, le=10)
    
    @validator('passing_year')
    def validate_passing_year(cls, v):
        if v and (v < 2000 or v > 2030):
            raise ValueError('Passing year must be between 2000 and 2030')
        return v


class StudentCreate(StudentBase):
    """Create student (admin/placement)"""
    password: str = Field(..., min_length=8)


class StudentUpdate(PersonalDetailsMixin, EducationDetailsMixin):
    """Update student (admin/placement)"""
    is_active: Optional[bool] = None


class StudentResponse(StudentBase):
    """Student response for admin/placement operations"""
    id: int
    full_name: str
    resume_url: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    profile_completeness: Optional[int] = Field(None, ge=0, le=100)
    saved_jobs_count: Optional[int] = None
    
    class Config:
        from_attributes = True


class StudentListResponse(BaseModel):
    """Paginated student list"""
    total: int
    limit: int
    offset: int
    students: List[StudentResponse]


# ==================== Student Self-Service Profile Schemas ====================

class StudentProfileCreate(PersonalDetailsMixin, EducationDetailsMixin):
    """
    Comprehensive student profile creation/update schema
    
    All fields are optional to allow partial updates.
    """
    email: Optional[EmailStr] = None
    
    # Skills
    technical_skills: Optional[List[str]] = Field(
        None, 
        description="Technical Skills (e.g., Java, Python, React, MS Excel)"
    )
    soft_skills: Optional[List[str]] = Field(
        None, 
        description="Soft Skills (e.g., Communication, Teamwork, Time Management)"
    )
    
    # Experience
    experience_type: Optional[str] = Field(None, description="Fresher or Experienced")
    internship_details: Optional[List[InternshipDetail]] = None
    projects: Optional[List[ProjectDetail]] = None
    
    # Languages
    languages: Optional[List[LanguageProficiency]] = None
    
    # Job Preferences
    job_type: Optional[List[str]] = Field(None, description="Internship, Full-Time, Part-Time")
    work_mode: Optional[List[str]] = Field(None, description="Remote, Hybrid, Office")
    preferred_job_role: Optional[List[str]] = None
    preferred_location: Optional[List[str]] = None
    expected_salary: Optional[int] = Field(None, ge=0)
    
    # Technical Profile Links
    github_profile: Optional[str] = Field(None, max_length=500)
    linkedin_profile: Optional[str] = Field(None, max_length=500)
    portfolio_url: Optional[str] = Field(None, max_length=500)
    coding_platforms: Optional[Dict[str, str]] = None
    
    @validator('experience_type')
    def validate_experience_type(cls, v):
        if v and v.lower() not in ['fresher', 'experienced']:
            raise ValueError('Experience type must be either "Fresher" or "Experienced"')
        return v.lower() if v else v
    
    @validator('highest_qualification')
    def validate_qualification(cls, v):
        if v:
            allowed = ['10th', '12th', 'diploma', 'graduation', 'post-graduation', 'phd']
            if v.lower() not in allowed:
                raise ValueError(f'Qualification must be one of: {", ".join(allowed)}')
        return v.lower() if v else v


class StudentProfileResponse(BaseModel):
    """Comprehensive student profile response with all fields"""
    # Personal Details
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = None
    current_address: Optional[str] = None
    
    # Education Details
    highest_qualification: Optional[str] = None
    college_name: Optional[str] = None
    college_id: Optional[int] = None
    course: Optional[str] = None
    branch: Optional[str] = None
    passing_year: Optional[int] = None
    percentage: Optional[float] = None
    cgpa: Optional[float] = None
    
    # Skills
    technical_skills: Optional[List[str]] = None
    soft_skills: Optional[List[str]] = None
    
    # Experience
    experience_type: Optional[str] = None
    internship_details: Optional[List[Dict[str, Any]]] = None
    projects: Optional[List[Dict[str, Any]]] = None
    
    # Languages
    languages: Optional[List[Dict[str, str]]] = None
    
    # Job Preferences
    job_type: Optional[List[str]] = None
    work_mode: Optional[List[str]] = None
    preferred_job_role: Optional[List[str]] = None
    preferred_location: Optional[List[str]] = None
    expected_salary: Optional[int] = None
    
    # Technical Profile Links
    github_profile: Optional[str] = None
    linkedin_profile: Optional[str] = None
    portfolio_url: Optional[str] = None
    coding_platforms: Optional[Dict[str, str]] = None
    
    # Resume
    resume_url: Optional[str] = None
    
    # Metadata
    id: Optional[int] = None
    is_active: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    profile_completeness: Optional[int] = None
    
    class Config:
        from_attributes = True


# Legacy schema - kept for backward compatibility
class StudentProfileUpdate(BaseModel):
    """Student updates their own profile (legacy - use StudentProfileCreate)"""
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, min_length=1, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    degree: Optional[str] = Field(None, max_length=100)
    branch: Optional[str] = Field(None, max_length=100)
    passing_year: Optional[int] = None
    cgpa: Optional[float] = Field(None, ge=0, le=10)


# ==================== Password Management ====================

class StudentPasswordChange(BaseModel):
    """Change password"""
    current_password: str
    new_password: str = Field(..., min_length=8)


# ==================== Job Preferences Schemas ====================

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
    """
    skills: Optional[List[str]] = None
    preferred_locations: Optional[List[str]] = Field(
        None,
        description="Job locations: cities, 'Remote', 'Pan India', 'International', or countries"
    )
    preferred_job_types: Optional[List[str]] = None
    excluded_companies: Optional[List[str]] = None
    min_salary: Optional[int] = Field(None, ge=0)
    max_salary: Optional[int] = Field(None, ge=0)


class StudentPreferencesResponse(BaseModel):
    """Student preferences response"""
    skills: List[str]
    preferred_locations: List[str]
    preferred_job_types: List[str]
    excluded_companies: List[str]
    min_salary: Optional[int] = None
    max_salary: Optional[int] = None


# ==================== Job Recommendation Schemas ====================

class JobMatchReason(BaseModel):
    """Why a job was recommended"""
    emoji: str
    reason: str


class RecommendedJobResponse(BaseModel):
    """A recommended job with score and reasons"""
    job: Dict[str, Any]
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
    job: Optional[Dict[str, Any]] = None
    folder: Optional[str] = None
    notes: Optional[str] = None
    saved_at: datetime
    
    class Config:
        from_attributes = True


class SavedJobsResponse(BaseModel):
    """List of saved jobs"""
    total: int
    saved_jobs: List[SavedJobResponse]
    folders: List[str]


class FolderResponse(BaseModel):
    """Folder with count"""
    name: str
    count: int


# ==================== Job View Schemas ====================

class JobViewCreate(BaseModel):
    """Track job view"""
    job_id: int
    duration_seconds: Optional[int] = Field(None, ge=0)
    source: Optional[str] = Field(None, max_length=50)


# ==================== Notification Schemas ====================

class NotificationResponse(BaseModel):
    """Notification response"""
    id: int
    type: str
    title: str
    message: str
    link: Optional[str] = None
    job_id: Optional[int] = None
    data: Optional[Dict[str, Any]] = None
    read: bool
    read_at: Optional[datetime] = None
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
    cgpa_min: Optional[float] = Field(None, ge=0)
    cgpa_max: Optional[float] = Field(None, ge=0)
    is_active: Optional[bool] = None
    search: Optional[str] = None
    sort_by: Optional[str] = Field(default="created_at", pattern="^(name|cgpa|created_at|updated_at)$")
    sort_order: Optional[str] = Field(default="desc", pattern="^(asc|desc)$")
