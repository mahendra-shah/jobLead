"""
Pydantic schemas for Student APIs
Request/Response models - Normalized and organized
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, EmailStr, validator


# ==================== Shared Field Definitions ====================

class PersonalDetailsMixin(BaseModel):
    """Shared personal details fields - Real-world student information"""
    first_name: Optional[str] = Field(
        None, 
        min_length=1, 
        max_length=100,
        description="Student's first/given name"
    )
    last_name: Optional[str] = Field(
        None, 
        min_length=1, 
        max_length=100,
        description="Student's last/family name"
    )
    phone: Optional[str] = Field(
        None, 
        max_length=20,
        description="Contact phone number (e.g., +91-9876543210)"
    )
    date_of_birth: Optional[date] = Field(
        None,
        description="Date of birth (YYYY-MM-DD format)"
    )
    gender: Optional[str] = Field(
        None, 
        max_length=50,
        description="Gender identity (e.g., Male, Female, Other, Prefer not to say)"
    )
    current_address: Optional[str] = Field(
        None, 
        max_length=500,
        description="Current residential address"
    )


class EducationDetailsMixin(BaseModel):
    """Shared education details fields - Real-world academic information"""
    highest_qualification: Optional[str] = Field(
        None, 
        max_length=100,
        description="Highest educational qualification (e.g., Bachelor's, Master's, Diploma)"
    )
    college_name: Optional[str] = Field(
        None, 
        max_length=200,
        description="Name of the college/university (e.g., IIT Delhi, MIT, Stanford)"
    )
    college_id: Optional[int] = Field(
        None,
        description="Internal college ID for reference"
    )
    course: Optional[str] = Field(
        None, 
        max_length=100,
        description="Course name (e.g., B.Tech, B.Sc, MCA, MBA)"
    )
    branch: Optional[str] = Field(
        None, 
        max_length=100,
        description="Branch/Stream/Department (e.g., Computer Science, Mechanical, Electronics)"
    )
    degree: Optional[str] = Field(
        None, 
        max_length=100,
        description="Degree type (e.g., Bachelor's, Master's) - Legacy field"
    )
    passing_year: Optional[int] = Field(
        None, 
        ge=2000, 
        le=2030,
        description="Expected or actual year of graduation (e.g., 2024, 2025)"
    )
    percentage: Optional[float] = Field(
        None, 
        ge=0, 
        le=100,
        description="Overall percentage (0-100, e.g., 85.5)"
    )
    cgpa: Optional[float] = Field(
        None, 
        ge=0, 
        le=10,
        description="CGPA on 10-point scale (e.g., 8.5, 9.2) or GPA on 4-point scale converted"
    )


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
    
    **Real-world usage**: This schema represents a complete student profile as used in
    placement portals, job applications, and career services.
    
    All fields are optional to allow partial updates. Use this for both creating
    and updating profiles.
    
    **Example**:
    ```json
    {
      "first_name": "Raj",
      "last_name": "Kumar",
      "phone": "+91-9876543210",
      "branch": "Computer Science",
      "passing_year": 2025,
      "cgpa": 8.5,
      "technical_skills": ["Python", "React", "Node.js"],
      "preferred_location": ["Bangalore", "Remote"]
    }
    ```
    """
    email: Optional[EmailStr] = Field(
        None,
        description="Email address (usually read-only, managed by auth system)"
    )
    
    # Skills - Industry-standard categorization
    technical_skills: Optional[List[str]] = Field(
        None, 
        description="Technical/Programming skills (e.g., Python, Java, React, SQL, AWS, Docker)"
    )
    soft_skills: Optional[List[str]] = Field(
        None, 
        description="Soft/Interpersonal skills (e.g., Communication, Leadership, Problem Solving)"
    )
    
    # Experience - Real-world career progression
    experience_type: Optional[str] = Field(
        None, 
        description="Experience level: 'fresher' (no work experience) or 'experienced' (has work experience)"
    )
    internship_details: Optional[List[InternshipDetail]] = Field(
        None,
        description="List of internships completed (company, duration, role, description)"
    )
    projects: Optional[List[ProjectDetail]] = Field(
        None,
        description="Academic or personal projects (title, description, technologies, GitHub links)"
    )
    
    # Languages - Communication skills
    languages: Optional[List[LanguageProficiency]] = Field(
        None,
        description="Languages known with proficiency levels (e.g., English: Fluent, Hindi: Native)"
    )
    
    # Job Preferences - Career goals and requirements
    job_type: Optional[List[str]] = Field(
        None, 
        description="Types of jobs interested in: 'Full-Time', 'Internship', 'Part-Time', 'Contract'"
    )
    work_mode: Optional[List[str]] = Field(
        None, 
        description="Preferred work arrangements: 'Remote', 'Hybrid', 'Office', 'On-site'"
    )
    preferred_job_role: Optional[List[str]] = Field(
        None,
        description="Desired job roles/titles (e.g., 'Software Developer', 'Data Analyst', 'Product Manager')"
    )
    preferred_location: Optional[List[str]] = Field(
        None,
        description="Preferred job locations (e.g., 'Bangalore', 'Mumbai', 'Remote', 'Pan India')"
    )
    expected_salary: Optional[int] = Field(
        None, 
        ge=0,
        description="Expected annual salary in INR (e.g., 600000 for 6 LPA)"
    )
    
    # Technical Profile Links - Online presence
    github_profile: Optional[str] = Field(
        None, 
        max_length=500,
        description="GitHub profile URL (e.g., https://github.com/username)"
    )
    linkedin_profile: Optional[str] = Field(
        None, 
        max_length=500,
        description="LinkedIn profile URL (e.g., https://linkedin.com/in/username)"
    )
    portfolio_url: Optional[str] = Field(
        None, 
        max_length=500,
        description="Personal portfolio website URL (e.g., https://yourname.dev)"
    )
    coding_platforms: Optional[Dict[str, str]] = Field(
        None,
        description="Coding platform profiles (e.g., {'LeetCode': 'username', 'HackerRank': 'username', 'CodeChef': 'username'})"
    )
    
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
    # NOTE: This dashboard payload is consumed by the frontend as a flexible
    # "StudentProfile"-like object (many optional fields, UUID ids as strings).
    # Using Dict keeps the contract stable even as the profile schema evolves.
    student: Dict[str, Any]
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
