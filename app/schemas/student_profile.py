"""
Comprehensive Student Profile Schema
Matches the exact requirements from the user
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, EmailStr, validator


# ==================== Nested Object Schemas ====================

class InternshipDetail(BaseModel):
    """Internship details"""
    company_name: str = Field(..., min_length=1, max_length=200, description="Company name")
    duration: str = Field(..., min_length=1, max_length=100, description="Duration (e.g., '3 months', '6 months')")
    role: Optional[str] = Field(None, max_length=200, description="Role/Position")
    description: Optional[str] = Field(None, description="Detailed description")


class ProjectDetail(BaseModel):
    """Project details"""
    title: str = Field(..., min_length=1, max_length=200, description="Project title")
    description: str = Field(..., min_length=1, description="Project description")
    technologies: Optional[List[str]] = Field(None, description="Technologies used")
    github_url: Optional[str] = Field(None, max_length=500, description="GitHub repository URL")
    live_url: Optional[str] = Field(None, max_length=500, description="Live demo URL")


class LanguageProficiency(BaseModel):
    """Language and proficiency level"""
    language: str = Field(..., min_length=1, max_length=100, description="Language name (e.g., English, Hindi)")
    proficiency_level: str = Field(..., description="Proficiency: beginner, proficient, fluent, native")
    
    @validator('proficiency_level')
    def validate_proficiency(cls, v):
        allowed = ['beginner', 'proficient', 'fluent', 'native']
        if v and v.lower() not in allowed:
            raise ValueError(f'Proficiency level must be one of: {", ".join(allowed)}')
        return v.lower() if v else v


class JobPreferences(BaseModel):
    """Job preferences nested object"""
    job_type: Optional[List[str]] = Field(
        None,
        description="Job types: ['Internship', 'Full-Time', 'Part-Time']"
    )
    work_mode: Optional[List[str]] = Field(
        None,
        description="Work modes: ['Remote', 'Hybrid', 'Office']"
    )
    preferred_job_role: Optional[List[str]] = Field(
        None,
        description="Preferred job roles (e.g., ['Software Developer', 'Data Analyst'])"
    )
    preferred_location: Optional[List[str]] = Field(
        None,
        description="Preferred locations (e.g., ['Bangalore', 'Mumbai', 'Remote'])"
    )
    expected_salary: Optional[int] = Field(None, ge=0, description="Expected salary in INR (optional)")


# ==================== Student Profile Schemas ====================

class StudentProfileUpdate(BaseModel):
    """
    Student Profile Update Schema
    All fields are optional for partial updates
    """
    # Personal Details
    first_name: Optional[str] = Field(None, min_length=1, max_length=100, description="First name")
    last_name: Optional[str] = Field(None, min_length=1, max_length=100, description="Last name")
    phone: Optional[str] = Field(None, max_length=20, description="Mobile number (e.g., +91-9876543210)")
    date_of_birth: Optional[str] = Field(None, description="Date of birth (YYYY-MM-DD format as string)")
    gender: Optional[str] = Field(None, max_length=50, description="Gender (e.g., Male, Female, Other)")
    current_address: Optional[str] = Field(None, max_length=500, description="Current address")
    
    # Education Details
    highest_qualification: Optional[str] = Field(
        None, 
        max_length=100,
        description="Highest qualification: 10th, 12th, Diploma, Graduation, Post-Graduation"
    )
    college_name: Optional[str] = Field(None, max_length=200, description="College/University name")
    college_id: Optional[int] = Field(None, description="College ID")
    course: Optional[str] = Field(None, max_length=100, description="Course (e.g., B.Tech, B.Sc, MCA)")
    branch: Optional[str] = Field(None, max_length=100, description="Branch/Stream (e.g., Computer Science)")
    passing_year: Optional[int] = Field(None, ge=2000, le=2030, description="Year of passing")
    percentage: Optional[float] = Field(None, ge=0, le=100, description="Percentage (0-100)")
    cgpa: Optional[float] = Field(None, ge=0, le=10, description="CGPA (0-10 scale)")
    
    # Skills
    technical_skills: Optional[List[str]] = Field(
        None,
        description="Technical skills array (e.g., ['Java', 'Python', 'React', 'MS Excel'])"
    )
    soft_skills: Optional[List[str]] = Field(
        None,
        description="Soft skills array (e.g., ['Communication', 'Teamwork', 'Time Management'])"
    )
    skill_required: Optional[List[str]] = Field(
        None,
        description="Required skills for job matching (e.g., ['Python', 'FastAPI', 'PostgreSQL'])"
    )
    
    # Experience
    experience_type: Optional[str] = Field(
        None,
        description="Experience type: 'Fresher' or 'Experienced'"
    )
    internship_details: Optional[List[InternshipDetail]] = Field(
        None,
        description="Array of internship details (Company Name, Duration, Role, Description)"
    )
    projects: Optional[List[ProjectDetail]] = Field(
        None,
        description="Array of project details (Title, Description, Technologies, GitHub URL, Live URL)"
    )
    
    # Languages & Communication
    languages: Optional[List[LanguageProficiency]] = Field(
        None,
        description="Array of languages with proficiency levels"
    )
    
    # Job Preferences (flat fields like technical_skills)
    job_type: Optional[List[str]] = Field(
        None,
        description="Job types: ['Internship', 'Full-Time', 'Part-Time']"
    )
    work_mode: Optional[List[str]] = Field(
        None,
        description="Work modes: ['Remote', 'Hybrid', 'Office']"
    )
    preferred_job_role: Optional[List[str]] = Field(
        None,
        description="Preferred job roles (e.g., ['Software Developer', 'Data Analyst'])"
    )
    preferred_location: Optional[List[str]] = Field(
        None,
        description="Preferred locations (e.g., ['Bangalore', 'Mumbai', 'Remote'])"
    )
    expected_salary: Optional[int] = Field(None, ge=0, description="Expected salary in INR (optional)")
    
    # Job Preferences (nested object - for backward compatibility)
    preference: Optional[JobPreferences] = Field(
        None,
        description="Job preferences (job_type, work_mode, preferred_job_role, preferred_location, expected_salary)"
    )
    
    # Technical Profile Links
    github_profile: Optional[str] = Field(None, max_length=500, description="GitHub profile URL")
    linkedin_profile: Optional[str] = Field(None, max_length=500, description="LinkedIn profile URL")
    portfolio_url: Optional[str] = Field(None, max_length=500, description="Portfolio/Personal website URL")
    coding_platforms: Optional[Dict[str, str]] = Field(
        None,
        description="Coding platforms (e.g., {'LeetCode': 'username', 'HackerRank': 'username'})"
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
    """Complete student profile response"""
    # Personal Details
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    date_of_birth: Optional[str] = None  # Return as string (YYYY-MM-DD) for JSON compatibility
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
    skill_required: Optional[List[str]] = None
    
    # Experience
    experience_type: Optional[str] = None
    internship_details: Optional[List[Dict[str, Any]]] = None
    projects: Optional[List[Dict[str, Any]]] = None
    
    # Languages
    languages: Optional[List[Dict[str, str]]] = None
    
    # Job Preferences (flat fields like technical_skills)
    job_type: Optional[List[str]] = None
    work_mode: Optional[List[str]] = None
    preferred_job_role: Optional[List[str]] = None
    preferred_location: Optional[List[str]] = None
    expected_salary: Optional[int] = None
    
    # Job Preferences (nested object - for backward compatibility)
    preference: Optional[Dict[str, Any]] = None
    
    # Technical Profile Links
    github_profile: Optional[str] = None
    linkedin_profile: Optional[str] = None
    portfolio_url: Optional[str] = None
    coding_platforms: Optional[Dict[str, str]] = None
    
    # Resume
    resume_url: Optional[str] = None
    
    # Metadata
    id: Optional[str] = None
    is_active: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    profile_completeness: Optional[int] = None
    
    class Config:
        from_attributes = True

