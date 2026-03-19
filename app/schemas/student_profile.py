"""
Comprehensive Student Profile Schema
Matches the exact requirements from the user
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, EmailStr, validator


# ==================== Nested Object Schemas ====================

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
    job_category: Optional[str] = Field(
        None,
        description="Preferred job category (e.g., IT, Data, Core)"
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
    phone: Optional[str] = Field(None, max_length=20, description="Mobile number (e.g., +91-9876543210)")
    date_of_birth: Optional[str] = Field(None, description="Date of birth (YYYY-MM-DD format as string)")
    gender: Optional[str] = Field(None, max_length=50, description="Gender (e.g., Male, Female, Other)")
    
    # Education Details
    highest_qualification: Optional[str] = Field(
        None, 
        max_length=100,
        description="Highest qualification: 10th, 12th, Diploma, Graduation, Post-Graduation"
    )
    course: Optional[str] = Field(None, max_length=100, description="Course (e.g., B.Tech, B.Sc, MCA)")
    passing_year: Optional[int] = Field(None, ge=2000, le=2030, description="Year of passing")
    
    # Skills
    skills: Optional[List[str]] = Field(
        None,
        description="General skills array"
    )
    technical_skills: Optional[List[str]] = Field(
        None,
        description="Technical skills array (e.g., ['Java', 'Python', 'React', 'MS Excel'])"
    )
    soft_skills: Optional[List[str]] = Field(
        None,
        description="Soft skills array (e.g., ['Communication', 'Teamwork', 'Time Management'])"
    )
    
    # Experience
    experience_type: Optional[str] = Field(
        None,
        description="Experience type: 'Fresher' or 'Experienced'"
    )
    
    # Languages & Communication
    spoken_languages: Optional[List[LanguageProficiency]] = Field(
        None,
        description="Array of languages with proficiency levels"
    )
    email: Optional[EmailStr] = Field(None, description="Primary email")
    
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
    job_category: Optional[str] = Field(
        None,
        description="Preferred job category (e.g., IT, Data, Core)"
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
    tech_links: Optional[Dict[str, Any]] = Field(
        None,
        description="Tech links object: {github_profile, linkedin_profile, portfolio_url, coding_platforms}"
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
    full_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    date_of_birth: Optional[str] = None  # Return as string (YYYY-MM-DD) for JSON compatibility
    gender: Optional[str] = None
    
    # Education Details
    highest_qualification: Optional[str] = None
    course: Optional[str] = None
    passing_year: Optional[int] = None
    
    # Skills
    skills: Optional[List[str]] = None
    technical_skills: Optional[List[str]] = None
    soft_skills: Optional[List[str]] = None
    
    # Experience
    experience_type: Optional[str] = None
    
    # Languages
    spoken_languages: Optional[List[Dict[str, str]]] = None
    email: Optional[EmailStr] = None
    
    # Job Preferences (consolidated into single JSONB object)
    preference: Optional[JobPreferences] = Field(
        None,
        description="Job preferences containing job_type, work_mode, preferred_job_role, preferred_location, expected_salary"
    )
    preferred_job_role: Optional[List[str]] = None
    job_category: Optional[str] = None
    tech_links: Optional[Dict[str, Any]] = None
    
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

