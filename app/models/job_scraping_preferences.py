"""
Job Scraping Preferences Model
Controls what types of jobs to fetch and process
"""
from sqlalchemy import Column, String, Integer, Boolean, ARRAY, DECIMAL, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from uuid import uuid4

from app.db.base import Base


class JobScrapingPreferences(Base):
    __tablename__ = "job_scraping_preferences"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Job Types to Accept
    allowed_job_types = Column(
        ARRAY(String),
        nullable=False,
        default=['full-time', 'internship', 'contract']
    )
    excluded_job_types = Column(
        ARRAY(String),
        default=['part-time']
    )
    
    # Experience Range
    min_experience_years = Column(Integer, default=0)  # Accept freshers
    max_experience_years = Column(Integer, default=5)  # Don't fetch senior roles
    accept_unspecified_experience = Column(Boolean, default=True)
    
    # Education
    allowed_education_levels = Column(
        ARRAY(String),
        default=['B.Tech', 'B.E', 'B.Sc', 'BCA', 'M.Tech', 'MCA', 'Any Graduate']
    )
    
    # Locations
    preferred_locations = Column(
        ARRAY(String),
        default=['Bangalore', 'Mumbai', 'Hyderabad', 'Pune', 'Remote']
    )
    allow_all_india = Column(Boolean, default=True)
    allow_international = Column(Boolean, default=False)
    
    # Work Mode
    allowed_work_modes = Column(
        ARRAY(String),
        default=['remote', 'hybrid', 'office']
    )
    
    # Skills
    priority_skills = Column(
        ARRAY(String),
        default=['Python', 'Java', 'JavaScript', 'React', 'Node.js', 'AWS', 'Data Science']
    )
    excluded_skills = Column(ARRAY(String), default=[])
    
    # Salary Range
    min_salary_lpa = Column(DECIMAL(10, 2), nullable=True)
    max_salary_lpa = Column(DECIMAL(10, 2), nullable=True)
    filter_by_salary = Column(Boolean, default=False)
    
    # Company Filters
    excluded_companies = Column(ARRAY(String), default=[])
    preferred_companies = Column(ARRAY(String), default=[])
    
    # Message Content Filters
    required_keywords = Column(
        ARRAY(String),
        default=['hiring', 'opening', 'position', 'job', 'role', 'opportunity']
    )
    excluded_keywords = Column(
        ARRAY(String),
        default=[
            'looking for job',
            'need a job',
            'searching for',
            'anyone hiring',
            'job alert',
            'urgent requirement for client',
            'bench sales'
        ]
    )
    
    # AI Filtering
    min_ai_confidence_score = Column(Integer, default=70)  # 0-100
    
    # Processing Limits
    max_messages_per_run = Column(Integer, default=50)
    skip_duplicate_threshold_hours = Column(Integer, default=24)
    
    # Active/Inactive
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Metadata
    created_by = Column(UUID(as_uuid=True), nullable=True)
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    notes = Column(Text, nullable=True)

    def __repr__(self):
        return f"<JobScrapingPreferences active={self.is_active}>"
