"""Student model."""

from sqlalchemy import Boolean, Column, Integer, String, Date
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.db.base import Base


class Student(Base):
    """Student profile model."""

    __tablename__ = "students"

    full_name = Column(String(255), nullable=False)
    phone = Column(String(20))
    resume_url = Column(String(500))  # S3 URL for resume
    
    # Personal Details
    date_of_birth = Column(Date)
    gender = Column(String(50))
    
    # Education Details
    highest_qualification = Column(String(100))
    course = Column(String(100))
    passing_year = Column(Integer)
    
    # Skills
    skills = Column(JSONB, default=list)
    technical_skills = Column(JSONB, default=list)  # ["Python", "React", ...]
    soft_skills = Column(JSONB, default=list)  # ["Communication", "Teamwork", ...]
    
    # Experience
    experience_type = Column(String(20))  # "Fresher" or "Experienced"
    
    # Communication
    spoken_languages = Column(JSONB, default=list)
    email = Column(String(255), unique=True, nullable=True)
    
    # Job Preferences - Consolidated into single JSONB field
    preference = Column(JSONB, default=dict)  # {"job_type": [...], "work_mode": [...], "preferred_job_role": [...], "preferred_location": [...], "expected_salary": ...}
    preferred_job_role = Column(JSONB, default=list)
    job_category = Column(String(100), nullable=True)
    tech_links = Column(JSONB, default=dict)
    
    # Settings
    email_notifications = Column(Boolean, default=True)
    status = Column(String(20), default="active")  # active, placed, inactive
    
    # Relationships
    applications = relationship("Application", back_populates="student", cascade="all, delete-orphan")
    notifications = relationship("StudentNotification", back_populates="student", cascade="all, delete-orphan")

    @property
    def languages(self):
        return self.spoken_languages

    @languages.setter
    def languages(self, value):
        self.spoken_languages = value

    def __repr__(self):
        return f"<Student {self.full_name}>"
