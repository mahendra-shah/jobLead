"""Student model."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Date, Float, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.db.base import Base


class Student(Base):
    """Student profile model."""

    __tablename__ = "students"

    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), unique=True, nullable=False)
    full_name = Column(String(255), nullable=False)
    phone = Column(String(20))
    resume_url = Column(String(500))  # S3 URL for resume
    
    # Personal Details
    first_name = Column(String(100))
    last_name = Column(String(100))
    date_of_birth = Column(Date)
    gender = Column(String(50))
    current_address = Column(Text)
    
    # Education Details
    highest_qualification = Column(String(100))
    college_name = Column(String(200))
    college_id = Column(Integer, nullable=True)  # ForeignKey removed - colleges table doesn't exist yet
    course = Column(String(100))
    branch = Column(String(100))
    passing_year = Column(Integer)
    percentage = Column(Float)
    cgpa = Column(Float)
    
    # Skills (separate technical and soft skills)
    technical_skills = Column(JSONB, default=list)  # ["Python", "React", ...]
    soft_skills = Column(JSONB, default=list)  # ["Communication", "Teamwork", ...]
    
    # Experience
    experience_type = Column(String(20))  # "Fresher" or "Experienced"
    internship_details = Column(JSONB, default=list)  # List of internship objects
    projects = Column(JSONB, default=list)  # List of project objects
    
    # Languages
    languages = Column(JSONB, default=list)  # List of language objects with proficiency
    
    # Job Preferences - Consolidated into single JSONB field
    preference = Column(JSONB, default=dict)  # {"job_type": [...], "work_mode": [...], "preferred_job_role": [...], "preferred_location": [...], "expected_salary": ...}
    
    # Technical Profile Links
    github_profile = Column(String(500))
    linkedin_profile = Column(String(500))
    portfolio_url = Column(String(500))
    coding_platforms = Column(JSONB, default=dict)  # {"LeetCode": "username", ...}
    
    # Settings
    email_notifications = Column(Boolean, default=True)
    status = Column(String(20), default="active")  # active, placed, inactive
    
    # Relationships
    user = relationship("User", backref="student_profile")
    applications = relationship("Application", back_populates="student", cascade="all, delete-orphan")
    notifications = relationship("StudentNotification", back_populates="student", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Student {self.full_name}>"
