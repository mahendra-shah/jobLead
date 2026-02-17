"""Student model."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Date, Float, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector

from app.db.base import Base


class Student(Base):
    """Student profile model."""

    __tablename__ = "students"

    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), unique=True, nullable=False)
    full_name = Column(String(255), nullable=False)
    phone = Column(String(20))
    resume_path = Column(String(500))
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
    degree = Column(String(100))  # Keep for backward compatibility
    
    # Skills (separate technical and soft skills)
    technical_skills = Column(JSONB, default=list)  # ["Python", "React", ...]
    soft_skills = Column(JSONB, default=list)  # ["Communication", "Teamwork", ...]
    skills = Column(JSONB, default=list)  # Legacy field - keep for backward compatibility
    
    # Experience
    experience_type = Column(String(20))  # "Fresher" or "Experienced"
    internship_details = Column(JSONB, default=list)  # List of internship objects
    projects = Column(JSONB, default=list)  # List of project objects
    experience = Column(JSONB, default=list)  # Legacy field - keep for backward compatibility
    
    # Languages
    languages = Column(JSONB, default=list)  # List of language objects with proficiency
    
    # Job Preferences
    job_type = Column(JSONB, default=list)  # ["Full-Time", "Internship", "Part-Time"]
    work_mode = Column(JSONB, default=list)  # ["Remote", "Hybrid", "Office"]
    preferred_job_role = Column(JSONB, default=list)  # ["Software Developer", ...]
    preferred_location = Column(JSONB, default=list)  # ["Bangalore", "Remote", ...]
    expected_salary = Column(Integer)
    preferences = Column(JSONB, default=dict)  # Legacy field - keep for backward compatibility
    
    # Technical Profile Links
    github_profile = Column(String(500))
    linkedin_profile = Column(String(500))
    portfolio_url = Column(String(500))
    coding_platforms = Column(JSONB, default=dict)  # {"LeetCode": "username", ...}
    
    # Education (legacy JSONB field - keep for backward compatibility)
    education = Column(JSONB, default=list)
    
    # Profile metadata
    profile_score = Column(Integer, default=0)
    profile_embedding = Column(Vector(1536))  # For personalized job matching
    
    # Settings
    email_notifications = Column(Boolean, default=True)
    status = Column(String(20), default="active")  # active, placed, inactive
    
    # Relationships
    user = relationship("User", backref="student_profile")
    applications = relationship("Application", back_populates="student", cascade="all, delete-orphan")
    notifications = relationship("StudentNotification", back_populates="student", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Student {self.full_name}>"
