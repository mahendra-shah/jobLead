"""Student model."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
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
    
    # JSON fields
    skills = Column(JSONB, default=list)  # ["Python", "React", ...]
    preferences = Column(JSONB, default=dict)  # {"remote": true, "min_salary": 50000, ...}
    education = Column(JSONB, default=list)  # [{"degree": "B.Tech", "year": 2024}, ...]
    experience = Column(JSONB, default=list)  # [{"company": "X", "role": "Dev", "years": 2}, ...]
    
    # Profile metadata
    profile_score = Column(Integer, default=0)
    profile_embedding = Column(Vector(1536))  # For personalized job matching
    
    # Settings
    email_notifications = Column(Boolean, default=True)
    status = Column(String(20), default="active")  # active, placed, inactive
    
    # Relationships
    user = relationship("User", backref="student_profile")
    applications = relationship("Application", back_populates="student", cascade="all, delete-orphan")
    saved_jobs = relationship("SavedJob", back_populates="student", cascade="all, delete-orphan")
    notifications = relationship("StudentNotification", back_populates="student", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Student {self.full_name}>"
