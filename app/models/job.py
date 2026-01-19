"""Job model."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector

from app.db.base import Base


class Job(Base):
    """Job posting model."""

    __tablename__ = "jobs"

    title = Column(String(500), nullable=False, index=True)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id"), nullable=True)  # Made nullable for ML processing
    description = Column(Text)
    
    # Job details
    skills_required = Column(JSONB, default=list)  # ["Python", "FastAPI", ...]
    experience_required = Column(String(50))  # "0-2 years", "2-5 years"
    salary_range = Column(JSONB, default=dict)  # {"min": 50000, "max": 80000, "currency": "USD"}
    
    # Location
    location = Column(String(255))
    job_type = Column(String(50))  # remote, office, hybrid
    employment_type = Column(String(50))  # fulltime, parttime, contract, freelance, internship
    
    # Source information
    source = Column(String(100), default="telegram")  # telegram, direct, linkedin, etc.
    source_url = Column(Text)
    source_channel_id = Column(UUID(as_uuid=True), ForeignKey("channels.id"), nullable=True)  # Made nullable
    raw_text = Column(Text)  # Original job posting text
    
    # ML & Deduplication
    embedding = Column(Vector(1536))  # OpenAI text-embedding-3-small dimensions
    content_hash = Column(String(32), index=True)  # MD5 hash for deduplication
    duplicate_of_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=True)  # Made nullable
    source_message_id = Column(String(255))  # Link to raw messages (MongoDB message_id)
    ml_confidence = Column(String(10))  # Confidence score from ML classifier
    
    # Visibility & Recommendation Tracking
    students_shown_to = Column(JSONB, default=list)  # List of student IDs who saw this job
    max_students_to_show = Column(Integer, default=999)  # Default: show to all
    visibility_mode = Column(String(20), default='all')  # 'all', 'random_one', 'vacancy_based'
    vacancy_count = Column(Integer, default=1)  # Number of openings
    
    # Status
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    expires_at = Column(String)  # Job expiration date
    
    # Stats
    view_count = Column(Integer, default=0)
    application_count = Column(Integer, default=0)
    
    # Relationships
    company = relationship("Company", back_populates="jobs")
    source_channel = relationship("Channel", back_populates="jobs")
    applications = relationship("Application", back_populates="job", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Job {self.title} at {self.company_id}>"
