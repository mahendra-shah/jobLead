"""Job model."""

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, Text, BigInteger
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
    experience_required = Column(String(50))  # "0-2 years", "2-5 years" (legacy)
    salary_range = Column(JSONB, default=dict)  # {"min": 50000, "max": 80000, "currency": "USD"} (legacy)
    
    # Structured fields (new - for filtering and stats)
    is_fresher = Column(Boolean, nullable=True, index=True)  # True for fresher jobs (0-1 year)
    work_type = Column(String(50), nullable=True)  # remote, on-site, hybrid
    experience_min = Column(Integer, nullable=True, index=True)  # Minimum years
    experience_max = Column(Integer, nullable=True, index=True)  # Maximum years
    salary_min = Column(Float, nullable=True)  # Minimum salary (in INR)
    salary_max = Column(Float, nullable=True)  # Maximum salary (in INR)
    
    # Location
    location = Column(String(255))
    job_type = Column(String(50))  # remote, office, hybrid
    employment_type = Column(String(50))  # fulltime, parttime, contract, freelance, internship
    
    # Source information
    source = Column(String(100), default="telegram")  # telegram, direct, linkedin, etc.
    source_url = Column(Text)
    source_channel_id = Column(UUID(as_uuid=True), ForeignKey("channels.id"), nullable=True)  # OLD - deprecated
    source_channel_name = Column(String(500), nullable=True)  # NEW - Channel username from MongoDB
    source_telegram_channel_id = Column(String(100), nullable=True)  # NEW - Actual Telegram channel_id from MongoDB
    sender_id = Column(BigInteger, nullable=True)  # NEW - Sender user ID from MongoDB
    fetched_by_account = Column(Integer, nullable=True)  # NEW - MongoDB account ID (integer like 1, 2, 3)
    telegram_group_id = Column(UUID(as_uuid=True), nullable=True)  # NO FK - telegram_groups table empty
    scraped_by_account_id = Column(UUID(as_uuid=True), nullable=True)  # NO FK - telegram_accounts table empty
    raw_text = Column(Text)  # Original job posting text
    
    # ML & Deduplication
    embedding = Column(Vector(1536))  # OpenAI text-embedding-3-small dimensions
    content_hash = Column(String(32), index=True)  # MD5 hash for deduplication
    duplicate_of_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=True)  # Made nullable
    source_message_id = Column(String(255))  # Link to raw messages (MongoDB message_id)
    ml_confidence = Column(String(10))  # Confidence score from ML classifier
    
    # Job Quality Scoring (NEW - Feb 2026)
    quality_score = Column(Float, nullable=True, index=True)  # Overall quality 0-100
    relevance_score = Column(Float, nullable=True)  # Relevance to criteria 0-100
    extraction_completeness_score = Column(Float, nullable=True)  # Field completeness 0-100
    meets_relevance_criteria = Column(Boolean, default=False, index=True)  # Passes relevance filters
    quality_breakdown = Column(JSONB, default=dict)  # Detailed scoring breakdown
    # Example: {"experience_match": 85, "field_completeness": 70, "skill_relevance": 90}
    relevance_reasons = Column(JSONB, default=list)  # List of match/mismatch reasons
    
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
    source_channel = relationship("Channel", back_populates="jobs")  # OLD - deprecated
    # telegram_group = relationship("TelegramGroup", foreign_keys=[telegram_group_id])  # Commented out - tables empty
    # scraped_by_account = relationship("TelegramAccount", foreign_keys=[scraped_by_account_id])  # Commented out - tables empty
    # Using forward reference to avoid circular import issues
    applications = relationship("Application", back_populates="job", cascade="all, delete-orphan", lazy="dynamic")
    
    def __repr__(self):
        return f"<Job {self.title} at {self.company_id}>"
