"""Channel model for job sources."""

from sqlalchemy import Boolean, Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.db.base import Base


class Channel(Base):
    """Channel/Group source for job postings."""

    __tablename__ = "channels"

    name = Column(String(255), nullable=False)
    platform = Column(String(50), default="telegram")  # telegram, linkedin, indeed, direct
    channel_id = Column(String(100), unique=True, nullable=False, index=True)
    channel_url = Column(String(500))
    
    # Configuration
    config = Column(JSONB, default=dict)  # {"scrape_interval": 30, "filters": {...}}
    
    # Search parameters
    search_keywords = Column(JSONB, default=list)  # ["python developer", "backend engineer"]
    job_type_filter = Column(JSONB, default=list)  # ["remote", "office"]
    
    # Status
    is_active = Column(Boolean, default=True)
    last_scraped_at = Column(DateTime)
    last_error = Column(String(500))
    
    # Stats
    total_jobs_scraped = Column(String, default="0")
    
    # Relationships
    jobs = relationship("Job", back_populates="source_channel")

    def __repr__(self):
        return f"<Channel {self.name} ({self.platform})>"
