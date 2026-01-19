"""
Scraping Log Model
Tracks Lambda function executions and scraping runs
"""
from sqlalchemy import Column, String, Integer, Float, DateTime, Text, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from uuid import uuid4

from app.db.base import Base


class ScrapingLog(Base):
    __tablename__ = "scraping_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Lambda function info
    lambda_function = Column(String(100), nullable=False, index=True)  # joiner, scraper, processor
    execution_id = Column(String(255), nullable=True)  # AWS execution ID
    
    # Timing
    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Float, nullable=True)
    
    # Status
    status = Column(String(50), default='running', nullable=False)  # running, success, failed, partial
    
    # Metrics
    accounts_used = Column(Integer, default=0, nullable=False)
    groups_processed = Column(Integer, default=0, nullable=False)
    messages_fetched = Column(Integer, default=0, nullable=False)
    jobs_extracted = Column(Integer, default=0, nullable=False)
    duplicates_found = Column(Integer, default=0, nullable=False)
    errors_count = Column(Integer, default=0, nullable=False)
    
    # Details
    errors = Column(JSON, nullable=True)  # Array of error messages
    extra_metadata = Column(JSON, nullable=True)  # Additional data
    
    # Cost estimation
    cost_estimate = Column(Float, nullable=True)  # USD
    
    # Notes
    notes = Column(Text, nullable=True)

    def __repr__(self):
        return f"<ScrapingLog {self.lambda_function} at {self.started_at} ({self.status})>"
    
    def calculate_duration(self):
        """Calculate duration if completed"""
        if self.completed_at and self.started_at:
            delta = self.completed_at - self.started_at
            self.duration_seconds = delta.total_seconds()
        return self.duration_seconds
