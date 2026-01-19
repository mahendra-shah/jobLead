"""
Telegram Group Model
Stores Telegram channels/groups being monitored
"""
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from uuid import uuid4

from app.db.base import Base


class TelegramGroup(Base):
    __tablename__ = "telegram_groups"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    username = Column(String(255), unique=True, nullable=False, index=True)  # @channelname
    title = Column(String(500), nullable=True)
    url = Column(String(500), nullable=True)  # Full Telegram URL (https://t.me/channelname)
    category = Column(String(100), nullable=True)  # tech, non-tech, freelance, etc.
    
    # Group info
    members_count = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    
    # Join status
    is_joined = Column(Boolean, default=False, nullable=False)
    joined_by_account_id = Column(UUID(as_uuid=True), nullable=True)
    joined_at = Column(DateTime(timezone=True), nullable=True)
    
    # Scraping info
    last_scraped_at = Column(DateTime(timezone=True), nullable=True)
    last_message_id = Column(String(50), nullable=True)  # Telegram message ID
    last_message_date = Column(DateTime(timezone=True), nullable=True)
    messages_fetched_total = Column(Integer, default=0, nullable=False)
    
    # Health scoring
    health_score = Column(Float, default=100.0, nullable=False)
    total_messages_scraped = Column(Integer, default=0, nullable=False)
    job_messages_found = Column(Integer, default=0, nullable=False)
    quality_jobs_found = Column(Integer, default=0, nullable=False)  # Jobs with applications
    last_job_posted_at = Column(DateTime(timezone=True), nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    deactivated_at = Column(DateTime(timezone=True), nullable=True)
    deactivation_reason = Column(Text, nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    notes = Column(Text, nullable=True)

    def __repr__(self):
        return f"<TelegramGroup {self.username} (Score: {self.health_score})>"
    
    def calculate_health_score(self) -> float:
        """
        Calculate channel health score (0-100)
        Based on: job frequency, job ratio, quality ratio
        """
        from datetime import datetime, timedelta
        
        score = 100.0
        now = datetime.now()
        
        # Factor 1: Job posting frequency (40 points)
        if self.last_job_posted_at:
            days_since_last_job = (now - self.last_job_posted_at).days
            if days_since_last_job > 30:
                score -= 40  # Dead channel
            elif days_since_last_job > 14:
                score -= 20  # Slow channel
            elif days_since_last_job > 7:
                score -= 10  # Moderate
        else:
            score -= 40  # No jobs posted yet
        
        # Factor 2: Job-to-message ratio (30 points)
        if self.total_messages_scraped > 10:  # Only if we have enough data
            job_ratio = self.job_messages_found / self.total_messages_scraped
            if job_ratio < 0.05:    # Less than 5% jobs
                score -= 30
            elif job_ratio < 0.15:  # 5-15% jobs
                score -= 15
            elif job_ratio < 0.30:  # 15-30% jobs
                score -= 5
        
        # Factor 3: Job quality (30 points)
        if self.job_messages_found > 5:  # Only if we have enough jobs
            quality_ratio = self.quality_jobs_found / self.job_messages_found
            if quality_ratio < 0.10:    # Less than 10% quality
                score -= 30
            elif quality_ratio < 0.30:  # 10-30% quality
                score -= 15
        
        self.health_score = max(0.0, score)
        
        # Auto-update status
        if score < 30 and self.is_active:
            self.is_active = False
            self.deactivated_at = now
            self.deactivation_reason = f"Health score dropped to {score:.1f}"
        elif score >= 50 and not self.is_active and self.deactivated_at:
            # Reactivate if score improves
            self.is_active = True
            self.deactivated_at = None
            self.deactivation_reason = None
        
        return self.health_score
