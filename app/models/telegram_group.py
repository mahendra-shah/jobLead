"""
Telegram Group Model
Stores Telegram channels/groups being monitored
"""
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Text, ForeignKey
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
    telegram_account_id = Column(UUID(as_uuid=True), ForeignKey('telegram_accounts.id', ondelete='SET NULL'), nullable=True, index=True)  # FK to telegram_accounts
    joined_by_phone = Column(String(20), nullable=True)  # Phone number of account that joined
    joined_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationship to TelegramAccount
    telegram_account = relationship("TelegramAccount", foreign_keys=[telegram_account_id], backref="joined_groups")
    
    # Scraping info
    last_scraped_at = Column(DateTime(timezone=True), nullable=True)
    last_scraped_by_account = Column(UUID(as_uuid=True), ForeignKey('telegram_accounts.id', ondelete='SET NULL'), nullable=True, index=True)  # Which account last scraped this channel
    last_message_id = Column(String(50), nullable=True)  # Telegram message ID
    last_message_date = Column(DateTime(timezone=True), nullable=True)
    
    # Relationship for last scraper account
    last_scraper_account = relationship("TelegramAccount", foreign_keys=[last_scraped_by_account])
    
    # Health scoring
    health_score = Column(Float, default=100.0, nullable=False)
    total_messages_scraped = Column(Integer, default=0, nullable=False)
    job_messages_found = Column(Integer, default=0, nullable=False)
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    deactivated_at = Column(DateTime(timezone=True), nullable=True)
    deactivation_reason = Column(Text, nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<TelegramGroup {self.username} (Score: {self.health_score})>"
    
    def calculate_health_score(self) -> float:
        """
        Simple channel health score calculation (0-100)
        
        Factors:
        - Message activity (50%)
        - Job posting ratio (50%)
        """
        score = 0.0
        
        # Factor 1: Message activity (50 points)
        if self.total_messages_scraped > 0:
            # More messages = more active channel
            activity_score = min(50.0, self.total_messages_scraped / 100 * 50)
            score += activity_score
        
        # Factor 2: Job posting ratio (50 points)
        if self.total_messages_scraped > 0:
            job_ratio = self.job_messages_found / self.total_messages_scraped
            ratio_score = min(50.0, job_ratio * 100)
            score += ratio_score
        else:
            # New channel - start with neutral score
            score = 50.0
        
        self.health_score = round(max(0.0, min(100.0, score)), 2)
        return self.health_score
