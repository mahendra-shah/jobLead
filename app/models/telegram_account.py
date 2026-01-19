"""
Telegram Account Model
Stores Telegram account credentials for rotation
"""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from uuid import uuid4

from app.db.base import Base


class TelegramAccount(Base):
    __tablename__ = "telegram_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    phone = Column(String(20), unique=True, nullable=False, index=True)
    api_id = Column(String(50), nullable=False)
    api_hash = Column(String(100), nullable=False)
    session_string = Column(Text, nullable=True)  # Encrypted session data
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    is_banned = Column(Boolean, default=False, nullable=False)
    
    # Usage tracking
    groups_joined_count = Column(Integer, default=0, nullable=False)
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    last_join_at = Column(DateTime(timezone=True), nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    notes = Column(Text, nullable=True)  # Admin notes

    def __repr__(self):
        return f"<TelegramAccount {self.phone}>"
    
    def can_join_today(self, max_joins_per_day: int = 2) -> bool:
        """Check if account can join more groups today"""
        from datetime import datetime, timedelta
        
        if not self.is_active or self.is_banned:
            return False
        
        if not self.last_join_at:
            return True
        
        # Check if last join was today
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if self.last_join_at < today_start:
            return True  # Last join was yesterday or earlier
        
        # Check daily limit (would need to query account_group_joins table)
        return True  # Simplified - actual check in service layer
