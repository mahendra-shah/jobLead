"""
Telegram Account Model
Stores Telegram account credentials for rotation
"""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from uuid import uuid4
import enum

from app.db.base import Base


class HealthStatus(str, enum.Enum):
    """Account health status enum."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    BANNED = "BANNED"


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
    
    # Health Tracking
    health_status = Column(Enum(HealthStatus, native_enum=False, values_callable=lambda x: [e.value for e in x]), default=HealthStatus.HEALTHY, nullable=False)
    last_successful_fetch_at = Column(DateTime(timezone=True), nullable=True)
    consecutive_errors = Column(Integer, default=0, nullable=False)
    last_error_message = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)
    
    # Usage tracking
    groups_joined_count = Column(Integer, default=0, nullable=False)
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    last_join_at = Column(DateTime(timezone=True), nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    notes = Column(Text, nullable=True)  # Admin notes

    def __repr__(self):
        return f"<TelegramAccount {self.phone} ({self.health_status.value})>"
    
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
    
    def is_healthy(self) -> bool:
        """Check if account is in healthy status."""
        return self.health_status == HealthStatus.HEALTHY and self.is_active and not self.is_banned
    
    def mark_error(self, error_message: str) -> None:
        """
        Mark an error occurrence and update health status.
        
        Args:
            error_message: Error message to record
        """
        from datetime import datetime
        
        self.consecutive_errors += 1
        self.last_error_message = error_message
        self.last_error_at = datetime.utcnow()
        
        # Update health status based on error count
        if self.consecutive_errors >= 3:
            self.health_status = HealthStatus.DEGRADED
        
        # If it's an AuthKey error, mark as banned
        if "AuthKeyError" in error_message or "auth key" in error_message.lower():
            self.health_status = HealthStatus.BANNED
            self.is_banned = True
            self.is_active = False
    
    def mark_success(self) -> None:
        """Mark successful operation and reset error counters."""
        from datetime import datetime
        
        self.consecutive_errors = 0
        self.last_successful_fetch_at = datetime.utcnow()
        
        # Only reset to healthy if not banned
        if not self.is_banned and self.is_active:
            self.health_status = HealthStatus.HEALTHY
