"""
Raw Telegram Message Model
Stores unprocessed messages from Telegram channels
"""
from sqlalchemy import Column, String, Text, Boolean, DateTime, BigInteger
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from uuid import uuid4

from app.db.base import Base


class RawTelegramMessage(Base):
    __tablename__ = "raw_telegram_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Telegram message info
    message_id = Column(BigInteger, nullable=False)  # Telegram's message ID
    group_username = Column(String(255), nullable=False, index=True)
    group_title = Column(String(500), nullable=True)
    
    # Message content
    text = Column(Text, nullable=False)
    sender_id = Column(BigInteger, nullable=True)
    sender_username = Column(String(255), nullable=True)
    
    # Message metadata
    message_date = Column(DateTime(timezone=True), nullable=False)
    has_media = Column(Boolean, default=False, nullable=False)
    media_type = Column(String(50), nullable=True)  # photo, document, etc.
    
    # Processing status
    fetched_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    processed = Column(Boolean, default=False, nullable=False, index=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processing_status = Column(String(50), default='pending', nullable=False)  # pending, processed, failed, duplicate
    processing_error = Column(Text, nullable=True)
    
    # Link to extracted job (if any)
    job_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Unique constraint on message_id + group
    __table_args__ = (
        {'comment': 'Raw messages fetched from Telegram channels before AI processing'},
    )

    def __repr__(self):
        return f"<RawTelegramMessage {self.message_id} from {self.group_username}>"
