"""
DiscoverySource model for Phase 1 discovery.
Stores job boards, communities, Telegram/Discord sources found during discovery.
Separate from channels/telegram_groups; shortlisted items can be promoted later.
"""

from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.db.base import Base


class DiscoverySource(Base):
    __tablename__ = "discovery_sources"

    name = Column(String(500), nullable=False)
    url = Column(Text, nullable=False, index=True)
    source_type = Column(String(50), nullable=False, index=True)  # job_board, community, telegram_channel, discord, website, github_repo
    platform = Column(String(50), nullable=True)  # telegram, discord, web, meetup, etc.
    city = Column(String(100), nullable=True, index=True)
    region = Column(String(100), nullable=True)  # India, Global, etc.
    country_code = Column(String(10), nullable=True)
    metadata_ = Column("metadata", JSONB, default=dict)
    phase = Column(Integer, nullable=False, default=1, index=True)
    is_shortlisted = Column(Boolean, default=False, nullable=False)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    notes = Column(Text, nullable=True)

    def __repr__(self):
        return f"<DiscoverySource {self.name} ({self.source_type})>"
