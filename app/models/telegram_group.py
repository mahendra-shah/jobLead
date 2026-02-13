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
    
    # Enhanced Scoring (NEW - Feb 2026)
    health_score_breakdown = Column(Text, nullable=True)  # JSON string of score components
    relevant_jobs_count = Column(Integer, default=0, nullable=False)  # Jobs meeting relevance criteria
    total_jobs_posted = Column(Integer, default=0, nullable=False)  # All jobs extracted
    relevance_ratio = Column(Float, nullable=True)  # relevant_jobs / total_jobs
    avg_job_quality_score = Column(Float, nullable=True)  # Average quality_score of jobs
    status_label = Column(String(50), default="active", nullable=False)  # active, less_relevant, inactive
    last_score_update = Column(DateTime(timezone=True), nullable=True)
    score_history = Column(Text, nullable=True)  # JSON array of historical scores
    
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
        Enhanced channel health score calculation (0-100)
        
        New Factors (Feb 2026):
        - Job quality average (40%) - Average quality_score of jobs
        - Relevance ratio (30%) - Percentage of jobs meeting relevance criteria
        - Posting frequency (15%) - How recently jobs were posted
        - Engagement rate (15%) - Application/view rates
        
        Uses env-configured thresholds for status updates.
        """
        from datetime import datetime, timedelta
        from app.config import settings
        import json
        
        score = 0.0
        breakdown = {}
        now = datetime.now()
        
        # Factor 1: Average Job Quality (40 points)
        if self.total_jobs_posted > 0 and self.avg_job_quality_score is not None:
            # Scale 0-100 score to 0-40 points
            quality_points = (self.avg_job_quality_score / 100) * 40
            breakdown['job_quality_avg'] = round(quality_points, 2)
            score += quality_points
        else:
            # No jobs yet - neutral score
            breakdown['job_quality_avg'] = 0
            score += 0
        
        # Factor 2: Relevance Ratio (30 points)
        if self.total_jobs_posted >= settings.CHANNEL_MIN_JOBS_FOR_SCORING:
            # Use actual relevance ratio
            if self.relevance_ratio is not None:
                relevance_points = self.relevance_ratio * 30
                breakdown['relevance_ratio'] = round(relevance_points, 2)
                score += relevance_points
            else:
                # Fallback to neutral
                breakdown['relevance_ratio'] = 15
                score += 15
        else:
            # Not enough data - give neutral score
            breakdown['relevance_ratio'] = 15
            score += 15
        
        # Factor 3: Posting Frequency (15 points)
        frequency_points = self._calculate_frequency_score()
        breakdown['posting_frequency'] = round(frequency_points, 2)
        score += frequency_points
        
        # Factor 4: Engagement Rate (15 points)
        engagement_points = self._calculate_engagement_score()
        breakdown['engagement_rate'] = round(engagement_points, 2)
        score += engagement_points
        
        # Store breakdown as JSON string
        self.health_score_breakdown = json.dumps(breakdown)
        self.health_score = round(max(0.0, min(100.0, score)), 2)
        self.last_score_update = now
        
        # Update status label based on thresholds
        self._update_status_label()
        
        # Append to score history
        self._append_score_history(self.health_score)
        
        return self.health_score
    
    def _calculate_frequency_score(self) -> float:
        """Calculate points based on job posting frequency (0-15 points)"""
        from datetime import datetime, timedelta
        
        if not self.last_job_posted_at:
            return 0.0  # No jobs posted yet
        
        days_since_last_job = (datetime.now() - self.last_job_posted_at).days
        
        if days_since_last_job <= 3:
            return 15.0  # Very active
        elif days_since_last_job <= 7:
            return 12.0  # Active
        elif days_since_last_job <= 14:
            return 8.0   # Moderate
        elif days_since_last_job <= 30:
            return 4.0   # Slow
        else:
            return 0.0   # Dead
    
    def _calculate_engagement_score(self) -> float:
        """Calculate points based on job engagement (0-15 points)"""
        # This would require joining with jobs table to get avg engagement
        # For now, use quality_jobs_found as proxy
        if self.total_jobs_posted == 0:
            return 7.5  # Neutral
        
        engagement_ratio = self.quality_jobs_found / self.total_jobs_posted
        
        if engagement_ratio >= 0.30:
            return 15.0  # High engagement
        elif engagement_ratio >= 0.15:
            return 11.0  # Good engagement
        elif engagement_ratio >= 0.05:
            return 7.0   # Medium engagement
        else:
            return 3.0   # Low engagement
    
    def _update_status_label(self):
        """Update status label based on env-configured thresholds"""
        from app.config import settings
        
        old_status = self.status_label
        score = self.health_score
        
        if score < settings.CHANNEL_SCORE_INACTIVE_THRESHOLD:
            self.status_label = "inactive"
            if settings.ENABLE_AUTO_DEACTIVATION and self.is_active:
                self.is_active = False
                self.deactivated_at = datetime.now()
                self.deactivation_reason = (
                    f"Health score {score:.1f} below inactive threshold "
                    f"({settings.CHANNEL_SCORE_INACTIVE_THRESHOLD})"
                )
        
        elif score < settings.CHANNEL_SCORE_LOW_THRESHOLD:
            self.status_label = "less_relevant"
            # Keep active but flagged as less relevant
        
        else:
            self.status_label = "active"
            # Reactivate if was previously deactivated
            if not self.is_active and self.deactivated_at:
                self.is_active = True
                self.deactivated_at = None
                self.deactivation_reason = None
    
    def _append_score_history(self, score: float):
        """Append current score to history (keep last 30 entries)"""
        import json
        from datetime import datetime
        
        try:
            history = json.loads(self.score_history) if self.score_history else []
        except (json.JSONDecodeError, TypeError):
            history = []
        
        # Add new entry
        history.append({
            "date": datetime.now().date().isoformat(),
            "score": round(score, 2),
            "status": self.status_label
        })
        
        # Keep only last 30 entries
        history = history[-30:]
        
        self.score_history = json.dumps(history)
