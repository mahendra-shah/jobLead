"""
Student-related database models
Saved jobs, job views, and student preferences
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, Float, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime

from app.db.base import Base


class SavedJob(Base):
    """
    Jobs saved/bookmarked by users
    One-to-many relation with users table
    One-to-one relation with jobs table
    """
    __tablename__ = "saved_jobs"
    
    # Note: id, created_at, updated_at are inherited from Base class (UUID)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Optional organization
    folder = Column(String(100), nullable=True)  # e.g., "Applied", "Interested", "Dream Companies"
    notes = Column(Text, nullable=True)  # User's private notes
    
    # Timestamps
    saved_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    user = relationship("User", back_populates="saved_jobs")
    job = relationship("Job", back_populates="saved_jobs")
    
    # Indexes
    __table_args__ = (
        Index("idx_saved_jobs_user", "user_id"),
        Index("idx_saved_jobs_job", "job_id"),
        Index("idx_saved_jobs_user_job", "user_id", "job_id", unique=True),  # Prevent duplicates
        Index("idx_saved_jobs_folder", "user_id", "folder"),
    )
    
    def __repr__(self):
        return f"<SavedJob(user_id={self.user_id}, job_id={self.job_id})>"


class JobView(Base):
    """
    Track when students view jobs (for analytics and recommendations)
    """
    __tablename__ = "job_views"
    
    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(UUID(as_uuid=True), ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    
    # View metadata
    viewed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    duration_seconds = Column(Integer, nullable=True)  # How long they stayed on page
    source = Column(String(50), nullable=True)  # 'feed', 'search', 'saved', 'notification', etc.
    
    # Device/context (optional)
    user_agent = Column(String(500), nullable=True)
    ip_address = Column(String(50), nullable=True)
    
    # Relationships
    student = relationship("Student")
    job = relationship("Job")
    
    # Indexes
    __table_args__ = (
        Index("idx_job_views_student", "student_id"),
        Index("idx_job_views_job", "job_id"),
        Index("idx_job_views_viewed_at", "viewed_at"),
        Index("idx_job_views_student_job", "student_id", "job_id"),
    )
    
    def __repr__(self):
        return f"<JobView(student_id={self.student_id}, job_id={self.job_id}, viewed_at={self.viewed_at})>"


class StudentNotification(Base):
    """
    Notifications for students (new jobs, deadlines, etc.)
    """
    __tablename__ = "student_notifications"
    
    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(UUID(as_uuid=True), ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    
    # Notification type
    type = Column(String(50), nullable=False)  # 'new_job', 'job_deadline', 'saved_job_update', etc.
    
    # Content
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    
    # Optional deep link
    link = Column(String(500), nullable=True)  # URL to navigate to
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True)
    
    # Metadata
    data = Column(JSON, nullable=True)  # Additional data
    
    # Status
    read = Column(Boolean, default=False, nullable=False)
    read_at = Column(DateTime, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=True)  # Optional expiration
    
    # Relationships
    student = relationship("Student", back_populates="notifications")
    job = relationship("Job")
    
    # Indexes
    __table_args__ = (
        Index("idx_notifications_student", "student_id"),
        Index("idx_notifications_student_unread", "student_id", "read"),
        Index("idx_notifications_type", "type"),
        Index("idx_notifications_created_at", "created_at"),
    )
    
    def __repr__(self):
        return f"<StudentNotification(student_id={self.student_id}, type={self.type}, read={self.read})>"


class JobRecommendation(Base):
    """
    Cached job recommendations for students (optional - for performance)
    Regenerated daily or when preferences change
    """
    __tablename__ = "job_recommendations"
    
    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(UUID(as_uuid=True), ForeignKey("students.id", ondelete="CASCADE"), nullable=False)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    
    # Recommendation score (0-100)
    score = Column(Float, nullable=False)
    
    # Match reasons (for display to user)
    match_reasons = Column(JSON, nullable=True)  # Array of reasons
    missing_skills = Column(JSON, nullable=True)  # Array of skills student doesn't have
    
    # Metadata
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False)  # Recommendations expire after 24 hours
    
    # Relationships
    student = relationship("Student")
    job = relationship("Job")
    
    # Indexes
    __table_args__ = (
        Index("idx_recommendations_student", "student_id"),
        Index("idx_recommendations_student_score", "student_id", "score"),
        Index("idx_recommendations_expires_at", "expires_at"),
        Index("idx_recommendations_student_job", "student_id", "job_id", unique=True),
    )
    
    def __repr__(self):
        return f"<JobRecommendation(student_id={self.student_id}, job_id={self.job_id}, score={self.score})>"
