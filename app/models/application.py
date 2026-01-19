"""Application model."""

from sqlalchemy import Column, ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.db.base import Base


class Application(Base):
    """Job application model."""

    __tablename__ = "applications"
    __table_args__ = (
        UniqueConstraint("student_id", "job_id", name="unique_student_job_application"),
    )

    student_id = Column(UUID(as_uuid=True), ForeignKey("students.id"), nullable=False, index=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False, index=True)
    
    # Matching
    match_score = Column(Numeric(5, 2))  # 0.00 - 100.00
    
    # Status tracking
    status = Column(String(20), default="applied")  # applied, viewed, shortlisted, interviewed, offered, rejected, accepted
    
    # Additional data
    cover_letter = Column(String(2000))
    extra_data = Column(JSONB, default=dict)  # Any additional tracking data (renamed from 'metadata' to avoid SQLAlchemy conflict)
    
    # Relationships
    student = relationship("Student", back_populates="applications")
    job = relationship("Job", back_populates="applications")

    def __repr__(self):
        return f"<Application {self.student_id} -> {self.job_id}>"
