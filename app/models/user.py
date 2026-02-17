"""User model."""

from sqlalchemy import Boolean, Column, String
from sqlalchemy.orm import relationship

from app.db.base import Base


class User(Base):
    """User model for authentication."""

    __tablename__ = "users"

    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(150), unique=True, index=True, nullable=True)
    password_hash = Column(String(255), nullable=True)
    role = Column(String(20), nullable=False, default="student")
    is_active = Column(Boolean, default=True, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    
    # Relationships
    saved_jobs = relationship("SavedJob", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User {self.email} ({self.role})>"
