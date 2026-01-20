"""Database models."""

# Import all models in dependency order to ensure proper relationship initialization
# This prevents SQLAlchemy circular dependency errors

# Base models (no foreign keys)
from app.models.user import User
from app.models.company import Company
from app.models.channel import Channel

# Models with foreign keys to base models
from app.models.student import Student
from app.models.job import Job

# Models with foreign keys to other models
from app.models.application import Application
from app.models.student_interactions import SavedJob, JobView, StudentNotification

# Export all models
__all__ = [
    "User",
    "Company",
    "Channel",
    "Student",
    "Job",
    "Application",
    "SavedJob",
    "JobView",
    "StudentNotification",
]
