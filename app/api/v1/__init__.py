"""API v1 routes."""

from fastapi import APIRouter

from app.api.v1 import auth, students, jobs, companies, channels, applications, admin
from app.api.v1.endpoints import (
    admin_ml, 
    students as student_crud, 
    student_profile,
    job_recommendations,
    saved_jobs,
    student_dashboard
)

api_router = APIRouter()

# Include all route modules
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(students.router, prefix="/students", tags=["Students"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])
api_router.include_router(companies.router, prefix="/companies", tags=["Companies"])
api_router.include_router(channels.router, prefix="/channels", tags=["Channels"])
api_router.include_router(applications.router, prefix="/applications", tags=["Applications"])
api_router.include_router(admin.router, prefix="/admin", tags=["Admin"])
api_router.include_router(admin_ml.router, prefix="/admin/ml", tags=["Admin ML"])

# Student Management System - New endpoints
api_router.include_router(student_crud.router, prefix="/admin/students", tags=["Admin - Student Management"])
api_router.include_router(student_profile.router, prefix="/students/me", tags=["Student Profile"])
api_router.include_router(job_recommendations.router, prefix="/students/me", tags=["Job Recommendations"])
api_router.include_router(saved_jobs.router, prefix="/students/me/saved-jobs", tags=["Saved Jobs"])
api_router.include_router(student_dashboard.router, prefix="/students/me", tags=["Student Dashboard"])
