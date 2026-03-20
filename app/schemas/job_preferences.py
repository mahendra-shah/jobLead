"""
Schemas for job scraping preferences
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime


class JobPreferencesBase(BaseModel):
    """Base schema for job preferences"""
    allowed_job_types: List[str] = Field(
        ...,
        description="Allowed job types (full-time, internship, contract, part-time)",
        min_items=1
    )
    excluded_job_types: Optional[List[str]] = Field(
        default=None,
        description="Job types to exclude"
    )
    experience: Optional[str] = Field(
        default=None,
        description="Experience preference as text (e.g., '0-2 years', 'fresher')"
    )
    accept_unspecified_experience: Optional[bool] = Field(
        default=True,
        description="Accept jobs without experience specified"
    )
    allowed_education_levels: Optional[List[str]] = Field(
        default=None,
        description="Allowed education levels (Bachelor's, Master's, PhD, etc.)"
    )
    preferred_locations: Optional[List[str]] = Field(
        default=None,
        description="Preferred job locations"
    )
    allow_all_india: Optional[bool] = Field(
        default=True,
        description="Accept jobs from anywhere in India"
    )
    allow_international: Optional[bool] = Field(
        default=False,
        description="Accept international jobs"
    )
    allowed_work_modes: Optional[List[str]] = Field(
        default=None,
        description="Allowed work modes (remote, hybrid, onsite)"
    )
    priority_skills: Optional[List[str]] = Field(
        default=None,
        description="Skills to prioritize"
    )
    excluded_skills: Optional[List[str]] = Field(
        default=None,
        description="Skills to exclude"
    )
    salary: Optional[str] = Field(
        default=None,
        description="Salary preference as text (e.g., '3-6 LPA')"
    )
    filter_by_salary: Optional[bool] = Field(
        default=False,
        description="Whether to filter by salary range"
    )
    excluded_companies: Optional[List[str]] = Field(
        default=None,
        description="Companies to exclude"
    )
    preferred_companies: Optional[List[str]] = Field(
        default=None,
        description="Companies to prefer"
    )
    required_keywords: Optional[List[str]] = Field(
        default=None,
        description="Keywords that must be present"
    )
    excluded_keywords: Optional[List[str]] = Field(
        default=None,
        description="Keywords to exclude"
    )
    min_ai_confidence_score: Optional[int] = Field(
        default=70,
        ge=0,
        le=100,
        description="Minimum AI confidence score (0-100)"
    )
    max_messages_per_run: Optional[int] = Field(
        default=50,
        ge=1,
        le=500,
        description="Maximum messages to process per run"
    )
    skip_duplicate_threshold_hours: Optional[int] = Field(
        default=48,
        ge=1,
        description="Hours to check for duplicate jobs"
    )
    notes: Optional[str] = Field(
        default=None,
        description="Admin notes about these preferences"
    )

class JobPreferencesUpdate(BaseModel):
    """Schema for updating job preferences - all fields optional"""
    allowed_job_types: Optional[List[str]] = None
    excluded_job_types: Optional[List[str]] = None
    experience: Optional[str] = None
    accept_unspecified_experience: Optional[bool] = None
    allowed_education_levels: Optional[List[str]] = None
    preferred_locations: Optional[List[str]] = None
    allow_all_india: Optional[bool] = None
    allow_international: Optional[bool] = None
    allowed_work_modes: Optional[List[str]] = None
    priority_skills: Optional[List[str]] = None
    excluded_skills: Optional[List[str]] = None
    salary: Optional[str] = None
    filter_by_salary: Optional[bool] = None
    excluded_companies: Optional[List[str]] = None
    preferred_companies: Optional[List[str]] = None
    required_keywords: Optional[List[str]] = None
    excluded_keywords: Optional[List[str]] = None
    min_ai_confidence_score: Optional[int] = Field(default=None, ge=0, le=100)
    max_messages_per_run: Optional[int] = Field(default=None, ge=1, le=500)
    skip_duplicate_threshold_hours: Optional[int] = Field(default=None, ge=1)
    notes: Optional[str] = None


class JobPreferencesResponse(JobPreferencesBase):
    """Schema for job preferences response"""
    id: UUID
    is_active: bool
    created_by: Optional[UUID] = None
    updated_by: Optional[UUID] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class FilteringStats(BaseModel):
    """Statistics about message filtering"""
    total_messages: int = Field(description="Total messages in storage")
    processed_count: int = Field(description="Messages successfully processed")
    pending_count: int = Field(description="Messages pending processing")
    not_a_job_count: int = Field(description="Messages rejected as not a job")
    skipped_count: int = Field(description="Messages skipped for other reasons")
    error_count: int = Field(description="Messages with processing errors")
    jobs_created: int = Field(description="Jobs successfully created")
    average_attempts: float = Field(description="Average processing attempts")
    skip_reasons: dict = Field(description="Breakdown of skip reasons")
    time_range_days: int = Field(description="Number of days included in stats")


class ProcessingStatsResponse(BaseModel):
    """Response for processing statistics"""
    storage_stats: FilteringStats
    storage_type: str = Field(description="Type of storage (local/dynamodb)")
    storage_size: Optional[str] = Field(default=None, description="Storage size (local only)")
    preferences_active: bool = Field(description="Whether preferences are active")
    last_updated: datetime = Field(description="When stats were generated")
