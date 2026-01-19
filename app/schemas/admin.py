"""Admin schemas for Telegram scraping management."""
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel


# Scraping Log Schemas
class ScrapingLogBase(BaseModel):
    lambda_function: str
    execution_id: str
    status: str


class ScrapingLogResponse(ScrapingLogBase):
    id: int
    started_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    metrics: Optional[Dict[str, Any]]
    errors: Optional[Dict[str, Any]]
    cost_estimate: Optional[float]

    class Config:
        from_attributes = True


class ScrapingLogListResponse(BaseModel):
    logs: List[ScrapingLogResponse]
    total: int
    page: int
    page_size: int


# Telegram Account Schemas
class TelegramAccountBase(BaseModel):
    phone: str
    api_id: int
    api_hash: str


class TelegramAccountCreate(TelegramAccountBase):
    session_string: Optional[str] = None


class TelegramAccountUpdate(BaseModel):
    is_active: Optional[bool] = None
    is_banned: Optional[bool] = None


class TelegramAccountResponse(BaseModel):
    id: int
    phone: str
    api_id: int
    is_active: bool
    is_banned: bool
    groups_joined_count: int
    last_used_at: Optional[datetime]
    last_join_at: Optional[datetime]
    created_at: datetime
    
    # Usage stats
    can_join_today: bool
    groups_joined_today: int

    class Config:
        from_attributes = True


class TelegramAccountListResponse(BaseModel):
    accounts: List[TelegramAccountResponse]
    total: int


# Telegram Group Schemas
class TelegramGroupBase(BaseModel):
    username: str
    title: str
    category: Optional[str]


class TelegramGroupUpdate(BaseModel):
    is_active: Optional[bool] = None
    category: Optional[str] = None


class TelegramGroupResponse(BaseModel):
    id: int
    username: str
    title: str
    category: Optional[str]
    is_joined: bool
    joined_at: Optional[datetime]
    joined_by_account_id: Optional[int]
    last_message_id: Optional[int]
    last_scraped_at: Optional[datetime]
    health_score: Optional[float]
    total_messages_scraped: int
    job_messages_found: int
    quality_jobs_found: int
    is_active: bool
    deactivated_at: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True


class TelegramGroupListResponse(BaseModel):
    groups: List[TelegramGroupResponse]
    total: int
    page: int
    page_size: int


class HealthScoreHistory(BaseModel):
    date: datetime
    health_score: float
    total_messages: int
    job_messages: int
    quality_jobs: int


class TelegramGroupHealthHistoryResponse(BaseModel):
    group_id: int
    username: str
    current_health_score: Optional[float]
    history: List[HealthScoreHistory]


# Dashboard Stats
class DashboardStats(BaseModel):
    total_jobs: int
    total_jobs_today: int
    total_messages_processed: int
    total_messages_today: int
    total_accounts: int
    active_accounts: int
    total_groups: int
    active_groups: int
    joined_groups: int
    average_health_score: float
    
    # Recent activity
    last_group_join: Optional[datetime]
    last_message_scrape: Optional[datetime]
    last_job_extraction: Optional[datetime]
    
    # Performance metrics
    jobs_extracted_last_24h: int
    duplicates_found_last_24h: int
    messages_scraped_last_24h: int
    
    # Cost estimates
    estimated_cost_today: float
    estimated_cost_month: float


# Manual Trigger Schemas
class TriggerScrapeRequest(BaseModel):
    lambda_function: str  # "group_joiner", "message_scraper", "job_processor"
    force: bool = False  # Force run even outside working hours


class TriggerScrapeResponse(BaseModel):
    success: bool
    message: str
    execution_id: Optional[str]
