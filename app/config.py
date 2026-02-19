"""Application configuration using Pydantic Settings."""

from typing import List, Union, Annotated, Any
from pydantic import Field, field_validator, BeforeValidator
from pydantic_settings import BaseSettings, SettingsConfigDict


def parse_list_of_ints(v: Any) -> List[int]:
    """Parse comma-separated string to list of integers."""
    if isinstance(v, str):
        return [int(x.strip()) for x in v.split(",")]
    if isinstance(v, list):
        return v
    return [v]


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    APP_NAME: str = "Placement Dashboard API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    RELOAD: bool = True

    # Database
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/placement_db"
    )
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 10

    # JWT
    SECRET_KEY: str = Field(default="change-this-secret-key-in-production")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 hours
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30

    # Resume Storage (Local Filesystem)
    RESUME_STORAGE_DIR: str = "uploads/resumes"  # Local directory for storing resumes
    
    # MongoDB (for raw message storage - credentials in .env)
    MONGODB_USERNAME: str = ""
    MONGODB_PASSWORD: str = ""
    MONGODB_CLUSTER: str = "cluster0.apufdpu.mongodb.net"
    MONGODB_DATABASE: str = "placement_db"
    MONGODB_COLLECTION: str = "raw_messages"
    MONGODB_DB_NAME: str = "placement_db"  # Alias for database name
    STORAGE_TYPE: str = "mongodb"  # "local" or "mongodb" for raw message storage
    
    @property
    def MONGODB_URI(self) -> str:
        """Construct MongoDB connection URI"""
        if self.MONGODB_USERNAME and self.MONGODB_PASSWORD:
            return f"mongodb+srv://{self.MONGODB_USERNAME}:{self.MONGODB_PASSWORD}@{self.MONGODB_CLUSTER}/?retryWrites=true&w=majority"
        return f"mongodb+srv://{self.MONGODB_CLUSTER}/?retryWrites=true&w=majority"

    # Telegram
    TELEGRAM_API_ID: str = ""
    TELEGRAM_API_HASH: str = ""
    TELEGRAM_PHONE: str = ""
    
    # Telegram Scraping Config
    MAX_GROUPS_JOIN_PER_DAY: int = 2
    MIN_DELAY_BETWEEN_JOINS: int = 1800  # 30 minutes
    MAX_DELAY_BETWEEN_JOINS: int = 3600  # 60 minutes
    WORKING_HOURS_START: int = 10  # 10 AM
    WORKING_HOURS_END: int = 20  # 8 PM
    MESSAGES_FETCH_LIMIT: int = 75  # First time fetch
    INCREMENTAL_FETCH_LIMIT: int = 100  # Subsequent fetches

    # Email
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    EMAIL_FROM: str = "noreply@placement.org"
    
    # AI Providers
    AI_PROVIDER: str = "gemini"  # openai, gemini, openrouter
    AI_FALLBACK_PROVIDER: str = "openrouter"
    OPENAI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    OPENROUTER_API_KEY: str = ""
    APP_URL: str = "https://placement-dashboard.com"  # For OpenRouter referer
    
    # Google Sheets
    SHEET_ID: str = ""  # Google Sheets ID for daily job exports
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""

    # Celery (Optional - for background task queuing)
    CELERY_BROKER_URL: str = "redis://redis:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/0"
    
    # Redis Cache Configuration
    REDIS_URL: str = "redis://localhost:6379/1"  # Use database 1 for cache (0 is for Celery)
    REDIS_HOST: str = "localhost"  # Use 'redis' for Docker, 'localhost' for local dev
    REDIS_PORT: int = 6379
    REDIS_DB: int = 1
    REDIS_PASSWORD: str = ""  # Leave empty if no password
    REDIS_MAX_CONNECTIONS: int = 50
    REDIS_SOCKET_KEEPALIVE: bool = True
    REDIS_SOCKET_TIMEOUT: int = 5
    REDIS_RETRY_ON_TIMEOUT: bool = True
    REDIS_HEALTH_CHECK_INTERVAL: int = 30
    
    # Cache Settings
    CACHE_ENABLED: bool = True
    CACHE_DEFAULT_TTL: int = 3600  # 1 hour default
    CACHE_RECOMMENDATIONS_TTL: int = 1800  # 30 minutes for job recommendations
    CACHE_PROFILE_TTL: int = 7200  # 2 hours for student profiles
    CACHE_JOBS_TTL: int = 600  # 10 minutes for eligible jobs list
    CACHE_STATS_TTL: int = 300  # 5 minutes for statistics
    
    # File Upload
    MAX_UPLOAD_SIZE: int = 10485760  # 10MB
    ALLOWED_RESUME_EXTENSIONS: Union[str, List[str]] = ["pdf", "docx"]  # Accepts both formats

    # Pagination
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int = 100

    # Scraping
    TELEGRAM_SCRAPE_INTERVAL_MINUTES: int = 30
    JOB_RETENTION_DAYS: int = 90

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    # Monitoring & Alerting
    SENTRY_DSN: str = ""
    SENTRY_ENVIRONMENT: str = "production"
    SENTRY_TRACES_SAMPLE_RATE: float = 0.1
    CLOUDWATCH_ENABLED: bool = False  # Disabled to save AWS costs - use API visibility instead
    CLOUDWATCH_NAMESPACE: str = "TelegramScraper"
    SLACK_BOT_TOKEN: str = ""
    SLACK_CHANNEL_ID: str = ""
    SLACK_ALERTS_ENABLED: bool = True
    MAX_ALERTS_PER_HOUR: int = 5
    DAILY_SUMMARY_TIME: str = "09:00"  # Daily Slack summary time (24-hour format)
    
    # Channel Scoring & Quality Management
    CHANNEL_SCORE_LOW_THRESHOLD: int = 40
    CHANNEL_SCORE_INACTIVE_THRESHOLD: int = 25
    CHANNEL_MIN_JOBS_FOR_SCORING: int = 5
    JOB_RELEVANCE_CONFIG_PATH: str = "config/job_relevance_criteria.json"
    JOB_QUALITY_MIN_SCORE: int = 30
    SCORING_LOOKBACK_DAYS: int = 30
    SCORING_UPDATE_FREQUENCY: str = "daily"
    ENABLE_AUTO_DEACTIVATION: bool = True
    
    # ML Feedback & Training
    ML_MIN_FEEDBACK_FOR_RETRAIN: int = 50  # Minimum feedback samples before retraining
    ML_ACCURACY_THRESHOLD: float = 0.80  # Target accuracy (80%)
    ML_RETRAIN_SCHEDULE: str = "monthly"  # Auto-retrain frequency
    ML_EXTRACTOR_VERSION: str = "v2"  # Use enhanced extractor v2 by default
    ML_MODEL_PATH: str = "app/ml/models/"  # Path to store trained models
    
    # Scraping Schedule (4-hour intervals)
    SCRAPING_HOURS: Annotated[List[int], BeforeValidator(parse_list_of_ints)] = [4, 8, 12, 16, 20, 0]
    SCRAPING_TIMEZONE: str = "Asia/Kolkata"  # IST timezone

    @field_validator("ALLOWED_RESUME_EXTENSIONS", mode="before")
    @classmethod
    def parse_extensions(cls, v):
        """Parse comma-separated extensions."""
        if isinstance(v, str):
            return [ext.strip() for ext in v.split(",")]
        return v


# Create global settings instance
settings = Settings()
