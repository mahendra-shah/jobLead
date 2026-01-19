"""Application configuration using Pydantic Settings."""

from typing import List, Union, Annotated
from pydantic import Field, field_validator, BeforeValidator
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # AWS
    AWS_REGION: str = "ap-south-1"  # Mumbai region
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    S3_BUCKET_NAME: str = "placement-resumes"
    DYNAMODB_TABLE_NAME: str = "telegram-raw-jobs"
    
    # MongoDB (for raw message storage - credentials in .env)
    MONGODB_USERNAME: str = ""
    MONGODB_PASSWORD: str = ""
    MONGODB_CLUSTER: str = "cluster0.apufdpu.mongodb.net"
    MONGODB_DATABASE: str = "placement_db"
    MONGODB_COLLECTION: str = "raw_messages"
    MONGODB_DB_NAME: str = "placement_db"  # Alias for database name
    STORAGE_TYPE: str = "local"  # "local", "mongodb", or "dynamodb"
    
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

    # Celery (Optional - for background task queuing)
    CELERY_BROKER_URL: str = "redis://redis:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/0"
    
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

    @field_validator("ALLOWED_RESUME_EXTENSIONS", mode="before")
    @classmethod
    def parse_extensions(cls, v):
        """Parse comma-separated extensions."""
        if isinstance(v, str):
            return [ext.strip() for ext in v.split(",")]
        return v


# Create global settings instance
settings = Settings()
