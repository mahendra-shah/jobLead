"""Main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from app.api.v1 import api_router
from app.routers import telegram_scraper, visibility, ml_feedback, slack_commands
from app.config import settings
from app.core.logging import setup_logging
from app.core.scheduler import start_scheduler, stop_scheduler
from app.core.cache import CacheManager
from app.db.session import engine, init_db

# Setup logging
setup_logging()

# Initialize Redis cache manager
cache_manager = CacheManager(settings)

# Initialize Sentry for error tracking (only if DSN is properly configured)
if settings.SENTRY_DSN and settings.SENTRY_DSN.startswith('https://'):
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENVIRONMENT,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
        integrations=[
            FastApiIntegration(),
            LoggingIntegration(
                level=None,  # Capture all logs
                event_level="ERROR",  # Only send ERROR and above as events
            ),
        ],
        # Performance monitoring
        enable_tracing=True,
        # Release tracking (optional)
        release=settings.APP_VERSION,
        # Additional options
        attach_stacktrace=True,
        send_default_pii=False,  # Don't send personally identifiable info
    )
else:
    print("⚠️  Sentry DSN not configured - error tracking disabled")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    await init_db()
    cache_manager.connect()  # Initialize Redis cache
    start_scheduler()  # Start APScheduler for background tasks
    yield
    # Shutdown
    stop_scheduler()  # Stop scheduler gracefully
    cache_manager.disconnect()  # Close Redis connection
    await engine.dispose()


# Create FastAPI app with custom OpenAPI configuration
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Job aggregation and matching platform for placement management",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    # Add HTTP and HTTPS schemes for production SSL support
    servers=[
        {"url": "https://api.pd.navgurukul.org", "description": "Dev Server (HTTP)"},
        {"url": "http://localhost:8000", "description": "Local Development (HTTP)"},
    ],
    swagger_ui_parameters={
        "persistAuthorization": True,  # Persist authorization after page refresh
    },
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include API router
app.include_router(api_router, prefix="/api/v1")

# Include telegram scraper router (no prefix - uses /api/telegram-scraper)
app.include_router(telegram_scraper.router)

# Include visibility API router (no prefix - uses /api/visibility)
app.include_router(visibility.router, prefix="/api/visibility", tags=["Visibility"])

# Include ML feedback router (no prefix - uses /api/ml-feedback)
app.include_router(ml_feedback.router, prefix="/api/ml-feedback", tags=["ML Feedback"])

# Include Slack commands router (no prefix - uses /api/slack)
app.include_router(slack_commands.router)


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint."""
    return {
        "message": "Placement Dashboard API",
        "version": settings.APP_VERSION,
        "status": "operational",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint with cache status."""
    health_data = {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "cache": cache_manager.get_stats() if cache_manager else {"enabled": False},
    }
    return health_data


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "message": str(exc) if settings.DEBUG else "An error occurred",
        },
    )
