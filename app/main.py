"""Main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.v1 import api_router
from app.routers import monitoring, telegram_scraper
from app.config import settings
from app.core.logging import setup_logging
from app.core.scheduler import start_scheduler, stop_scheduler
from app.db.session import engine, init_db

# Setup logging
setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    await init_db()
    start_scheduler()  # Start APScheduler for background tasks
    yield
    # Shutdown
    stop_scheduler()  # Stop scheduler gracefully
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
        {"url": "http://65.0.6.163", "description": "Dev Server (HTTP)"},
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

# Include monitoring router (no prefix - uses /api/monitoring)
app.include_router(monitoring.router)

# Include telegram scraper router (no prefix - uses /api/telegram-scraper)
app.include_router(telegram_scraper.router)


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
    """Health check endpoint."""
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
    }


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
