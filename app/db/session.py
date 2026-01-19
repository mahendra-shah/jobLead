"""Database session and engine configuration."""

import os
from typing import AsyncGenerator, Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.db.base import Base

# Load environment variables
load_dotenv()

# Create async engine
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_size=settings.DATABASE_POOL_SIZE,
    max_overflow=settings.DATABASE_MAX_OVERFLOW,
    pool_pre_ping=True,
)

# Create synchronous engine for background tasks (ML processor)
# Use LOCAL_DATABASE_URL if available (for host machine execution), otherwise use DATABASE_URL
local_db_url = os.getenv("LOCAL_DATABASE_URL")
if local_db_url:
    sync_database_url = local_db_url
    print(f"✅ Using LOCAL_DATABASE_URL for sync engine: {sync_database_url}")
else:
    sync_database_url = settings.DATABASE_URL.replace("+asyncpg", "")  # Remove async driver
    print(f"⚠️  Using DATABASE_URL for sync engine: {sync_database_url}")

sync_engine = create_engine(
    sync_database_url,
    echo=settings.DEBUG,
    pool_size=settings.DATABASE_POOL_SIZE,
    max_overflow=settings.DATABASE_MAX_OVERFLOW,
    pool_pre_ping=True,
)

# Create async session factory
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

# Create sync session factory
SyncSessionLocal = sessionmaker(
    sync_engine,
    class_=Session,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_sync_db() -> Generator[Session, None, None]:
    """Get synchronous database session for background tasks."""
    session = SyncSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


async def init_db():
    """Initialize database tables."""
    async with engine.begin() as conn:
        # Enable pgvector extension (required for ML recommendations)
        from sqlalchemy import text
        try:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            print("✅ pgvector extension enabled")
        except Exception as e:
            print(f"⚠️  Could not enable pgvector: {e}")
            print("   This is OK if the extension is already enabled or not available.")
        
        # Import all models to register them
        from app.models import application, channel, company, job, student, user

        # Create tables (in production, use Alembic migrations)
        if settings.DEBUG:
            await conn.run_sync(Base.metadata.create_all)
