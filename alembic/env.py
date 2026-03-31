"""Alembic environment configuration."""

import os
import sys
from pathlib import Path
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Add the parent directory to Python path so we can import app
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.db.base import Base

# Import all models to ensure they are registered
from app.models import user, student, company, job, application, channel  # noqa

# Alembic Config object
config = context.config


def _sync_migrations_url() -> str:
    """Same DB as scripts/sync engine: LOCAL_DATABASE_URL wins, else DATABASE_URL (psycopg2-style)."""
    local = os.getenv("LOCAL_DATABASE_URL")
    if local and str(local).strip():
        return str(local).strip()
    database_url = str(settings.DATABASE_URL)
    if database_url.startswith("postgresql+asyncpg://"):
        database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
        database_url = database_url.replace("?ssl=false", "?sslmode=disable")
        database_url = database_url.replace("?ssl=true", "?sslmode=require")
        database_url = database_url.replace("?ssl=require", "?sslmode=require")
        database_url = database_url.replace("&ssl=false", "&sslmode=disable")
        database_url = database_url.replace("&ssl=true", "&sslmode=require")
        database_url = database_url.replace("&ssl=require", "&sslmode=require")
    return database_url


# Default URL for offline / config
config.set_main_option("sqlalchemy.url", _sync_migrations_url())

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    database_url = _sync_migrations_url()
    config.set_main_option("sqlalchemy.url", database_url)
    
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
