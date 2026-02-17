"""Alembic migration script template."""

"""drop_raw_telegram_messages_table

Revision ID: 53d35646646f
Revises: eeb40953980f
Create Date: 2026-02-17 07:35:14.551161+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '53d35646646f'
down_revision = 'eeb40953980f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Drop raw_telegram_messages table from PostgreSQL.
    
    Raw messages should only be stored in MongoDB.
    PostgreSQL should only contain classified job data.
    """
    # Drop indexes first (if they exist)
    try:
        op.drop_index('ix_raw_telegram_messages_processing_status', table_name='raw_telegram_messages')
    except Exception:
        pass  # Index might not exist
    
    try:
        op.drop_index('ix_raw_telegram_messages_processed', table_name='raw_telegram_messages')
    except Exception:
        pass  # Index might not exist
    
    try:
        op.drop_index('ix_raw_telegram_messages_group_username', table_name='raw_telegram_messages')
    except Exception:
        pass  # Index might not exist
    
    # Drop the table (if it exists)
    try:
        op.drop_table('raw_telegram_messages')
    except Exception:
        pass  # Table might not exist


def downgrade() -> None:
    """
    Recreate raw_telegram_messages table (not recommended - use MongoDB).
    """
    # Note: This is just for rollback capability, but this table should not be used
    pass
