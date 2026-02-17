"""Alembic migration script template."""

"""merge_all_heads_and_drop_raw_messages

Revision ID: f8b0e91096c3
Revises: 53d35646646f, add_skill_required, recreate_saved_jobs_user
Create Date: 2026-02-17 08:04:08.623870+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f8b0e91096c3'
down_revision = ('53d35646646f', 'add_skill_required', 'recreate_saved_jobs_user')
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Merge all migration heads and forcefully drop raw_telegram_messages table.
    
    Raw messages should ONLY be stored in MongoDB.
    PostgreSQL should ONLY contain classified job data.
    """
    # Forcefully drop the table with CASCADE to handle any dependencies
    op.execute("DROP TABLE IF EXISTS raw_telegram_messages CASCADE")
    
    # Also drop indexes if they still exist (shouldn't, but being thorough)
    # Note: These will fail silently if indexes don't exist
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_raw_telegram_messages_processing_status"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_raw_telegram_messages_processed"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_raw_telegram_messages_group_username"))


def downgrade() -> None:
    """
    No downgrade - raw_telegram_messages table should not be recreated.
    Use MongoDB for raw messages.
    """
    pass
