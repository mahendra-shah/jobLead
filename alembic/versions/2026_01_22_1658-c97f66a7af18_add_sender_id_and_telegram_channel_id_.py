"""Alembic migration script template."""

"""add_sender_id_and_telegram_channel_id_to_jobs

Revision ID: c97f66a7af18
Revises: dfed0c74a385
Create Date: 2026-01-22 16:58:54.055812+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c97f66a7af18'
down_revision = 'dfed0c74a385'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add source_telegram_channel_id column
    op.add_column('jobs', sa.Column('source_telegram_channel_id', sa.String(length=100), nullable=True))
    op.create_index(op.f('ix_jobs_source_telegram_channel_id'), 'jobs', ['source_telegram_channel_id'], unique=False)
    
    # Add sender_id column
    op.add_column('jobs', sa.Column('sender_id', sa.BigInteger(), nullable=True))
    op.create_index(op.f('ix_jobs_sender_id'), 'jobs', ['sender_id'], unique=False)


def downgrade() -> None:
    # Remove columns
    op.drop_index(op.f('ix_jobs_sender_id'), table_name='jobs')
    op.drop_column('jobs', 'sender_id')
    op.drop_index(op.f('ix_jobs_source_telegram_channel_id'), table_name='jobs')
    op.drop_column('jobs', 'source_telegram_channel_id')
