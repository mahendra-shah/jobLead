"""remove_unused_columns_from_telegram_groups

Revision ID: 587dbf1d2cf4
Revises: 894bc01aa60b
Create Date: 2026-02-26 10:28:36.307078+00:00

Removes the following columns from telegram_groups table:
- messages_fetched_total
- quality_jobs_found
- last_job_posted_at
- notes
- health_score_breakdown
- relevant_jobs_count
- total_jobs_posted
- avg_job_quality_score
- relevance_ratio
- status_label
- last_score_update
- score_history
- joined_by_account_id (legacy field, replaced by telegram_account_id)
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '587dbf1d2cf4'
down_revision = '894bc01aa60b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Remove unused columns from telegram_groups table."""
    # Drop columns
    op.drop_column('telegram_groups', 'messages_fetched_total')
    op.drop_column('telegram_groups', 'quality_jobs_found')
    op.drop_column('telegram_groups', 'last_job_posted_at')
    op.drop_column('telegram_groups', 'notes')
    op.drop_column('telegram_groups', 'health_score_breakdown')
    op.drop_column('telegram_groups', 'relevant_jobs_count')
    op.drop_column('telegram_groups', 'total_jobs_posted')
    op.drop_column('telegram_groups', 'avg_job_quality_score')
    op.drop_column('telegram_groups', 'relevance_ratio')
    op.drop_column('telegram_groups', 'status_label')
    op.drop_column('telegram_groups', 'last_score_update')
    op.drop_column('telegram_groups', 'score_history')
    op.drop_column('telegram_groups', 'joined_by_account_id')


def downgrade() -> None:
    """Restore removed columns (for rollback)."""
    # Restore columns in reverse order
    op.add_column('telegram_groups', sa.Column('joined_by_account_id', sa.Integer(), nullable=True))
    op.add_column('telegram_groups', sa.Column('score_history', sa.Text(), nullable=True))
    op.add_column('telegram_groups', sa.Column('last_score_update', sa.DateTime(timezone=True), nullable=True))
    op.add_column('telegram_groups', sa.Column('status_label', sa.String(50), server_default='active', nullable=False))
    op.add_column('telegram_groups', sa.Column('relevance_ratio', sa.Float(), nullable=True))
    op.add_column('telegram_groups', sa.Column('avg_job_quality_score', sa.Float(), nullable=True))
    op.add_column('telegram_groups', sa.Column('total_jobs_posted', sa.Integer(), server_default='0', nullable=False))
    op.add_column('telegram_groups', sa.Column('relevant_jobs_count', sa.Integer(), server_default='0', nullable=False))
    op.add_column('telegram_groups', sa.Column('health_score_breakdown', sa.Text(), nullable=True))
    op.add_column('telegram_groups', sa.Column('notes', sa.Text(), nullable=True))
    op.add_column('telegram_groups', sa.Column('last_job_posted_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('telegram_groups', sa.Column('quality_jobs_found', sa.Integer(), server_default='0', nullable=False))
    op.add_column('telegram_groups', sa.Column('messages_fetched_total', sa.Integer(), server_default='0', nullable=False))
