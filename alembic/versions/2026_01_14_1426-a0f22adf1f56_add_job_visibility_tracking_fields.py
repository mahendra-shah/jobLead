"""add_job_visibility_tracking_fields

Revision ID: a0f22adf1f56
Revises: add_job_prefs
Create Date: 2026-01-14 14:26:51.227817+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a0f22adf1f56'
down_revision = 'add_job_prefs'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add job visibility tracking and ML confidence fields."""
    # Add new columns for job processing pipeline
    op.add_column('jobs', sa.Column('content_hash', sa.String(length=32), nullable=True))
    op.add_column('jobs', sa.Column('source_message_id', sa.UUID(), nullable=True))
    op.add_column('jobs', sa.Column('ml_confidence', sa.String(length=10), nullable=True))
    op.add_column('jobs', sa.Column('students_shown_to', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('jobs', sa.Column('max_students_to_show', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('visibility_mode', sa.String(length=20), nullable=True))
    op.add_column('jobs', sa.Column('vacancy_count', sa.Integer(), nullable=True))
    
    # Create index on content_hash for deduplication
    op.create_index(op.f('ix_jobs_content_hash'), 'jobs', ['content_hash'], unique=False)


def downgrade() -> None:
    """Remove job visibility tracking and ML confidence fields."""
    # Drop index
    op.drop_index(op.f('ix_jobs_content_hash'), table_name='jobs')
    
    # Drop columns
    op.drop_column('jobs', 'vacancy_count')
    op.drop_column('jobs', 'visibility_mode')
    op.drop_column('jobs', 'max_students_to_show')
    op.drop_column('jobs', 'students_shown_to')
    op.drop_column('jobs', 'ml_confidence')
    op.drop_column('jobs', 'source_message_id')
    op.drop_column('jobs', 'content_hash')
