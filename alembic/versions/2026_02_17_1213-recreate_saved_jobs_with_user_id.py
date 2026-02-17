"""recreate_saved_jobs_with_user_id

Revision ID: recreate_saved_jobs_user
Revises: add_all_missing_cols
Create Date: 2026-02-17 12:13:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'recreate_saved_jobs_user'
down_revision = 'merge_heads_2026_02_17'  # After merge migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop existing saved_jobs table if it exists
    op.execute("DROP TABLE IF EXISTS saved_jobs CASCADE")
    
    # Create new saved_jobs table with user_id instead of student_id
    op.create_table(
        'saved_jobs',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('jobs.id', ondelete='CASCADE'), nullable=False),
        sa.Column('folder', sa.String(length=100), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('saved_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=True, onupdate=sa.text('CURRENT_TIMESTAMP')),
    )
    
    # Create indexes
    op.create_index('idx_saved_jobs_user', 'saved_jobs', ['user_id'])
    op.create_index('idx_saved_jobs_job', 'saved_jobs', ['job_id'])
    op.create_index('idx_saved_jobs_user_job', 'saved_jobs', ['user_id', 'job_id'], unique=True)
    op.create_index('idx_saved_jobs_folder', 'saved_jobs', ['user_id', 'folder'])
    op.create_index(op.f('ix_saved_jobs_id'), 'saved_jobs', ['id'], unique=False)


def downgrade() -> None:
    # Drop the new table
    op.drop_table('saved_jobs')
    
    # Note: We don't recreate the old table with student_id as it was removed per user request

