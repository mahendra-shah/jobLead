"""Alembic migration script template."""

"""add_experience_salary_fields

Revision ID: 441d50951a98
Revises: a8ca4893d0fa
Create Date: 2026-01-20 15:19:31.914776+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '441d50951a98'
down_revision = 'a8ca4893d0fa'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add experience and salary fields to jobs table."""
    
    # Add experience fields
    op.add_column('jobs', sa.Column('min_experience', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('max_experience', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('is_fresher', sa.Boolean(), nullable=False, server_default='false'))
    
    # Add salary fields (for easier queries since salary_range is JSONB)
    op.add_column('jobs', sa.Column('min_salary', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('max_salary', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('salary_currency', sa.String(length=3), nullable=True, server_default='INR'))
    
    # Add indexes for performance
    op.create_index('idx_jobs_min_experience', 'jobs', ['min_experience'])
    op.create_index('idx_jobs_is_fresher', 'jobs', ['is_fresher'])
    op.create_index('idx_jobs_salary_range', 'jobs', ['min_salary', 'max_salary'])
    
    # Composite index for active jobs with experience filtering
    op.create_index(
        'idx_jobs_active_experience',
        'jobs',
        ['is_active', 'min_experience', 'max_experience'],
        postgresql_where=sa.text('is_active = true')
    )


def downgrade() -> None:
    """Remove experience and salary fields from jobs table."""
    
    # Drop indexes
    op.drop_index('idx_jobs_active_experience', table_name='jobs')
    op.drop_index('idx_jobs_salary_range', table_name='jobs')
    op.drop_index('idx_jobs_is_fresher', table_name='jobs')
    op.drop_index('idx_jobs_min_experience', table_name='jobs')
    
    # Drop columns
    op.drop_column('jobs', 'salary_currency')
    op.drop_column('jobs', 'max_salary')
    op.drop_column('jobs', 'min_salary')
    op.drop_column('jobs', 'is_fresher')
    op.drop_column('jobs', 'max_experience')
    op.drop_column('jobs', 'min_experience')
