"""Alembic migration script template."""

"""add_performance_indexes_for_jobs

Revision ID: c18bc9f699d4
Revises: f8b0e91096c3
Create Date: 2026-02-17 08:32:35.873656+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c18bc9f699d4'
down_revision = 'f8b0e91096c3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Add performance indexes for jobs table to optimize GET /api/v1/jobs/ endpoint.
    
    These indexes target the most common filter combinations:
    - is_active + created_at (default query with sorting)
    - location (frequently filtered)
    - work_type (frequently filtered)
    - job_type (frequently filtered)
    - employment_type (frequently filtered)
    - is_active + is_fresher (fresher jobs filter)
    - is_active + work_type + experience_min (compound filter)
    """
    # Index for default query (active jobs sorted by created_at)
    op.create_index(
        'ix_jobs_is_active_created_at',
        'jobs',
        ['is_active', 'created_at'],
        postgresql_using='btree'
    )
    
    # Index for location filtering (case-insensitive partial match)
    # Using GIN index with pg_trgm for fast ILIKE queries
    op.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
    op.create_index(
        'ix_jobs_location_trgm',
        'jobs',
        ['location'],
        postgresql_using='gin',
        postgresql_ops={'location': 'gin_trgm_ops'}
    )
    
    # Index for work_type filtering
    op.create_index(
        'ix_jobs_work_type',
        'jobs',
        ['work_type'],
        postgresql_using='btree'
    )
    
    # Index for job_type filtering
    op.create_index(
        'ix_jobs_job_type',
        'jobs',
        ['job_type'],
        postgresql_using='btree'
    )
    
    # Index for employment_type filtering
    op.create_index(
        'ix_jobs_employment_type',
        'jobs',
        ['employment_type'],
        postgresql_using='btree'
    )
    
    # Composite index for fresher jobs (common query)
    op.create_index(
        'ix_jobs_is_active_is_fresher',
        'jobs',
        ['is_active', 'is_fresher'],
        postgresql_where=sa.text('is_fresher IS NOT NULL'),
        postgresql_using='btree'
    )
    
    # Composite index for remote jobs with experience (common query)
    op.create_index(
        'ix_jobs_is_active_work_type_exp',
        'jobs',
        ['is_active', 'work_type', 'experience_min'],
        postgresql_where=sa.text('work_type IS NOT NULL'),
        postgresql_using='btree'
    )
    
    # Index for salary filtering
    op.create_index(
        'ix_jobs_salary_min',
        'jobs',
        ['salary_min'],
        postgresql_where=sa.text('salary_min IS NOT NULL'),
        postgresql_using='btree'
    )
    
    op.create_index(
        'ix_jobs_salary_max',
        'jobs',
        ['salary_max'],
        postgresql_where=sa.text('salary_max IS NOT NULL'),
        postgresql_using='btree'
    )
    
    # Index for company_id (filtering by company)
    op.create_index(
        'ix_jobs_company_id',
        'jobs',
        ['company_id'],
        postgresql_where=sa.text('company_id IS NOT NULL'),
        postgresql_using='btree'
    )


def downgrade() -> None:
    """Remove performance indexes."""
    op.drop_index('ix_jobs_company_id', table_name='jobs')
    op.drop_index('ix_jobs_salary_max', table_name='jobs')
    op.drop_index('ix_jobs_salary_min', table_name='jobs')
    op.drop_index('ix_jobs_is_active_work_type_exp', table_name='jobs')
    op.drop_index('ix_jobs_is_active_is_fresher', table_name='jobs')
    op.drop_index('ix_jobs_employment_type', table_name='jobs')
    op.drop_index('ix_jobs_job_type', table_name='jobs')
    op.drop_index('ix_jobs_work_type', table_name='jobs')
    op.drop_index('ix_jobs_location_trgm', table_name='jobs')
    op.drop_index('ix_jobs_is_active_created_at', table_name='jobs')
