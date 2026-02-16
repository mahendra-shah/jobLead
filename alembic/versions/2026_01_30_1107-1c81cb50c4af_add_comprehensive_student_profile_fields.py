"""Alembic migration script template."""

"""add_comprehensive_student_profile_fields

Revision ID: 1c81cb50c4af
Revises: a8ca4893d0fa
Create Date: 2026-01-30 11:07:10.663899+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '1c81cb50c4af'
down_revision = 'a8ca4893d0fa'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Helper function to add column only if it doesn't exist
    def add_column_if_not_exists(table_name, column):
        conn = op.get_bind()
        inspector = sa.inspect(conn)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if column.name not in columns:
            op.add_column(table_name, column)
    
    # Add Personal Details columns
    add_column_if_not_exists('students', sa.Column('first_name', sa.String(length=100), nullable=True))
    add_column_if_not_exists('students', sa.Column('last_name', sa.String(length=100), nullable=True))
    add_column_if_not_exists('students', sa.Column('date_of_birth', sa.Date(), nullable=True))
    add_column_if_not_exists('students', sa.Column('gender', sa.String(length=50), nullable=True))
    add_column_if_not_exists('students', sa.Column('current_address', sa.Text(), nullable=True))
    
    # Add Education Details columns
    add_column_if_not_exists('students', sa.Column('highest_qualification', sa.String(length=100), nullable=True))
    add_column_if_not_exists('students', sa.Column('college_name', sa.String(length=200), nullable=True))
    add_column_if_not_exists('students', sa.Column('college_id', sa.Integer(), nullable=True))
    add_column_if_not_exists('students', sa.Column('course', sa.String(length=100), nullable=True))
    add_column_if_not_exists('students', sa.Column('branch', sa.String(length=100), nullable=True))
    add_column_if_not_exists('students', sa.Column('passing_year', sa.Integer(), nullable=True))
    add_column_if_not_exists('students', sa.Column('percentage', sa.Float(), nullable=True))
    add_column_if_not_exists('students', sa.Column('cgpa', sa.Float(), nullable=True))
    add_column_if_not_exists('students', sa.Column('degree', sa.String(length=100), nullable=True))
    
    # Add Skills columns (separate technical and soft skills)
    add_column_if_not_exists('students', sa.Column('technical_skills', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    # soft_skills already exists, skip it
    
    # Add Experience columns
    add_column_if_not_exists('students', sa.Column('experience_type', sa.String(length=20), nullable=True))
    add_column_if_not_exists('students', sa.Column('internship_details', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    # projects already exists, skip it
    
    # Languages already exists, skip it
    
    # Add Job Preferences columns
    add_column_if_not_exists('students', sa.Column('job_type', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    add_column_if_not_exists('students', sa.Column('work_mode', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    add_column_if_not_exists('students', sa.Column('preferred_job_role', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    add_column_if_not_exists('students', sa.Column('preferred_location', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    add_column_if_not_exists('students', sa.Column('expected_salary', sa.Integer(), nullable=True))
    
    # Add Technical Profile Links columns
    add_column_if_not_exists('students', sa.Column('github_profile', sa.String(length=500), nullable=True))
    add_column_if_not_exists('students', sa.Column('linkedin_profile', sa.String(length=500), nullable=True))
    add_column_if_not_exists('students', sa.Column('portfolio_url', sa.String(length=500), nullable=True))
    add_column_if_not_exists('students', sa.Column('coding_platforms', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Add resume_url column
    add_column_if_not_exists('students', sa.Column('resume_url', sa.String(length=500), nullable=True))


def downgrade() -> None:
    # Remove all added columns
    op.drop_column('students', 'resume_url')
    op.drop_column('students', 'coding_platforms')
    op.drop_column('students', 'portfolio_url')
    op.drop_column('students', 'linkedin_profile')
    op.drop_column('students', 'github_profile')
    op.drop_column('students', 'expected_salary')
    op.drop_column('students', 'preferred_location')
    op.drop_column('students', 'preferred_job_role')
    op.drop_column('students', 'work_mode')
    op.drop_column('students', 'job_type')
    op.drop_column('students', 'languages')
    op.drop_column('students', 'projects')
    op.drop_column('students', 'internship_details')
    op.drop_column('students', 'experience_type')
    op.drop_column('students', 'soft_skills')
    op.drop_column('students', 'technical_skills')
    op.drop_column('students', 'degree')
    op.drop_column('students', 'percentage')
    op.drop_column('students', 'course')
    op.drop_column('students', 'college_name')
    op.drop_column('students', 'highest_qualification')
    op.drop_column('students', 'current_address')
    op.drop_column('students', 'gender')
    op.drop_column('students', 'date_of_birth')
    op.drop_column('students', 'last_name')
    op.drop_column('students', 'first_name')
