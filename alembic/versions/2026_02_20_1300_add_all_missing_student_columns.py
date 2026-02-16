"""add_all_missing_student_profile_columns

Revision ID: add_all_missing_cols
Revises: add_resume_url_2026
Create Date: 2026-02-20 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'add_all_missing_cols'
down_revision = 'add_resume_url_2026'  # Based on the resume_url migration we just ran
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
            print(f"Added {column.name} column to {table_name} table")
        else:
            print(f"{column.name} column already exists in {table_name} table")
    
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
    
    # Add Skills columns (both technical and soft skills)
    add_column_if_not_exists('students', sa.Column('technical_skills', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    add_column_if_not_exists('students', sa.Column('soft_skills', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Add Experience columns
    add_column_if_not_exists('students', sa.Column('experience_type', sa.String(length=20), nullable=True))
    add_column_if_not_exists('students', sa.Column('internship_details', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    add_column_if_not_exists('students', sa.Column('projects', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Add Languages column
    add_column_if_not_exists('students', sa.Column('languages', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
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


def downgrade() -> None:
    # Remove all added columns (only if they exist)
    columns_to_drop = [
        'coding_platforms', 'portfolio_url', 'linkedin_profile', 'github_profile',
        'expected_salary', 'preferred_location', 'preferred_job_role', 'work_mode', 'job_type',
        'languages', 'projects', 'internship_details', 'experience_type',
        'soft_skills', 'technical_skills',
        'degree', 'cgpa', 'percentage', 'passing_year', 'branch', 'course',
        'college_id', 'college_name', 'highest_qualification',
        'current_address', 'gender', 'date_of_birth', 'last_name', 'first_name'
    ]
    
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_columns = [col['name'] for col in inspector.get_columns('students')]
    
    for col_name in columns_to_drop:
        if col_name in existing_columns:
            op.drop_column('students', col_name)

