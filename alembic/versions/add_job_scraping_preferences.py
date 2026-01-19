"""add job scraping preferences

Revision ID: add_job_prefs
Revises: telegram_scraping_v1
Create Date: 2026-01-14 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_job_prefs'
down_revision = 'telegram_scraping_v1'  # Depends on telegram scraping migration
branch_labels = None
depends_on = None


def upgrade():
    # Create job_scraping_preferences table
    op.create_table(
        'job_scraping_preferences',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('allowed_job_types', postgresql.ARRAY(sa.String()), nullable=False),
        sa.Column('excluded_job_types', postgresql.ARRAY(sa.String())),
        sa.Column('min_experience_years', sa.Integer(), nullable=True),
        sa.Column('max_experience_years', sa.Integer(), nullable=True),
        sa.Column('accept_unspecified_experience', sa.Boolean(), nullable=True),
        sa.Column('allowed_education_levels', postgresql.ARRAY(sa.String())),
        sa.Column('preferred_locations', postgresql.ARRAY(sa.String())),
        sa.Column('allow_all_india', sa.Boolean(), nullable=True),
        sa.Column('allow_international', sa.Boolean(), nullable=True),
        sa.Column('allowed_work_modes', postgresql.ARRAY(sa.String())),
        sa.Column('priority_skills', postgresql.ARRAY(sa.String())),
        sa.Column('excluded_skills', postgresql.ARRAY(sa.String())),
        sa.Column('min_salary_lpa', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('max_salary_lpa', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('filter_by_salary', sa.Boolean(), nullable=True),
        sa.Column('excluded_companies', postgresql.ARRAY(sa.String())),
        sa.Column('preferred_companies', postgresql.ARRAY(sa.String())),
        sa.Column('required_keywords', postgresql.ARRAY(sa.String())),
        sa.Column('excluded_keywords', postgresql.ARRAY(sa.String())),
        sa.Column('min_ai_confidence_score', sa.Integer(), nullable=True),
        sa.Column('max_messages_per_run', sa.Integer(), nullable=True),
        sa.Column('skip_duplicate_threshold_hours', sa.Integer(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('updated_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
    )
    
    # Create unique constraint for active preferences
    op.create_index(
        'idx_job_prefs_active_unique',
        'job_scraping_preferences',
        ['is_active'],
        unique=True,
        postgresql_where=sa.text('is_active = true')
    )
    
    # Insert default preferences
    op.execute("""
        INSERT INTO job_scraping_preferences (
            id,
            allowed_job_types,
            excluded_job_types,
            min_experience_years,
            max_experience_years,
            accept_unspecified_experience,
            allowed_education_levels,
            preferred_locations,
            allow_all_india,
            allow_international,
            allowed_work_modes,
            priority_skills,
            excluded_skills,
            required_keywords,
            excluded_keywords,
            min_ai_confidence_score,
            max_messages_per_run,
            skip_duplicate_threshold_hours,
            filter_by_salary,
            is_active
        ) VALUES (
            gen_random_uuid(),
            ARRAY['full-time', 'internship', 'contract'],
            ARRAY['part-time'],
            0,
            5,
            true,
            ARRAY['B.Tech', 'B.E', 'B.Sc', 'BCA', 'M.Tech', 'MCA', 'Any Graduate'],
            ARRAY['Bangalore', 'Mumbai', 'Hyderabad', 'Pune', 'Remote'],
            true,
            false,
            ARRAY['remote', 'hybrid', 'office'],
            ARRAY['Python', 'Java', 'JavaScript', 'React', 'Node.js', 'AWS', 'Data Science', 'Machine Learning'],
            ARRAY[]::text[],
            ARRAY['hiring', 'opening', 'position', 'job', 'role', 'opportunity'],
            ARRAY['looking for job', 'need a job', 'searching for', 'anyone hiring', 'job alert', 'urgent requirement for client', 'bench sales'],
            70,
            50,
            24,
            false,
            true
        );
    """)


def downgrade():
    op.drop_index('idx_job_prefs_active_unique', table_name='job_scraping_preferences')
    op.drop_table('job_scraping_preferences')
