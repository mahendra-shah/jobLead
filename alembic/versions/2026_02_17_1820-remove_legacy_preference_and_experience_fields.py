"""remove_legacy_preference_and_experience_fields

Remove duplicate and legacy fields after consolidation to JSONB

Revision ID: remove_legacy_fields
Revises: consolidate_prefs_jsonb
Create Date: 2026-02-17 18:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'remove_legacy_fields'
down_revision = 'consolidate_prefs_jsonb'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Remove duplicate legacy fields from students table:
    - skills (duplicate of technical_skills and soft_skills)
    - preferences (duplicate of preference JSONB)
    - experience (duplicate of internship_details)
    - job_type, work_mode, preferred_job_role, preferred_location, expected_salary (consolidated to preference)
    """
    conn = op.get_bind()
    
    # Fields to drop
    fields_to_drop = [
        'skills',
        'preferences', 
        'experience',
        'job_type',
        'work_mode',
        'preferred_job_role',
        'preferred_location',
        'expected_salary',
    ]
    
    # Drop each field if it exists
    for field in fields_to_drop:
        try:
            op.drop_column('students', field)
            print(f"✓ Dropped column: {field}")
        except Exception as e:
            print(f"Note: Could not drop {field}: {str(e)}")


def downgrade() -> None:
    """
    Restore legacy fields - not recommended for production
    """
    from sqlalchemy.dialects import postgresql
    
    # Restore fields in reverse order
    op.add_column('students', sa.Column('expected_salary', sa.Integer(), nullable=True))
    op.add_column('students', sa.Column('preferred_location', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('students', sa.Column('preferred_job_role', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('students', sa.Column('work_mode', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('students', sa.Column('job_type', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('students', sa.Column('experience', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('students', sa.Column('preferences', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='{}'))
    op.add_column('students', sa.Column('skills', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
