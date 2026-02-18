"""remove_additional_unused_fields

Remove additional unused legacy fields: degree, education, resume_path, profile_score, profile_embedding

Revision ID: remove_unused_fields
Revises: remove_legacy_fields
Create Date: 2026-02-17 18:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'remove_unused_fields'
down_revision = 'remove_legacy_fields'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Remove additional unused fields from students table:
    - degree (duplicate of course/highest_qualification)
    - education (legacy JSONB field - never used)
    - resume_path (replaced by resume_url for S3)
    - profile_score (not used in API)
    - profile_embedding (pgvector - not used)
    """
    fields_to_drop = [
        'degree',
        'education',
        'resume_path',
        'profile_score',
        'profile_embedding',
    ]
    
    for field in fields_to_drop:
        try:
            op.drop_column('students', field)
            print(f"✓ Dropped column: {field}")
        except Exception as e:
            print(f"Note: Could not drop {field}: {str(e)}")


def downgrade() -> None:
    """
    Restore unused fields - not recommended for production
    """
    op.add_column('students', sa.Column('profile_embedding', sa.Float(), nullable=True))
    op.add_column('students', sa.Column('profile_score', sa.Integer(), nullable=True, server_default='0'))
    op.add_column('students', sa.Column('resume_path', sa.String(500), nullable=True))
    op.add_column('students', sa.Column('education', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('students', sa.Column('degree', sa.String(100), nullable=True))
