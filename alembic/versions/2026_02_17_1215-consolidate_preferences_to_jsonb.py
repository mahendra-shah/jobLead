"""consolidate_preferences_to_jsonb

Consolidate job preferences from flat fields to single JSONB column

Revision ID: consolidate_prefs_jsonb
Revises: merge_heads_2026_02_17
Create Date: 2026-02-17 12:15:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'consolidate_prefs_jsonb'
down_revision = 'merge_heads_2026_02_17'  # Based on the latest merge migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add the new preference JSONB column if it doesn't exist
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('students')]
    
    if 'preference' not in columns:
        op.add_column('students', sa.Column('preference', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='{}'))
        print("Added 'preference' JSONB column to students table")
    else:
        print("'preference' JSONB column already exists in students table")


def downgrade() -> None:
    # Drop the preference column if we need to rollback
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('students')]
    
    if 'preference' in columns:
        op.drop_column('students', 'preference')
        print("Dropped 'preference' JSONB column from students table")
