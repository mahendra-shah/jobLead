"""add_skill_required_to_students

Revision ID: add_skill_required
Revises: add_all_missing_cols
Create Date: 2026-02-16 22:51:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'add_skill_required'
down_revision = '1c81cb50c4af'  # Based on the current head
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
    
    # Add skill_required column to students table
    add_column_if_not_exists('students', sa.Column('skill_required', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))


def downgrade() -> None:
    # Check if column exists before dropping
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('students')]
    if 'skill_required' in columns:
        op.drop_column('students', 'skill_required')
        print("Dropped skill_required column from students table")
    else:
        print("skill_required column does not exist in students table")

