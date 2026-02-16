"""add_resume_url_column

Revision ID: add_resume_url_2026
Revises: 0bbb92afbecc
Create Date: 2026-02-20 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_resume_url_2026'
down_revision = '0bbb92afbecc'  # Latest migration: make_password_hash_nullable_for_oauth
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Check if column exists before adding
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('students')]
    
    if 'resume_url' not in columns:
        op.add_column('students', sa.Column('resume_url', sa.String(length=500), nullable=True))
        print("Added resume_url column to students table")
    else:
        print("resume_url column already exists")


def downgrade() -> None:
    # Check if column exists before dropping
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('students')]
    
    if 'resume_url' in columns:
        op.drop_column('students', 'resume_url')
        print("Dropped resume_url column from students table")

