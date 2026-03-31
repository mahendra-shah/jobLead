"""Fix job_category string default

Revision ID: fix_job_category_default
Revises: conv_email_job_cat
Create Date: 2026-03-18 13:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = 'fix_job_category_default'
down_revision = 'conv_email_job_cat'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("ALTER TABLE students ALTER COLUMN job_category TYPE VARCHAR(100) USING job_category::text"))
    op.execute(sa.text("ALTER TABLE students ALTER COLUMN job_category DROP DEFAULT"))
    op.execute(sa.text("""
        UPDATE students
        SET job_category = NULL
        WHERE job_category IN ('[]', '{}', 'null', '')
    """))


def downgrade() -> None:
    op.execute(sa.text("ALTER TABLE students ALTER COLUMN job_category SET DEFAULT '[]'"))
