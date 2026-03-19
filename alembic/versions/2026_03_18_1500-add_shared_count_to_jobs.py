"""Add shared_count field to jobs table

Revision ID: add_shared_count_to_jobs
Revises: fix_job_category_default
Create Date: 2026-03-18 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = 'add_shared_count_to_jobs'
down_revision = 'fix_job_category_default'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('jobs', sa.Column('shared_count', sa.Integer(), nullable=False, server_default='0'))
    op.execute(sa.text("UPDATE jobs SET shared_count = 0 WHERE shared_count IS NULL"))


def downgrade() -> None:
    op.drop_column('jobs', 'shared_count')
