"""Alembic migration script template."""

"""add_fetched_by_account_to_jobs

Revision ID: dfed0c74a385
Revises: 1a953d4a5d01
Create Date: 2026-01-22 16:33:09.120597+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dfed0c74a385'
down_revision = '1a953d4a5d01'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add fetched_by_account column to jobs table
    op.add_column('jobs', sa.Column('fetched_by_account', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_jobs_fetched_by_account'), 'jobs', ['fetched_by_account'], unique=False)


def downgrade() -> None:
    # Remove the column
    op.drop_index(op.f('ix_jobs_fetched_by_account'), table_name='jobs')
    op.drop_column('jobs', 'fetched_by_account')
