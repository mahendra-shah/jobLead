"""Alembic migration script template."""

"""add_fetched_by_account_to_jobs

Revision ID: 1a953d4a5d01
Revises: add_telegram_metadata
Create Date: 2026-01-22 16:32:08.226984+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1a953d4a5d01'
down_revision = 'add_telegram_metadata'
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
