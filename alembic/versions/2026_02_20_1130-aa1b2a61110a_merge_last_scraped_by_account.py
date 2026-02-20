"""Alembic migration script template."""

"""merge last_scraped_by_account

Revision ID: aa1b2a61110a
Revises: optimize_not_exists_2026_02_19, add_last_scraped_by_acct
Create Date: 2026-02-20 11:30:23.922201+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa1b2a61110a'
down_revision = ('optimize_not_exists_2026_02_19', 'add_last_scraped_by_acct')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
