"""Alembic migration script template."""

"""merge joined_by_phone and other heads

Revision ID: 34e1b392ea09
Revises: 5368b08c373d, add_joined_by_phone
Create Date: 2026-02-20 10:32:31.629380+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '34e1b392ea09'
down_revision = ('5368b08c373d', 'add_joined_by_phone')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
