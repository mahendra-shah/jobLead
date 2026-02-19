"""Alembic migration script template."""

"""merge_performance_indexes

Revision ID: 5368b08c373d
Revises: c18bc9f699d4, a1b2c3d4e5f6
Create Date: 2026-02-18 16:40:48.952667+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5368b08c373d'
down_revision = ('c18bc9f699d4', 'a1b2c3d4e5f6')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
