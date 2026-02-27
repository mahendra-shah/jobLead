"""Alembic migration script template."""

"""merge_heads

Revision ID: 894bc01aa60b
Revises: 836b082bbd38, 96d1589311a4
Create Date: 2026-02-26 10:28:00.281449+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '894bc01aa60b'
down_revision = ('836b082bbd38', '96d1589311a4')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
