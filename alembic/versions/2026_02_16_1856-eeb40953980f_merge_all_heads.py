"""Alembic migration script template."""

"""merge_all_heads

Revision ID: eeb40953980f
Revises: 1c81cb50c4af, 2026_02_13_1254, fc8a9b2e3d1a, add_all_missing_cols
Create Date: 2026-02-16 18:56:18.304367+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eeb40953980f'
down_revision = ('1c81cb50c4af', '2026_02_13_1254', 'fc8a9b2e3d1a', 'add_all_missing_cols')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
