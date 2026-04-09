"""Alembic migration script template."""

"""merge all heads

Revision ID: c4045c41dbba
Revises: add_discovery_sources, drop_jobs_minmax_exp_salary
Create Date: 2026-04-09 05:20:52.670096+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c4045c41dbba'
down_revision = ('add_discovery_sources', 'drop_jobs_minmax_exp_salary')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
