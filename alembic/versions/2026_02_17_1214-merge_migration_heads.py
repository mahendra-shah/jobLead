"""merge migration heads

Revision ID: merge_heads_2026_02_17
Revises: ('add_all_missing_cols', '2026_02_13_1254', '1c81cb50c4af')
Create Date: 2026-02-17 12:14:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'merge_heads_2026_02_17'
down_revision = ('add_all_missing_cols', '2026_02_13_1254', '1c81cb50c4af')  # Merge all three heads
branch_labels = None
depends_on = None


def upgrade() -> None:
    # This is a merge migration - no schema changes needed
    pass


def downgrade() -> None:
    # This is a merge migration - no schema changes needed
    pass

