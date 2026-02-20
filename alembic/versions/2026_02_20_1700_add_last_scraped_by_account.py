"""add_last_scraped_by_account_to_telegram_groups

Revision ID: add_last_scraped_by_acct
Revises: 34e1b392ea09
Create Date: 2026-02-20 17:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_last_scraped_by_acct'
down_revision = '34e1b392ea09'  # The merge migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add last_scraped_by_account column to telegram_groups (as integer, not UUID)
    op.add_column('telegram_groups',
        sa.Column('last_scraped_by_account', sa.Integer(), nullable=True)
    )
    
    print("✅ Added last_scraped_by_account column to telegram_groups table")


def downgrade() -> None:
    # Remove the column if migration is rolled back
    op.drop_column('telegram_groups', 'last_scraped_by_account')
