"""Alembic migration script template."""

"""change_last_scraped_by_account_to_uuid

Revision ID: 71596bc8feb4
Revises: 587dbf1d2cf4
Create Date: 2026-02-26 10:58:52.387584+00:00

Change last_scraped_by_account from Integer (1-5) to UUID foreign key
referencing telegram_accounts.id for proper relational integrity.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '71596bc8feb4'
down_revision = '587dbf1d2cf4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Change last_scraped_by_account from Integer to UUID FK."""
    
    # Step 1: Drop existing Integer column (if exists)
    op.drop_column('telegram_groups', 'last_scraped_by_account')
    
    # Step 2: Add new UUID column with foreign key constraint
    op.add_column('telegram_groups',
        sa.Column('last_scraped_by_account', 
                  postgresql.UUID(as_uuid=True), 
                  sa.ForeignKey('telegram_accounts.id', ondelete='SET NULL'),
                  nullable=True)
    )
    
    # Step 3: Create index for performance
    op.create_index(
        'ix_telegram_groups_last_scraped_by_account',
        'telegram_groups',
        ['last_scraped_by_account']
    )
    
    print("✅ Changed last_scraped_by_account to UUID foreign key")
    print("ℹ️  Column will be populated with telegram_account UUIDs during next scrape")


def downgrade() -> None:
    """Revert back to Integer column."""
    # Drop index
    op.drop_index('ix_telegram_groups_last_scraped_by_account', 'telegram_groups')
    
    # Drop UUID column
    op.drop_column('telegram_groups', 'last_scraped_by_account')
    
    # Restore Integer column
    op.add_column('telegram_groups',
        sa.Column('last_scraped_by_account', sa.Integer(), nullable=True)
    )
