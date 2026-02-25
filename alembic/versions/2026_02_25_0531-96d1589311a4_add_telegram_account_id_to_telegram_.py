"""add telegram_account_id to telegram_groups

Adds telegram_account_id (UUID FK) to replace joined_by_account_id (Integer).
This allows proper foreign key relationship to telegram_accounts table.

Revision ID: 96d1589311a4
Revises: change_joined_to_int
Create Date: 2026-02-25 05:31:03.516945+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers, used by Alembic.
revision = '96d1589311a4'
down_revision = 'change_joined_to_int'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Add telegram_account_id column to telegram_groups table.
    
    NOTE: Before running this migration:
    1. Run update_telegram_accounts_mapping.py to populate telegram_accounts
    2. Script will populate telegram_account_id values automatically
    3. Then run this migration to add FK constraint
    """
    # Add new telegram_account_id column (UUID)
    op.execute("""
        ALTER TABLE telegram_groups 
        ADD COLUMN IF NOT EXISTS telegram_account_id UUID
    """)
    
    # Add foreign key constraint to telegram_accounts table
    op.execute("""
        ALTER TABLE telegram_groups 
        ADD CONSTRAINT fk_telegram_groups_account_id 
        FOREIGN KEY (telegram_account_id) 
        REFERENCES telegram_accounts(id) 
        ON DELETE SET NULL
    """)
    
    # Create index for better query performance
    op.create_index(
        'ix_telegram_groups_telegram_account_id',
        'telegram_groups',
        ['telegram_account_id'],
        unique=False,
        if_not_exists=True
    )
    
    print("✅ Added telegram_account_id column and FK to telegram_groups")
    print("⚠️  Keep joined_by_account_id for reference until data is verified")


def downgrade() -> None:
    """Remove telegram_account_id column and FK."""
    # Drop index
    op.drop_index('ix_telegram_groups_telegram_account_id', table_name='telegram_groups', if_exists=True)
    
    # Drop FK constraint
    op.execute("""
        ALTER TABLE telegram_groups 
        DROP CONSTRAINT IF EXISTS fk_telegram_groups_account_id
    """)
    
    # Drop column
    op.drop_column('telegram_groups', 'telegram_account_id')
    
    print("✅ Removed telegram_account_id from telegram_groups")
