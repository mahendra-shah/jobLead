"""change_joined_by_account_to_integer

Revision ID: change_joined_to_int
Revises: aa1b2a61110a
Create Date: 2026-02-20 17:45:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'change_joined_to_int'
down_revision = 'aa1b2a61110a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Change joined_by_account_id from UUID to Integer.
    This makes it consistent with MongoDB's integer account IDs (1-5).
    """
    
    # Step 1: Add new integer column
    op.add_column('telegram_groups',
        sa.Column('joined_by_account_id_new', sa.Integer(), nullable=True)
    )
    
    # Step 2: Populate new column based on phone mapping
    # We'll set it to NULL for now, will be populated when groups are joined
    op.execute("""
        UPDATE telegram_groups
        SET joined_by_account_id_new = NULL
        WHERE joined_by_account_id IS NOT NULL
    """)
    
    # Step 3: Drop old UUID column
    op.drop_column('telegram_groups', 'joined_by_account_id')
    
    # Step 4: Rename new column to original name
    op.alter_column('telegram_groups', 'joined_by_account_id_new',
        new_column_name='joined_by_account_id'
    )
    
    print("✅ Changed joined_by_account_id from UUID to Integer")
    print("ℹ️  Groups will be re-assigned to accounts (1-5) when next joined/scraped")


def downgrade() -> None:
    """Revert back to UUID type (not recommended)."""
    
    # Add UUID column back
    op.add_column('telegram_groups',
        sa.Column('joined_by_account_id_uuid', postgresql.UUID(as_uuid=True), nullable=True)
    )
    
    # Drop integer column
    op.drop_column('telegram_groups', 'joined_by_account_id')
    
    # Rename UUID column
    op.alter_column('telegram_groups', 'joined_by_account_id_uuid',
        new_column_name='joined_by_account_id'
    )
