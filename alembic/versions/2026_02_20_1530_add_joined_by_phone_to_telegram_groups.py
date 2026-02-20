"""add_joined_by_phone_to_telegram_groups

Revision ID: add_joined_by_phone
Revises: add_all_missing_cols
Create Date: 2026-02-20 15:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_joined_by_phone'
down_revision = 'add_all_missing_cols'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add joined_by_phone column to telegram_groups
    op.add_column('telegram_groups',
        sa.Column('joined_by_phone', sa.String(20), nullable=True)
    )
    
    # Update existing joined groups with phone numbers from accounts
    op.execute("""
        UPDATE telegram_groups tg
        SET joined_by_phone = ta.phone
        FROM telegram_accounts ta
        WHERE tg.joined_by_account_id = ta.id
        AND tg.is_joined = true
        AND tg.joined_by_phone IS NULL
    """)


def downgrade() -> None:
    # Remove the column if migration is rolled back
    op.drop_column('telegram_groups', 'joined_by_phone')
