"""drop_last_scraped_by_account_from_telegram_groups

Revision ID: b2c3d4e5f6a7
Revises: b2c3d4e5f6a7
Create Date: 2026-04-09

"""

revision = 'b2c3d4e5f6a7'
down_revision = 'c4045c41dbba'
branch_labels = None
depends_on = None
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.drop_column('telegram_groups', 'last_scraped_by_account')

def downgrade():
    op.add_column('telegram_groups', sa.Column('last_scraped_by_account', sa.UUID(), nullable=True))
    op.create_index('ix_telegram_groups_last_scraped_by_account', 'telegram_groups', ['last_scraped_by_account'])
    op.create_foreign_key('telegram_groups_last_scraped_by_account_fkey', 'telegram_groups', 'telegram_accounts', ['last_scraped_by_account'], ['id'], ondelete='SET NULL')
