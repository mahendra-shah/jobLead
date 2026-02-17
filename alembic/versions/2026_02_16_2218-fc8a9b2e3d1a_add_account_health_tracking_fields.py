"""add account health tracking fields

Revision ID: fc8a9b2e3d1a
Revises: 
Create Date: 2026-02-16 22:18:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'fc8a9b2e3d1a'
down_revision = None  # Will be set to latest migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add health tracking fields to telegram_accounts table."""
    # Create health_status enum type
    health_status_enum = postgresql.ENUM('healthy', 'degraded', 'banned', name='healthstatus', create_type=True)
    health_status_enum.create(op.get_bind(), checkfirst=True)
    
    # Add new columns
    op.add_column('telegram_accounts', sa.Column('health_status', sa.Enum('healthy', 'degraded', 'banned', name='healthstatus'), nullable=False, server_default='healthy'))
    op.add_column('telegram_accounts', sa.Column('last_successful_fetch_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('telegram_accounts', sa.Column('consecutive_errors', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('telegram_accounts', sa.Column('last_error_message', sa.Text(), nullable=True))
    op.add_column('telegram_accounts', sa.Column('last_error_at', sa.DateTime(timezone=True), nullable=True))
    
    # Create index on health_status for faster queries
    op.create_index('ix_telegram_accounts_health_status', 'telegram_accounts', ['health_status'])


def downgrade() -> None:
    """Remove health tracking fields from telegram_accounts table."""
    # Drop index
    op.drop_index('ix_telegram_accounts_health_status', table_name='telegram_accounts')
    
    # Drop columns
    op.drop_column('telegram_accounts', 'last_error_at')
    op.drop_column('telegram_accounts', 'last_error_message')
    op.drop_column('telegram_accounts', 'consecutive_errors')
    op.drop_column('telegram_accounts', 'last_successful_fetch_at')
    op.drop_column('telegram_accounts', 'health_status')
    
    # Drop enum type
    health_status_enum = postgresql.ENUM('healthy', 'degraded', 'banned', name='healthstatus')
    health_status_enum.drop(op.get_bind(), checkfirst=True)
