"""Alembic migration script template."""

"""increase_api_credentials_column_size

Revision ID: 836b082bbd38
Revises: change_joined_to_int
Create Date: 2026-02-24 11:06:52.445954+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '836b082bbd38'
down_revision = 'change_joined_to_int'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Increase api_id and api_hash column sizes to accommodate encrypted values."""
    # Increase api_id from varchar(50) to varchar(255)
    op.alter_column('telegram_accounts', 'api_id',
                    existing_type=sa.String(50),
                    type_=sa.String(255),
                    existing_nullable=False)
    
    # Increase api_hash from varchar(100) to varchar(255)
    op.alter_column('telegram_accounts', 'api_hash',
                    existing_type=sa.String(100),
                    type_=sa.String(255),
                    existing_nullable=False)


def downgrade() -> None:
    """Revert column sizes back to original."""
    # Revert api_id back to varchar(50)
    op.alter_column('telegram_accounts', 'api_id',
                    existing_type=sa.String(255),
                    type_=sa.String(50),
                    existing_nullable=False)
    
    # Revert api_hash back to varchar(100)
    op.alter_column('telegram_accounts', 'api_hash',
                    existing_type=sa.String(255),
                    type_=sa.String(100),
                    existing_nullable=False)
