"""Alembic migration script template."""

"""remove_telegram_foreign_keys_from_jobs

Revision ID: 001b9d08d6b4
Revises: c97f66a7af18
Create Date: 2026-02-02 09:16:20.170393+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001b9d08d6b4'
down_revision = 'c97f66a7af18'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Remove foreign key constraints that reference telegram tables."""
    
    # Drop foreign key constraints
    with op.batch_alter_table('jobs', schema=None) as batch_op:
        # Drop the telegram_group_id foreign key
        batch_op.drop_constraint('fk_jobs_telegram_group_id', type_='foreignkey')
        
        # Drop the scraped_by_account_id foreign key
        batch_op.drop_constraint('fk_jobs_scraped_by_account_id', type_='foreignkey')


def downgrade() -> None:
    """Re-add the foreign key constraints (for rollback)."""
    
    with op.batch_alter_table('jobs', schema=None) as batch_op:
        batch_op.create_foreign_key(
            'fk_jobs_telegram_group_id',
            'telegram_groups', ['telegram_group_id'], ['id'],
            ondelete='SET NULL'
        )
        batch_op.create_foreign_key(
            'fk_jobs_scraped_by_account_id',
            'telegram_accounts', ['scraped_by_account_id'], ['id'],
            ondelete='SET NULL'
        )
