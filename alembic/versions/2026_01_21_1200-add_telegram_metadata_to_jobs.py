"""add telegram metadata to jobs

Revision ID: add_telegram_metadata
Revises: add_structured_fields_to_jobs
Create Date: 2026-01-21 12:00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_telegram_metadata'
down_revision = '441d50951a98'  # Previous migration: add_experience_salary_fields
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add telegram_group_id and scraped_by_account_id to jobs table"""
    
    # Add telegram_group_id (FK to telegram_groups)
    op.add_column('jobs', sa.Column('telegram_group_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        'fk_jobs_telegram_group_id',
        'jobs', 'telegram_groups',
        ['telegram_group_id'], ['id'],
        ondelete='SET NULL'
    )
    op.create_index('ix_jobs_telegram_group_id', 'jobs', ['telegram_group_id'])
    
    # Add scraped_by_account_id (FK to telegram_accounts)
    op.add_column('jobs', sa.Column('scraped_by_account_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        'fk_jobs_scraped_by_account_id',
        'jobs', 'telegram_accounts',
        ['scraped_by_account_id'], ['id'],
        ondelete='SET NULL'
    )
    op.create_index('ix_jobs_scraped_by_account_id', 'jobs', ['scraped_by_account_id'])
    
    print("✅ Added telegram_group_id and scraped_by_account_id to jobs table")


def downgrade() -> None:
    """Remove telegram metadata columns"""
    
    op.drop_index('ix_jobs_scraped_by_account_id', 'jobs')
    op.drop_constraint('fk_jobs_scraped_by_account_id', 'jobs', type_='foreignkey')
    op.drop_column('jobs', 'scraped_by_account_id')
    
    op.drop_index('ix_jobs_telegram_group_id', 'jobs')
    op.drop_constraint('fk_jobs_telegram_group_id', 'jobs', type_='foreignkey')
    op.drop_column('jobs', 'telegram_group_id')
