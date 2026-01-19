"""Add telegram scraping infrastructure

Revision ID: telegram_scraping_v1
Revises: 6a598231698e
Create Date: 2026-01-13 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'telegram_scraping_v1'
down_revision: Union[str, None] = '6a598231698e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable pgvector extension
    # Temporarily disabled due to version mismatch - can be added later
    # op.execute('CREATE EXTENSION IF NOT EXISTS vector')
    
    # Create telegram_accounts table
    op.create_table(
        'telegram_accounts',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('phone', sa.String(length=20), nullable=False),
        sa.Column('api_id', sa.String(length=50), nullable=False),
        sa.Column('api_hash', sa.String(length=100), nullable=False),
        sa.Column('session_string', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('is_banned', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('groups_joined_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_join_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('phone')
    )
    op.create_index(op.f('ix_telegram_accounts_phone'), 'telegram_accounts', ['phone'])
    
    # Create telegram_groups table
    op.create_table(
        'telegram_groups',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('username', sa.String(length=255), nullable=False),
        sa.Column('title', sa.String(length=500), nullable=True),
        sa.Column('category', sa.String(length=100), nullable=True),
        sa.Column('members_count', sa.Integer(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_joined', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('joined_by_account_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('joined_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_scraped_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_message_id', sa.String(length=50), nullable=True),
        sa.Column('last_message_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('messages_fetched_total', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('health_score', sa.Float(), nullable=False, server_default='100.0'),
        sa.Column('total_messages_scraped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('job_messages_found', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('quality_jobs_found', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_job_posted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('deactivated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('deactivation_reason', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('username')
    )
    op.create_index(op.f('ix_telegram_groups_username'), 'telegram_groups', ['username'])
    op.create_index(op.f('ix_telegram_groups_health_score'), 'telegram_groups', ['health_score'])
    op.create_index(op.f('ix_telegram_groups_is_active'), 'telegram_groups', ['is_active'])
    
    # Create raw_telegram_messages table
    op.create_table(
        'raw_telegram_messages',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('message_id', sa.BigInteger(), nullable=False),
        sa.Column('group_username', sa.String(length=255), nullable=False),
        sa.Column('group_title', sa.String(length=500), nullable=True),
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('sender_id', sa.BigInteger(), nullable=True),
        sa.Column('sender_username', sa.String(length=255), nullable=True),
        sa.Column('message_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column('has_media', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('media_type', sa.String(length=50), nullable=True),
        sa.Column('fetched_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('processed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('processing_status', sa.String(length=50), nullable=False, server_default='pending'),
        sa.Column('processing_error', sa.Text(), nullable=True),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('message_id', 'group_username', name='uq_message_id_group')
    )
    op.create_index(op.f('ix_raw_telegram_messages_group_username'), 'raw_telegram_messages', ['group_username'])
    op.create_index(op.f('ix_raw_telegram_messages_processed'), 'raw_telegram_messages', ['processed'])
    op.create_index(op.f('ix_raw_telegram_messages_processing_status'), 'raw_telegram_messages', ['processing_status'])
    
    # Create scraping_logs table
    op.create_table(
        'scraping_logs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('lambda_function', sa.String(length=100), nullable=False),
        sa.Column('execution_id', sa.String(length=255), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('duration_seconds', sa.Float(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='running'),
        sa.Column('accounts_used', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('groups_processed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('messages_fetched', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('jobs_extracted', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('duplicates_found', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('errors_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('errors', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('cost_estimate', sa.Float(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_scraping_logs_lambda_function'), 'scraping_logs', ['lambda_function'])
    op.create_index(op.f('ix_scraping_logs_started_at'), 'scraping_logs', ['started_at'])
    op.create_index(op.f('ix_scraping_logs_status'), 'scraping_logs', ['status'])
    
    # Add embedding columns to jobs table using pgvector
    op.add_column('jobs', sa.Column('embedding', postgresql.ARRAY(sa.Float()), nullable=True))
    op.add_column('jobs', sa.Column('content_hash', sa.String(length=32), nullable=True))
    op.add_column('jobs', sa.Column('source_message_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index(op.f('ix_jobs_content_hash'), 'jobs', ['content_hash'])
    
    # Add embedding column to students table
    op.add_column('students', sa.Column('profile_embedding', postgresql.ARRAY(sa.Float()), nullable=True))


def downgrade() -> None:
    # Drop indexes first
    op.drop_index(op.f('ix_jobs_content_hash'), table_name='jobs')
    
    # Drop columns from jobs
    op.drop_column('jobs', 'source_message_id')
    op.drop_column('jobs', 'content_hash')
    op.drop_column('jobs', 'embedding')
    
    # Drop column from students
    op.drop_column('students', 'profile_embedding')
    
    # Drop scraping_logs
    op.drop_index(op.f('ix_scraping_logs_status'), table_name='scraping_logs')
    op.drop_index(op.f('ix_scraping_logs_started_at'), table_name='scraping_logs')
    op.drop_index(op.f('ix_scraping_logs_lambda_function'), table_name='scraping_logs')
    op.drop_table('scraping_logs')
    
    # Drop raw_telegram_messages
    op.drop_index(op.f('ix_raw_telegram_messages_processing_status'), table_name='raw_telegram_messages')
    op.drop_index(op.f('ix_raw_telegram_messages_processed'), table_name='raw_telegram_messages')
    op.drop_index(op.f('ix_raw_telegram_messages_group_username'), table_name='raw_telegram_messages')
    op.drop_table('raw_telegram_messages')
    
    # Drop telegram_groups
    op.drop_index(op.f('ix_telegram_groups_is_active'), table_name='telegram_groups')
    op.drop_index(op.f('ix_telegram_groups_health_score'), table_name='telegram_groups')
    op.drop_index(op.f('ix_telegram_groups_username'), table_name='telegram_groups')
    op.drop_table('telegram_groups')
    
    # Drop telegram_accounts
    op.drop_index(op.f('ix_telegram_accounts_phone'), table_name='telegram_accounts')
    op.drop_table('telegram_accounts')
    
    # Drop pgvector extension
    op.execute('DROP EXTENSION IF EXISTS vector')
