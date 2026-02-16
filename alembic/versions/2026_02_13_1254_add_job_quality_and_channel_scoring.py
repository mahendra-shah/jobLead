"""add job quality and channel scoring fields

Revision ID: 2026_02_13_1254
Revises: 001b9d08d6b4
Create Date: 2026-02-13 12:54:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '2026_02_13_1254'
down_revision = '001b9d08d6b4'
branch_labels = None
depends_on = None


def upgrade():
    # Add quality scoring fields to jobs table
    op.add_column('jobs', sa.Column('quality_score', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('relevance_score', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('extraction_completeness_score', sa.Float(), nullable=True))
    op.add_column('jobs', sa.Column('meets_relevance_criteria', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('jobs', sa.Column('quality_breakdown', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('jobs', sa.Column('relevance_reasons', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('jobs', sa.Column('quality_factors', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('jobs', sa.Column('quality_scored_at', sa.DateTime(timezone=True), nullable=True))
    
    # Create indexes for performance
    op.create_index('ix_jobs_quality_score', 'jobs', ['quality_score'])
    op.create_index('ix_jobs_meets_relevance_criteria', 'jobs', ['meets_relevance_criteria'])
    
    # Add enhanced scoring fields to telegram_groups table
    op.add_column('telegram_groups', sa.Column('health_score_breakdown', sa.Text(), nullable=True))
    op.add_column('telegram_groups', sa.Column('relevant_jobs_count', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('telegram_groups', sa.Column('total_jobs_posted', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('telegram_groups', sa.Column('relevance_ratio', sa.Float(), nullable=True))
    op.add_column('telegram_groups', sa.Column('avg_job_quality_score', sa.Float(), nullable=True))
    op.add_column('telegram_groups', sa.Column('status_label', sa.String(length=50), nullable=False, server_default='active'))
    op.add_column('telegram_groups', sa.Column('last_score_update', sa.DateTime(timezone=True), nullable=True))
    op.add_column('telegram_groups', sa.Column('score_history', sa.Text(), nullable=True))


def downgrade():
    # Remove telegram_groups enhancements
    op.drop_column('telegram_groups', 'score_history')
    op.drop_column('telegram_groups', 'last_score_update')
    op.drop_column('telegram_groups', 'status_label')
    op.drop_column('telegram_groups', 'avg_job_quality_score')
    op.drop_column('telegram_groups', 'relevance_ratio')
    op.drop_column('telegram_groups', 'total_jobs_posted')
    op.drop_column('telegram_groups', 'relevant_jobs_count')
    op.drop_column('telegram_groups', 'health_score_breakdown')
    
    # Remove job quality scoring fields
    op.drop_index('ix_jobs_meets_relevance_criteria', table_name='jobs')
    op.drop_index('ix_jobs_quality_score', table_name='jobs')
    op.drop_column('jobs', 'quality_scored_at')
    op.drop_column('jobs', 'quality_factors')
    op.drop_column('jobs', 'relevance_reasons')
    op.drop_column('jobs', 'quality_breakdown')
    op.drop_column('jobs', 'meets_relevance_criteria')
    op.drop_column('jobs', 'extraction_completeness_score')
    op.drop_column('jobs', 'relevance_score')
    op.drop_column('jobs', 'quality_score')
