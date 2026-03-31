"""add discovery_sources table for Phase 1 discovery

Revision ID: add_discovery_sources
Revises: add_all_missing_cols
Create Date: 2026-03-02 10:00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'add_discovery_sources'
down_revision = 'add_all_missing_cols'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'discovery_sources',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(500), nullable=False),
        sa.Column('url', sa.Text(), nullable=False),
        sa.Column('source_type', sa.String(50), nullable=False),
        sa.Column('platform', sa.String(50), nullable=True),
        sa.Column('city', sa.String(100), nullable=True),
        sa.Column('region', sa.String(100), nullable=True),
        sa.Column('country_code', sa.String(10), nullable=True),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('phase', sa.Integer(), nullable=False),
        sa.Column('is_shortlisted', sa.Boolean(), nullable=False),
        sa.Column('discovered_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_discovery_sources_city'), 'discovery_sources', ['city'], unique=False)
    op.create_index(op.f('ix_discovery_sources_phase'), 'discovery_sources', ['phase'], unique=False)
    op.create_index(op.f('ix_discovery_sources_source_type'), 'discovery_sources', ['source_type'], unique=False)
    op.create_index(op.f('ix_discovery_sources_url'), 'discovery_sources', ['url'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_discovery_sources_url'), table_name='discovery_sources')
    op.drop_index(op.f('ix_discovery_sources_source_type'), table_name='discovery_sources')
    op.drop_index(op.f('ix_discovery_sources_phase'), table_name='discovery_sources')
    op.drop_index(op.f('ix_discovery_sources_city'), table_name='discovery_sources')
    op.drop_table('discovery_sources')
