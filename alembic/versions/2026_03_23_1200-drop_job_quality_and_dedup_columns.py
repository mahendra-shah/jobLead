"""Drop legacy job quality detail and dedup columns

Revision ID: drop_job_quality_dedup_cols
Revises: drop_student_edu_cols
Create Date: 2026-03-23 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = 'drop_job_quality_dedup_cols'
down_revision = 'drop_student_edu_cols'
branch_labels = None
depends_on = None


def _column_exists(connection, table_name: str, column_name: str) -> bool:
    result = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = :table_name
              AND column_name = :column_name
            LIMIT 1
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).fetchone()
    return result is not None


def _index_exists(connection, index_name: str) -> bool:
    result = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM pg_indexes
            WHERE indexname = :index_name
            LIMIT 1
            """
        ),
        {"index_name": index_name},
    ).fetchone()
    return result is not None


def _constraint_exists(connection, table_name: str, constraint_name: str) -> bool:
    result = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE table_name = :table_name
              AND constraint_name = :constraint_name
            LIMIT 1
            """
        ),
        {"table_name": table_name, "constraint_name": constraint_name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    connection = op.get_bind()

    if _index_exists(connection, 'ix_jobs_meets_relevance_criteria'):
        op.drop_index('ix_jobs_meets_relevance_criteria', table_name='jobs')

    if _column_exists(connection, 'jobs', 'duplicate_of_id') and _constraint_exists(connection, 'jobs', 'jobs_duplicate_of_id_fkey'):
        op.drop_constraint('jobs_duplicate_of_id_fkey', 'jobs', type_='foreignkey')

    for column_name in [
        'relevance_score',
        'extraction_completeness_score',
        'meets_relevance_criteria',
        'quality_breakdown',
        'relevance_reasons',
        'quality_factors',
        'quality_scored_at',
        'embedding',
        'duplicate_of_id',
    ]:
        if _column_exists(connection, 'jobs', column_name):
            op.drop_column('jobs', column_name)


def downgrade() -> None:
    connection = op.get_bind()

    if not _column_exists(connection, 'jobs', 'relevance_score'):
        op.add_column('jobs', sa.Column('relevance_score', sa.Float(), nullable=True))
    if not _column_exists(connection, 'jobs', 'extraction_completeness_score'):
        op.add_column('jobs', sa.Column('extraction_completeness_score', sa.Float(), nullable=True))
    if not _column_exists(connection, 'jobs', 'meets_relevance_criteria'):
        op.add_column(
            'jobs',
            sa.Column('meets_relevance_criteria', sa.Boolean(), nullable=True, server_default='false'),
        )
    if not _column_exists(connection, 'jobs', 'quality_breakdown'):
        op.add_column('jobs', sa.Column('quality_breakdown', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if not _column_exists(connection, 'jobs', 'relevance_reasons'):
        op.add_column('jobs', sa.Column('relevance_reasons', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if not _column_exists(connection, 'jobs', 'quality_factors'):
        op.add_column('jobs', sa.Column('quality_factors', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    if not _column_exists(connection, 'jobs', 'quality_scored_at'):
        op.add_column('jobs', sa.Column('quality_scored_at', sa.DateTime(timezone=True), nullable=True))
    if not _column_exists(connection, 'jobs', 'embedding'):
        op.add_column('jobs', sa.Column('embedding', sa.String(), nullable=True))
    if not _column_exists(connection, 'jobs', 'duplicate_of_id'):
        op.add_column('jobs', sa.Column('duplicate_of_id', postgresql.UUID(as_uuid=True), nullable=True))

    if _column_exists(connection, 'jobs', 'duplicate_of_id') and not _constraint_exists(connection, 'jobs', 'jobs_duplicate_of_id_fkey'):
        op.create_foreign_key('jobs_duplicate_of_id_fkey', 'jobs', 'jobs', ['duplicate_of_id'], ['id'])

    if _column_exists(connection, 'jobs', 'meets_relevance_criteria') and not _index_exists(connection, 'ix_jobs_meets_relevance_criteria'):
        op.create_index('ix_jobs_meets_relevance_criteria', 'jobs', ['meets_relevance_criteria'])
