"""Drop extra legacy job fields

Revision ID: drop_job_extra_legacy_cols
Revises: rename_student_social_links
Create Date: 2026-03-19 17:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = 'drop_job_extra_legacy_cols'
down_revision = 'rename_student_social_links'
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


def upgrade() -> None:
    connection = op.get_bind()

    if _index_exists(connection, 'ix_jobs_content_hash'):
        op.drop_index('ix_jobs_content_hash', table_name='jobs')

    # Safe/idempotent cleanup of extra legacy columns
    for col in [
        'experience_required',
        'salary_range',
        'raw_text',
        'content_hash',
        'salary_currency',
    ]:
        if _column_exists(connection, 'jobs', col):
            op.drop_column('jobs', col)


def downgrade() -> None:
    connection = op.get_bind()

    if not _column_exists(connection, 'jobs', 'experience_required'):
        op.add_column('jobs', sa.Column('experience_required', sa.String(length=50), nullable=True))
    if not _column_exists(connection, 'jobs', 'salary_range'):
        op.add_column('jobs', sa.Column('salary_range', sa.JSON(), nullable=True))
    if not _column_exists(connection, 'jobs', 'raw_text'):
        op.add_column('jobs', sa.Column('raw_text', sa.Text(), nullable=True))
    if not _column_exists(connection, 'jobs', 'content_hash'):
        op.add_column('jobs', sa.Column('content_hash', sa.String(length=32), nullable=True))
    if not _column_exists(connection, 'jobs', 'salary_currency'):
        op.add_column('jobs', sa.Column('salary_currency', sa.String(length=3), nullable=True, server_default='INR'))

    if not _index_exists(connection, 'ix_jobs_content_hash') and _column_exists(connection, 'jobs', 'content_hash'):
        op.create_index('ix_jobs_content_hash', 'jobs', ['content_hash'])
