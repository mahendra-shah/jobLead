"""Rename students.tech_links to social_links

Revision ID: rename_student_social_links
Revises: add_shared_count_to_jobs
Create Date: 2026-03-19 09:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = 'rename_student_social_links'
down_revision = 'add_shared_count_to_jobs'
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


def upgrade() -> None:
    connection = op.get_bind()

    has_tech_links = _column_exists(connection, 'students', 'tech_links')
    has_social_links = _column_exists(connection, 'students', 'social_links')

    if has_tech_links and not has_social_links:
        op.alter_column('students', 'tech_links', new_column_name='social_links')


def downgrade() -> None:
    connection = op.get_bind()

    has_tech_links = _column_exists(connection, 'students', 'tech_links')
    has_social_links = _column_exists(connection, 'students', 'social_links')

    if has_social_links and not has_tech_links:
        op.alter_column('students', 'social_links', new_column_name='tech_links')
