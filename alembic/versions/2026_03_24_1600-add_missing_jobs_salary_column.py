"""Ensure jobs.salary column exists

Revision ID: add_missing_jobs_salary
Revises: add_missing_jobs_experience
Create Date: 2026-03-24 16:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "add_missing_jobs_salary"
down_revision = "add_missing_jobs_experience"
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

    if not _column_exists(connection, "jobs", "salary"):
        op.add_column("jobs", sa.Column("salary", sa.String(length=255), nullable=True))


def downgrade() -> None:
    connection = op.get_bind()

    if _column_exists(connection, "jobs", "salary"):
        op.drop_column("jobs", "salary")
