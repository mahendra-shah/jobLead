"""Ensure jobs.experience column exists

Revision ID: add_missing_jobs_experience
Revises: drop_job_quality_dedup_cols
Create Date: 2026-03-23 17:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "add_missing_jobs_experience"
down_revision = "drop_job_quality_dedup_cols"
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

    if not _column_exists(connection, "jobs", "experience"):
        op.add_column("jobs", sa.Column("experience", sa.String(length=255), nullable=True))

    if _column_exists(connection, "jobs", "experience_required"):
        connection.execute(
            sa.text(
                """
                UPDATE jobs
                SET experience = experience_required
                WHERE experience IS NULL
                  AND experience_required IS NOT NULL
                """
            )
        )


def downgrade() -> None:
    connection = op.get_bind()

    if _column_exists(connection, "jobs", "experience"):
        op.drop_column("jobs", "experience")
