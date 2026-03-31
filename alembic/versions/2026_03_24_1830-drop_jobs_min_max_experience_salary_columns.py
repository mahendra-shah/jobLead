"""Drop legacy jobs min/max experience and salary columns.

Revision ID: drop_jobs_minmax_exp_salary
Revises: add_missing_jobs_salary
Create Date: 2026-03-24 18:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "drop_jobs_minmax_exp_salary"
down_revision = "add_missing_jobs_salary"
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

    # Drop known legacy indexes first (safe no-op if missing).
    for index_name in [
        "ix_jobs_is_active_work_type_exp",
        "ix_jobs_salary_min",
        "ix_jobs_salary_max",
        "idx_jobs_experience_range",
        "idx_jobs_active_experience",
        "idx_jobs_salary_range",
        "idx_jobs_min_experience",
    ]:
        op.execute(sa.text(f"DROP INDEX IF EXISTS {index_name}"))

    # Support both naming variants seen in migration history.
    for column_name in [
        "experience_min",
        "experience_max",
        "salary_min",
        "salary_max",
        "min_experience",
        "max_experience",
        "min_salary",
        "max_salary",
    ]:
        if _column_exists(connection, "jobs", column_name):
            op.drop_column("jobs", column_name)


def downgrade() -> None:
    connection = op.get_bind()

    if not _column_exists(connection, "jobs", "experience_min"):
        op.add_column("jobs", sa.Column("experience_min", sa.Float(), nullable=True))

    if not _column_exists(connection, "jobs", "experience_max"):
        op.add_column("jobs", sa.Column("experience_max", sa.Float(), nullable=True))

    if not _column_exists(connection, "jobs", "salary_min"):
        op.add_column("jobs", sa.Column("salary_min", sa.Integer(), nullable=True))

    if not _column_exists(connection, "jobs", "salary_max"):
        op.add_column("jobs", sa.Column("salary_max", sa.Integer(), nullable=True))
