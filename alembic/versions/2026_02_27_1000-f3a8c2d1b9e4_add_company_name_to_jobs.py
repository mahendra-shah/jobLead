"""add company_name denormalized column to jobs

Revision ID: f3a8c2d1b9e4
Revises: 71596bc8feb4
Create Date: 2026-02-27 10:00:00.000000

Adds a denormalized ``company_name`` VARCHAR column to the ``jobs`` table
so recommendation queries no longer need to load the ``companies``
relationship (which previously triggered a second SELECT … WHERE id IN (…)
on every cache miss).

The column is backfilled from ``companies.name`` via a single UPDATE join
so all existing jobs immediately have the correct value.  New jobs are
populated at write time by the ML processor service.
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f3a8c2d1b9e4"
down_revision = "71596bc8feb4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add company_name column and backfill from companies table."""

    # 1. Add the column (nullable — some jobs have no company_id)
    op.add_column(
        "jobs",
        sa.Column("company_name", sa.String(500), nullable=True),
    )

    # 2. Backfill from companies.name using a single UPDATE JOIN
    op.execute(
        """
        UPDATE jobs
        SET    company_name = companies.name
        FROM   companies
        WHERE  jobs.company_id = companies.id
          AND  jobs.company_name IS NULL
        """
    )

    # 3. Index for future company-based filtering
    op.create_index(
        "ix_jobs_company_name",
        "jobs",
        ["company_name"],
    )


def downgrade() -> None:
    """Drop company_name column."""
    op.drop_index("ix_jobs_company_name", table_name="jobs")
    op.drop_column("jobs", "company_name")
