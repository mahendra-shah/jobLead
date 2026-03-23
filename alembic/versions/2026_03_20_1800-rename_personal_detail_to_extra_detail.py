"""Rename personal_detail to extra_detail and backfill education keys

Revision ID: rename_personal_to_extra_detail
Revises: drop_job_extra_legacy_cols
Create Date: 2026-03-20 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = 'rename_personal_to_extra_detail'
down_revision = 'drop_job_extra_legacy_cols'
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

    has_personal_detail = _column_exists(connection, 'students', 'personal_detail')
    has_extra_detail = _column_exists(connection, 'students', 'extra_detail')

    if has_personal_detail and not has_extra_detail:
        op.alter_column('students', 'personal_detail', new_column_name='extra_detail')
        has_extra_detail = True

    if not has_extra_detail:
        op.add_column(
            'students',
            sa.Column(
                'extra_detail',
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
                server_default=sa.text("'{}'::jsonb"),
            ),
        )
        has_extra_detail = True

    if not has_extra_detail:
        return

    has_highest_qualification = _column_exists(connection, 'students', 'highest_qualification')
    has_course = _column_exists(connection, 'students', 'course')
    has_passing_year = _column_exists(connection, 'students', 'passing_year')

    json_fields = []
    if has_highest_qualification:
        json_fields.append("'highest_qualification', NULLIF(btrim(highest_qualification), '')")
    if has_course:
        json_fields.append("'course', NULLIF(btrim(course), '')")
    if has_passing_year:
        json_fields.append("'passing_year', passing_year")

    if json_fields:
        op.execute(
            sa.text(
                f"""
                UPDATE students
                SET extra_detail = COALESCE(extra_detail, '{{}}'::jsonb)
                    || jsonb_strip_nulls(jsonb_build_object({', '.join(json_fields)}))
                """
            )
        )

    op.execute(sa.text("ALTER TABLE students ALTER COLUMN extra_detail DROP DEFAULT"))


def downgrade() -> None:
    connection = op.get_bind()
    has_personal_detail = _column_exists(connection, 'students', 'personal_detail')
    has_extra_detail = _column_exists(connection, 'students', 'extra_detail')

    if has_extra_detail and not has_personal_detail:
        op.alter_column('students', 'extra_detail', new_column_name='personal_detail')
