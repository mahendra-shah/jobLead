"""Drop legacy student education columns after moving to extra_detail

Revision ID: drop_student_edu_cols
Revises: rename_personal_to_extra_detail
Create Date: 2026-03-20 20:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = 'drop_student_edu_cols'
down_revision = 'rename_personal_to_extra_detail'
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

    has_extra_detail = _column_exists(connection, 'students', 'extra_detail')
    has_highest_qualification = _column_exists(connection, 'students', 'highest_qualification')
    has_course = _column_exists(connection, 'students', 'course')
    has_passing_year = _column_exists(connection, 'students', 'passing_year')

    if has_extra_detail and (has_highest_qualification or has_course or has_passing_year):
        json_fields = []
        if has_highest_qualification:
            json_fields.append("'highest_qualification', NULLIF(btrim(highest_qualification), '')")
        if has_course:
            json_fields.append("'course', NULLIF(btrim(course), '')")
        if has_passing_year:
            json_fields.append("'passing_year', passing_year")

        op.execute(
            sa.text(
                f"""
                UPDATE students
                SET extra_detail = COALESCE(extra_detail, '{{}}'::jsonb)
                    || jsonb_strip_nulls(jsonb_build_object({', '.join(json_fields)}))
                """
            )
        )

    for column_name in ['highest_qualification', 'course', 'passing_year', 'personal_detail']:
        if _column_exists(connection, 'students', column_name):
            op.drop_column('students', column_name)


def downgrade() -> None:
    connection = op.get_bind()

    if not _column_exists(connection, 'students', 'highest_qualification'):
        op.add_column('students', sa.Column('highest_qualification', sa.String(length=100), nullable=True))
    if not _column_exists(connection, 'students', 'course'):
        op.add_column('students', sa.Column('course', sa.String(length=100), nullable=True))
    if not _column_exists(connection, 'students', 'passing_year'):
        op.add_column('students', sa.Column('passing_year', sa.Integer(), nullable=True))

    if _column_exists(connection, 'students', 'extra_detail'):
        op.execute(
            sa.text(
                """
                UPDATE students
                SET highest_qualification = COALESCE(highest_qualification, NULLIF(extra_detail->>'highest_qualification', '')),
                    course = COALESCE(course, NULLIF(extra_detail->>'course', '')),
                    passing_year = COALESCE(passing_year, NULLIF(extra_detail->>'passing_year', '')::int)
                """
            )
        )
