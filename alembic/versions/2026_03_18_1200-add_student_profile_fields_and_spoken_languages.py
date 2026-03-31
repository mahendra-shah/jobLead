"""Add student profile fields and rename languages to spoken_languages

Revision ID: add_student_profile_fields
Revises: drop_student_fields
Create Date: 2026-03-18 12:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'add_student_profile_fields'
down_revision = 'drop_student_fields'
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

    # Rename languages -> spoken_languages (if applicable)
    has_languages = _column_exists(connection, 'students', 'languages')
    has_spoken_languages = _column_exists(connection, 'students', 'spoken_languages')

    if has_languages and not has_spoken_languages:
        op.alter_column('students', 'languages', new_column_name='spoken_languages')
    elif not has_spoken_languages:
        op.add_column('students', sa.Column('spoken_languages', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default=sa.text("'[]'::jsonb")))

    # Add new requested columns
    if not _column_exists(connection, 'students', 'skills'):
        op.add_column('students', sa.Column('skills', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default=sa.text("'[]'::jsonb")))

    if not _column_exists(connection, 'students', 'emails'):
        op.add_column('students', sa.Column('emails', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default=sa.text("'[]'::jsonb")))

    if not _column_exists(connection, 'students', 'preferred_job_role'):
        op.add_column('students', sa.Column('preferred_job_role', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default=sa.text("'[]'::jsonb")))

    if not _column_exists(connection, 'students', 'job_category'):
        op.add_column('students', sa.Column('job_category', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default=sa.text("'[]'::jsonb")))

    # Backfill from existing data where possible
    if _column_exists(connection, 'students', 'technical_skills') and _column_exists(connection, 'students', 'skills'):
        op.execute(
            sa.text(
                """
                UPDATE students
                SET skills = technical_skills
                WHERE (skills IS NULL OR skills = '[]'::jsonb)
                  AND technical_skills IS NOT NULL
                  AND technical_skills <> '[]'::jsonb
                """
            )
        )

    if _column_exists(connection, 'students', 'preference'):
        if _column_exists(connection, 'students', 'preferred_job_role'):
            op.execute(
                sa.text(
                    """
                    UPDATE students
                    SET preferred_job_role = COALESCE(preference->'preferred_job_role', '[]'::jsonb)
                    WHERE preference IS NOT NULL
                    """
                )
            )

        if _column_exists(connection, 'students', 'job_category'):
            op.execute(
                sa.text(
                    """
                    UPDATE students
                    SET job_category = COALESCE(preference->'job_category', '[]'::jsonb)
                    WHERE preference IS NOT NULL
                    """
                )
            )


def downgrade() -> None:
    connection = op.get_bind()

    if _column_exists(connection, 'students', 'job_category'):
        op.drop_column('students', 'job_category')

    if _column_exists(connection, 'students', 'preferred_job_role'):
        op.drop_column('students', 'preferred_job_role')

    if _column_exists(connection, 'students', 'emails'):
        op.drop_column('students', 'emails')

    if _column_exists(connection, 'students', 'skills'):
        op.drop_column('students', 'skills')

    has_languages = _column_exists(connection, 'students', 'languages')
    has_spoken_languages = _column_exists(connection, 'students', 'spoken_languages')

    if has_spoken_languages and not has_languages:
        op.alter_column('students', 'spoken_languages', new_column_name='languages')
