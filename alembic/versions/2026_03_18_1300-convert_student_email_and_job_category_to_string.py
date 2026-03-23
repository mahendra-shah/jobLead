"""Convert students.emails to email(unique string) and job_category to string

Revision ID: conv_email_job_cat
Revises: add_student_profile_fields
Create Date: 2026-03-18 13:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = 'conv_email_job_cat'
down_revision = 'add_student_profile_fields'
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
            WHERE schemaname = 'public' AND indexname = :index_name
            LIMIT 1
            """
        ),
        {"index_name": index_name},
    ).fetchone()
    return result is not None


def upgrade() -> None:
    connection = op.get_bind()

    has_email = _column_exists(connection, 'students', 'email')
    has_emails = _column_exists(connection, 'students', 'emails')

    if not has_email:
        op.add_column('students', sa.Column('email', sa.String(length=255), nullable=True))

    # Backfill email from legacy jsonb emails (first string)
    if has_emails:
        op.execute(sa.text("""
            UPDATE students
            SET email = CASE
                WHEN emails IS NULL THEN email
                WHEN jsonb_typeof(emails) = 'string' THEN trim(both '"' from emails::text)
                WHEN jsonb_typeof(emails) = 'array' THEN emails->>0
                ELSE email
            END
            WHERE email IS NULL
        """))

        # Drop legacy emails column
        op.drop_column('students', 'emails')

    # Normalize empty emails
    op.execute(sa.text("""
        UPDATE students
        SET email = NULL
        WHERE email IS NOT NULL AND btrim(email) = ''
    """))

    # Remove duplicate emails before creating unique index (keep first row)
    op.execute(sa.text("""
        WITH ranked AS (
            SELECT id,
                   email,
                   row_number() OVER (
                       PARTITION BY lower(email)
                       ORDER BY created_at NULLS LAST, id
                   ) AS rn
            FROM students
            WHERE email IS NOT NULL AND btrim(email) <> ''
        )
        UPDATE students s
        SET email = NULL
        FROM ranked r
        WHERE s.id = r.id AND r.rn > 1
    """))

    # Unique index for email (case-insensitive, ignoring NULL)
    if not _index_exists(connection, 'uq_students_email_lower'):
        op.create_index(
            'uq_students_email_lower',
            'students',
            [sa.text('lower(email)')],
            unique=True,
            postgresql_where=sa.text('email IS NOT NULL')
        )

    # Convert job_category jsonb -> string
    if _column_exists(connection, 'students', 'job_category'):
        op.execute(sa.text("""
            ALTER TABLE students
            ALTER COLUMN job_category TYPE VARCHAR(100)
            USING (
                CASE
                    WHEN job_category IS NULL THEN NULL
                    WHEN jsonb_typeof(job_category::jsonb) = 'string' THEN trim(both '"' from job_category::text)
                    WHEN jsonb_typeof(job_category::jsonb) = 'array' THEN job_category::jsonb->>0
                    ELSE NULL
                END
            )
        """))


def downgrade() -> None:
    connection = op.get_bind()

    # Convert job_category string -> jsonb array
    if _column_exists(connection, 'students', 'job_category'):
        op.execute(sa.text("""
            ALTER TABLE students
            ALTER COLUMN job_category TYPE JSONB
            USING (
                CASE
                    WHEN job_category IS NULL OR btrim(job_category) = '' THEN '[]'::jsonb
                    ELSE jsonb_build_array(job_category)
                END
            )
        """))

    if _index_exists(connection, 'uq_students_email_lower'):
        op.drop_index('uq_students_email_lower', table_name='students')

    if not _column_exists(connection, 'students', 'emails'):
        op.add_column('students', sa.Column('emails', postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    # backfill emails from email
    if _column_exists(connection, 'students', 'email') and _column_exists(connection, 'students', 'emails'):
        op.execute(sa.text("""
            UPDATE students
            SET emails = CASE
                WHEN email IS NULL OR btrim(email) = '' THEN '[]'::jsonb
                ELSE jsonb_build_array(email)
            END
        """))

    if _column_exists(connection, 'students', 'email'):
        op.drop_column('students', 'email')
