"""Drop student fields: user_id, college_name, college_id, cgpa, percentage, current_address, internship_details, internships, projects, first_name, last_name, branch

Revision ID: drop_student_fields
Revises: f3a8c2d1b9e4
Create Date: 2026-03-18

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'drop_student_fields'
down_revision = 'f3a8c2d1b9e4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Remove unused fields from students table:
    - user_id (foreign key - Student now identified by direct id)
    - college_name (no longer needed)
    - college_id (no longer needed)
    - cgpa (CGPA removed from requirements)
    - percentage (branch percentage removed from requirements)
    - current_address (address removed from requirements)
    - internship_details (internship tracking removed)
    - internships (internship tracking removed)
    - projects (project tracking removed)
    - first_name (removed from requirements)
    - last_name (removed from requirements)
    - branch (removed from requirements)
    """
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = {
        column["name"] for column in inspector.get_columns("students")
    }

    links_target_column = None
    if "social_links" in existing_columns:
        links_target_column = "social_links"
    elif "tech_links" in existing_columns:
        links_target_column = "tech_links"

    if links_target_column is not None:
        legacy_tech_link_updates = [
            "github_profile",
            "linkedin_profile",
            "portfolio_url",
            "coding_platforms",
        ]

        for field in legacy_tech_link_updates:
            if field not in existing_columns:
                continue

            if field == "coding_platforms":
                where_clause = (
                    f"{field} IS NOT NULL AND {field} <> '{{}}'::jsonb"
                )
            else:
                where_clause = (
                    f"{field} IS NOT NULL AND btrim({field}) <> ''"
                )

            op.execute(
                sa.text(
                    f"""
                    UPDATE students
                    SET {links_target_column} =
                        COALESCE({links_target_column}, '{{}}'::jsonb)
                        || jsonb_build_object('{field}', {field})
                    WHERE {where_clause}
                    """
                )
            )

    fields_to_drop = [
        'user_id',
        'college_name',
        'college_id',
        'cgpa',
        'percentage',
        'current_address',
        'internship_details',
        'internships',
        'projects',
        'first_name',
        'last_name',
        'branch',
        'github_profile',
        'linkedin_profile',
        'portfolio_url',
        'coding_platforms',
    ]
    
    for field in fields_to_drop:
        if field in existing_columns:
            op.drop_column('students', field)


def downgrade() -> None:
    """
    Restore removed fields - not recommended for production.
    This is provided for development/testing rollback only.
    """
    # Add columns back in reverse order
    op.add_column('students', sa.Column('coding_platforms', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('students', sa.Column('portfolio_url', sa.String(length=500), nullable=True))
    op.add_column('students', sa.Column('linkedin_profile', sa.String(length=500), nullable=True))
    op.add_column('students', sa.Column('github_profile', sa.String(length=500), nullable=True))
    op.add_column('students', sa.Column('branch', sa.String(length=100), nullable=True))
    op.add_column('students', sa.Column('last_name', sa.String(length=100), nullable=True))
    op.add_column('students', sa.Column('first_name', sa.String(length=100), nullable=True))
    op.add_column('students', sa.Column('projects', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('students', sa.Column('internships', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('students', sa.Column('internship_details', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('students', sa.Column('current_address', sa.Text(), nullable=True))
    op.add_column('students', sa.Column('percentage', sa.Float(), nullable=True))
    op.add_column('students', sa.Column('cgpa', sa.Float(), nullable=True))
    op.add_column('students', sa.Column('college_id', sa.Integer(), nullable=True))
    op.add_column('students', sa.Column('college_name', sa.String(length=200), nullable=True))
    op.add_column('students', sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False, unique=True))
    op.create_foreign_key('students_user_id_fkey', 'students', 'users', ['user_id'], ['id'])
