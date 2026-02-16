"""Alembic migration script template."""

"""make_password_hash_nullable_for_oauth

Revision ID: 0bbb92afbecc
Revises: add_username_to_users
Create Date: 2026-02-16 05:58:03.957058+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0bbb92afbecc'
down_revision = 'add_username_to_users'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Make password_hash nullable to support OAuth users
    op.alter_column('users', 'password_hash',
                    existing_type=sa.String(length=255),
                    nullable=True)


def downgrade() -> None:
    # Revert password_hash to NOT NULL
    # Before altering the column, ensure there are no NULL values. If NULLs
    # exist (e.g., for OAuth-only users), fail deterministically with a
    # clear error instead of relying on the database to reject the ALTER.
    bind = op.get_bind()
    null_count = bind.execute(
        sa.text("SELECT COUNT(*) FROM users WHERE password_hash IS NULL")
    ).scalar()

    if null_count and null_count > 0:
        raise RuntimeError(
            f"Cannot downgrade migration 0bbb92afbecc: found {null_count} "
            "user(s) with NULL password_hash. Set a non-NULL sentinel value "
            "or remove those users before retrying the downgrade."
        )

    op.alter_column(
        'users',
        'password_hash',
        existing_type=sa.String(length=255),
        nullable=False,
    )
