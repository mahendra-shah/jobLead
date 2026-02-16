"""add unique constraint to username

Revision ID: add_unique_username_2026
Revises: add_resume_url_2026
Create Date: 2026-02-16 12:47:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_unique_username_2026'
down_revision = 'add_resume_url_2026'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # First, ensure no duplicate usernames exist by appending counter to duplicates
    conn = op.get_bind()
    
    # Find duplicate usernames
    result = conn.execute(sa.text("""
        SELECT username, COUNT(*) as count
        FROM users
        WHERE username IS NOT NULL
        GROUP BY username
        HAVING COUNT(*) > 1
    """))
    
    duplicates = result.fetchall()
    
    # If duplicates exist, rename them
    for username, count in duplicates:
        # Get all user IDs with this username
        users_result = conn.execute(sa.text("""
            SELECT id FROM users 
            WHERE username = :username 
            ORDER BY created_at
        """), {"username": username})
        
        user_ids = [row[0] for row in users_result.fetchall()]
        
        # Keep the first one, rename the rest
        for i, user_id in enumerate(user_ids[1:], start=1):
            new_username = f"{username}_{i}"[:150]
            # Ensure the new username doesn't already exist
            counter = i
            while True:
                check_result = conn.execute(sa.text("""
                    SELECT id FROM users WHERE username = :username
                """), {"username": new_username})
                if check_result.fetchone() is None:
                    break
                counter += 1
                new_username = f"{username}_{counter}"[:150]
            
            # Update the duplicate username
            conn.execute(sa.text("""
                UPDATE users SET username = :new_username WHERE id = :user_id
            """), {"new_username": new_username, "user_id": user_id})
    
    # Now add the unique constraint
    # First drop the existing non-unique index
    op.drop_index('ix_users_username', table_name='users')
    
    # Create a new unique index
    op.create_index('ix_users_username', 'users', ['username'], unique=True)


def downgrade() -> None:
    # Remove unique constraint and recreate as non-unique
    op.drop_index('ix_users_username', table_name='users')
    op.create_index('ix_users_username', 'users', ['username'], unique=False)
