#!/usr/bin/env python3
"""
Quick script to add resume_url column to students table
Run this directly: python3 fix_resume_url.py
"""

import asyncio
import asyncpg
from app.config import settings

async def add_resume_url_column():
    """Add resume_url column if it doesn't exist"""
    try:
        # Connect to database
        conn = await asyncpg.connect(
            host=settings.DATABASE_URL.split('@')[1].split('/')[0].split(':')[0] if '@' in settings.DATABASE_URL else 'localhost',
            port=5432,
            user=settings.DATABASE_URL.split('://')[1].split(':')[0] if '://' in settings.DATABASE_URL else 'postgres',
            password=settings.DATABASE_URL.split(':')[2].split('@')[0] if '@' in settings.DATABASE_URL else '',
            database=settings.DATABASE_URL.split('/')[-1] if '/' in settings.DATABASE_URL else 'placement_db'
        )
        
        # Check if column exists
        check_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'students' 
        AND column_name = 'resume_url';
        """
        
        result = await conn.fetch(check_query)
        
        if result:
            print("✓ resume_url column already exists")
        else:
            # Add column
            alter_query = "ALTER TABLE students ADD COLUMN resume_url VARCHAR(500);"
            await conn.execute(alter_query)
            print("✓ Successfully added resume_url column to students table")
        
        await conn.close()
        print("Done!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nPlease run this SQL directly in your database:")
        print("ALTER TABLE students ADD COLUMN IF NOT EXISTS resume_url VARCHAR(500);")

if __name__ == "__main__":
    asyncio.run(add_resume_url_column())

