"""
Direct migration script for Neon database
Adds the new stats columns and indexes to the jobs table
Run from project root: python -m scripts.migrate_neon
"""
import sys
import asyncio
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.db.session import AsyncSessionLocal


async def run_migration():
    """Run migration directly on Neon database"""
    print("🔄 Starting Neon Database Migration")
    print("=" * 50)
    
    async with AsyncSessionLocal() as db:
        try:
            # 1. Check current state
            print("\n📊 Checking current table structure...")
            result = await db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'jobs' 
                AND column_name IN ('is_fresher', 'work_type', 'experience_min', 'experience_max', 'salary_min', 'salary_max')
            """))
            existing_columns = [row[0] for row in result.fetchall()]
            
            if len(existing_columns) == 6:
                print("✅ All columns already exist! No migration needed.")
                return
            
            print(f"Found {len(existing_columns)} of 6 columns. Starting migration...")
            
            # 2. Add new columns
            print("\n📝 Adding new columns...")
            
            if 'is_fresher' not in existing_columns:
                await db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS is_fresher BOOLEAN"))
                print("  ✅ Added is_fresher column")
            
            if 'work_type' not in existing_columns:
                await db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS work_type VARCHAR(50)"))
                print("  ✅ Added work_type column")
            
            if 'experience_min' not in existing_columns:
                await db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS experience_min INTEGER"))
                print("  ✅ Added experience_min column")
            
            if 'experience_max' not in existing_columns:
                await db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS experience_max INTEGER"))
                print("  ✅ Added experience_max column")
            
            if 'salary_min' not in existing_columns:
                await db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS salary_min DECIMAL(12, 2)"))
                print("  ✅ Added salary_min column")
            
            if 'salary_max' not in existing_columns:
                await db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS salary_max DECIMAL(12, 2)"))
                print("  ✅ Added salary_max column")
            
            # 3. Create indexes
            print("\n🔍 Creating indexes...")
            
            await db.execute(text("CREATE INDEX IF NOT EXISTS idx_jobs_is_fresher ON jobs(is_fresher)"))
            print("  ✅ Created idx_jobs_is_fresher")
            
            await db.execute(text("CREATE INDEX IF NOT EXISTS idx_jobs_work_type ON jobs(work_type)"))
            print("  ✅ Created idx_jobs_work_type")
            
            await db.execute(text("CREATE INDEX IF NOT EXISTS idx_jobs_experience_range ON jobs(experience_min, experience_max)"))
            print("  ✅ Created idx_jobs_experience_range")
            
            await db.execute(text("CREATE INDEX IF NOT EXISTS idx_jobs_salary_range ON jobs(salary_min, salary_max)"))
            print("  ✅ Created idx_jobs_salary_range")
            
            # 4. Commit changes
            await db.commit()
            print("\n✅ Migration completed successfully!")
            
            # 5. Verify
            print("\n🔍 Verifying migration...")
            result = await db.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'jobs' 
                AND column_name IN ('is_fresher', 'work_type', 'experience_min', 'experience_max', 'salary_min', 'salary_max')
                ORDER BY column_name
            """))
            columns = result.fetchall()
            print(f"\nFound {len(columns)} columns:")
            for col in columns:
                print(f"  ✅ {col[0]}: {col[1]}")
            
        except Exception as e:
            print(f"\n❌ Migration failed: {e}")
            await db.rollback()
            raise


if __name__ == "__main__":
    asyncio.run(run_migration())
