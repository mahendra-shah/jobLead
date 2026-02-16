-- Direct SQL to add resume_url column
-- Run this in your PostgreSQL database

-- Option 1: Simple ALTER TABLE (PostgreSQL 9.1+)
ALTER TABLE students ADD COLUMN IF NOT EXISTS resume_url VARCHAR(500);

-- Option 2: Check first then add (if IF NOT EXISTS doesn't work)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'students' 
        AND column_name = 'resume_url'
    ) THEN
        ALTER TABLE students ADD COLUMN resume_url VARCHAR(500);
        RAISE NOTICE 'Added resume_url column to students table';
    ELSE
        RAISE NOTICE 'resume_url column already exists';
    END IF;
END $$;

