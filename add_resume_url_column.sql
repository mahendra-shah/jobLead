-- Add resume_url column to students table if it doesn't exist
-- Run this SQL directly in your database if migration doesn't work

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

