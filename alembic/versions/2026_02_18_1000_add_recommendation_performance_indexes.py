"""add_recommendation_performance_indexes

Revision ID: a1b2c3d4e5f6
Revises: add_all_missing_student_columns
Create Date: 2026-02-18 10:00:00.000000+00:00

Critical performance indexes for job recommendation system:
- Composite indexes for recommendation eligibility filtering
- GIN indexes for JSONB array operations (skills, students_shown_to)
- Covering indexes for common query patterns
- Student profile indexes for fast lookups

Expected impact: 30-50x faster recommendations (5-15s → 50-300ms)
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'remove_unused_fields'  # Last applied migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Add comprehensive indexes for recommendation system performance.
    
    Strategy:
    1. Composite indexes for main recommendation query filters
    2. GIN indexes for JSONB array containment checks
    3. Individual indexes for high-frequency filters
    4. Covering indexes to avoid table lookups
    """
    
    # ============================================================================
    # JOBS TABLE INDEXES - CRITICAL FOR RECOMMENDATION PERFORMANCE
    # ============================================================================
    
    # 1. MAIN RECOMMENDATION ELIGIBILITY INDEX (Composite)
    # Covers: is_active = TRUE AND created_at >= cutoff AND visibility_mode checks
    # This is THE most critical index for recommendations
    op.create_index(
        'idx_jobs_recommendation_eligible',
        'jobs',
        ['is_active', 'created_at', 'visibility_mode'],
        postgresql_using='btree',
        postgresql_where=sa.text("is_active = TRUE")
    )
    
    # 2. ACTIVE + QUALITY SCORE INDEX
    # For pre-filtering high-quality jobs (quality_score >= 50)
    op.create_index(
        'idx_jobs_active_quality',
        'jobs',
        ['is_active', 'quality_score'],
        postgresql_using='btree',
        postgresql_where=sa.text("is_active = TRUE AND quality_score >= 50")
    )
    
    # 3. ACTIVE + RECENT JOBS INDEX (Covering)
    # Includes commonly selected columns to avoid table lookups
    op.create_index(
        'idx_jobs_active_recent_covering',
        'jobs',
        ['is_active', 'created_at', 'id', 'quality_score'],
        postgresql_using='btree',
        postgresql_where=sa.text("is_active = TRUE")
    )
    
    # 4. STUDENTS_SHOWN_TO GIN INDEX
    # Critical for fast "WHERE student_id != ALL(students_shown_to)" checks
    # GIN indexes are perfect for JSONB array containment queries
    op.create_index(
        'idx_jobs_students_shown_to_gin',
        'jobs',
        ['students_shown_to'],
        postgresql_using='gin'
    )
    
    # 5. SKILLS_REQUIRED GIN INDEX
    # For fast skill matching in recommendations
    op.create_index(
        'idx_jobs_skills_required_gin',
        'jobs',
        ['skills_required'],
        postgresql_using='gin'
    )
    
    # 6. LOCATION + WORK_TYPE INDEX
    # Common filter combination for remote/onsite filtering
    op.create_index(
        'idx_jobs_location_work_type',
        'jobs',
        ['location', 'work_type', 'is_active'],
        postgresql_using='btree',
        postgresql_where=sa.text("is_active = TRUE")
    )
    
    # 7. EXPERIENCE RANGE INDEX
    # For matching student experience against job requirements
    # Note: Using IF NOT EXISTS to handle case where index already exists
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_experience_range 
        ON jobs USING btree (experience_min, experience_max, is_active) 
        WHERE is_active = TRUE
    """)
    
    # 8. IS_FRESHER + ACTIVE INDEX
    # Fast filtering for fresher-specific jobs
    op.create_index(
        'idx_jobs_fresher_active',
        'jobs',
        ['is_fresher', 'is_active', 'created_at'],
        postgresql_using='btree',
        postgresql_where=sa.text("is_active = TRUE AND is_fresher = TRUE")
    )
    
    # ============================================================================
    # STUDENTS TABLE INDEXES - FOR FAST PROFILE LOOKUPS
    # ============================================================================
    
    # 9. TECHNICAL_SKILLS GIN INDEX
    # Fast skill matching for students
    op.create_index(
        'idx_students_technical_skills_gin',
        'students',
        ['technical_skills'],
        postgresql_using='gin'
    )
    
    # 10. SOFT_SKILLS GIN INDEX
    op.create_index(
        'idx_students_soft_skills_gin',
        'students',
        ['soft_skills'],
        postgresql_using='gin'
    )
    
    # 11. PREFERENCE JSONB GIN INDEX
    # For querying preference fields (job_type, work_mode, location, etc.)
    op.create_index(
        'idx_students_preference_gin',
        'students',
        ['preference'],
        postgresql_using='gin'
    )
    
    # 12. USER_ID INDEX (if not already exists)
    # Fast join with users table
    op.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE tablename = 'students' 
                AND indexname = 'idx_students_user_id'
            ) THEN
                CREATE INDEX idx_students_user_id ON students(user_id);
            END IF;
        END $$;
    """)
    
    # ============================================================================
    # SAVED_JOBS TABLE INDEXES - EXCLUDE SAVED JOBS IN RECOMMENDATIONS
    # ============================================================================
    
    # 13. USER + JOB COMPOSITE INDEX
    # For fast "WHERE job_id NOT IN (saved_jobs)" checks
    # Note: saved_jobs uses user_id, not student_id
    op.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE tablename = 'saved_jobs' 
                AND indexname = 'idx_saved_jobs_user_job'
            ) THEN
                CREATE UNIQUE INDEX idx_saved_jobs_user_job 
                ON saved_jobs(user_id, job_id);
            END IF;
        END $$;
    """)
    
    # ============================================================================
    # APPLICATIONS TABLE INDEXES - EXCLUDE APPLIED JOBS IN RECOMMENDATIONS
    # ============================================================================
    
    # 14. STUDENT + JOB + STATUS COMPOSITE INDEX
    # For fast "WHERE job_id NOT IN (applications)" checks
    op.create_index(
        'idx_applications_student_job_status',
        'applications',
        ['student_id', 'job_id', 'status'],
        postgresql_using='btree'
    )
    
    # ============================================================================
    # JOB_VIEWS TABLE INDEXES - TRACK VIEW HISTORY
    # ============================================================================
    
    # 15. STUDENT + JOB COMPOSITE INDEX
    # For fast "WHERE job_id NOT IN (viewed_jobs)" checks
    op.create_index(
        'idx_job_views_student_job',
        'job_views',
        ['student_id', 'job_id'],
        postgresql_using='btree'
    )
    
    # 16. VIEWED_AT INDEX
    # For recent view history queries
    op.create_index(
        'idx_job_views_viewed_at',
        'job_views',
        ['viewed_at'],
        postgresql_using='btree'
    )
    
    # ============================================================================
    # ANALYZE TABLES FOR QUERY PLANNER
    # ============================================================================
    
    # Update table statistics so PostgreSQL query planner can use new indexes
    op.execute("ANALYZE jobs")
    op.execute("ANALYZE students")
    op.execute("ANALYZE saved_jobs")
    op.execute("ANALYZE applications")
    op.execute("ANALYZE job_views")
    
    print("✅ All recommendation performance indexes created successfully!")
    print("📊 Expected improvement: 5-15 seconds → 50-300ms (30-50x faster)")


def downgrade() -> None:
    """Remove all recommendation performance indexes."""
    
    # Jobs table indexes
    op.drop_index('idx_jobs_recommendation_eligible', table_name='jobs')
    op.drop_index('idx_jobs_active_quality', table_name='jobs')
    op.drop_index('idx_jobs_active_recent_covering', table_name='jobs')
    op.drop_index('idx_jobs_students_shown_to_gin', table_name='jobs')
    op.drop_index('idx_jobs_skills_required_gin', table_name='jobs')
    op.drop_index('idx_jobs_location_work_type', table_name='jobs')
    op.drop_index('idx_jobs_experience_range', table_name='jobs')
    op.drop_index('idx_jobs_fresher_active', table_name='jobs')
    
    # Students table indexes
    op.drop_index('idx_students_technical_skills_gin', table_name='students')
    op.drop_index('idx_students_soft_skills_gin', table_name='students')
    op.drop_index('idx_students_preference_gin', table_name='students')
    op.execute("DROP INDEX IF EXISTS idx_students_user_id")
    
    # Saved jobs indexes
    op.drop_index('idx_saved_jobs_student_job', table_name='saved_jobs')
    
    # Applications indexes
    op.drop_index('idx_applications_student_job_status', table_name='applications')
    
    # Job views indexes
    op.drop_index('idx_job_views_student_job', table_name='job_views')
    op.drop_index('idx_job_views_viewed_at', table_name='job_views')
    
    print("🔄 All recommendation performance indexes removed")
