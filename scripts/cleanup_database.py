#!/usr/bin/env python3
"""
PostgreSQL Data Cleanup Script
===============================
Automatically deletes old data based on retention policies:
- Jobs older than 30 days (is_active=false)
- Applications older than 90 days (status=rejected/withdrawn)

Usage:
    python scripts/cleanup_database.py

Cron Setup:
    # Daily at 2 AM
    0 2 * * * ubuntu cd /home/ubuntu/placementdashboard-be && .venv/bin/python scripts/cleanup_database.py >> /home/ubuntu/logs/cleanup.log 2>&1
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import get_sync_db

load_dotenv()


def cleanup_old_jobs(days=30):
    """Delete inactive jobs older than specified days"""
    try:
        db = next(get_sync_db())
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Count jobs to delete
        count_query = text("""
            SELECT COUNT(*) 
            FROM jobs 
            WHERE is_active = false 
            AND created_at < :cutoff_date
        """)
        count = db.execute(count_query, {"cutoff_date": cutoff_date}).scalar()
        
        if count == 0:
            print(f"✅ No inactive jobs older than {days} days found.")
            return 0
        
        # Delete old inactive jobs
        delete_query = text("""
            DELETE FROM jobs 
            WHERE is_active = false 
            AND created_at < :cutoff_date
        """)
        db.execute(delete_query, {"cutoff_date": cutoff_date})
        db.commit()
        
        print(f"✅ Deleted {count} inactive jobs older than {days} days.")
        return count
        
    except Exception as e:
        print(f"❌ Error cleaning up jobs: {str(e)}")
        db.rollback()
        return 0


def cleanup_old_applications(days=90):
    """Delete old rejected/withdrawn applications"""
    try:
        db = next(get_sync_db())
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Count applications to delete
        count_query = text("""
            SELECT COUNT(*) 
            FROM applications 
            WHERE status IN ('rejected', 'withdrawn') 
            AND created_at < :cutoff_date
        """)
        count = db.execute(count_query, {"cutoff_date": cutoff_date}).scalar()
        
        if count == 0:
            print(f"✅ No old applications found.")
            return 0
        
        # Delete old applications
        delete_query = text("""
            DELETE FROM applications 
            WHERE status IN ('rejected', 'withdrawn') 
            AND created_at < :cutoff_date
        """)
        db.execute(delete_query, {"cutoff_date": cutoff_date})
        db.commit()
        
        print(f"✅ Deleted {count} old applications (rejected/withdrawn) older than {days} days.")
        return count
        
    except Exception as e:
        print(f"❌ Error cleaning up applications: {str(e)}")
        db.rollback()
        return 0


def get_database_stats():
    """Get current database statistics"""
    try:
        db = next(get_sync_db())
        
        # Get counts
        total_jobs = db.execute(text("SELECT COUNT(*) FROM jobs")).scalar()
        active_jobs = db.execute(text("SELECT COUNT(*) FROM jobs WHERE is_active = true")).scalar()
        total_applications = db.execute(text("SELECT COUNT(*) FROM applications")).scalar()
        
        # Get oldest records
        oldest_job = db.execute(text("SELECT MIN(created_at) FROM jobs")).scalar()
        oldest_app = db.execute(text("SELECT MIN(created_at) FROM applications")).scalar()
        
        print("\n📊 Database Statistics:")
        print(f"   Total Jobs: {total_jobs}")
        print(f"   Active Jobs: {active_jobs}")
        print(f"   Inactive Jobs: {total_jobs - active_jobs}")
        print(f"   Total Applications: {total_applications}")
        print(f"   Oldest Job: {oldest_job}")
        print(f"   Oldest Application: {oldest_app}")
        print()
        
    except Exception as e:
        print(f"❌ Error getting database stats: {str(e)}")


def main():
    """Run database cleanup"""
    print("\n" + "="*70)
    print("🧹 DATABASE CLEANUP")
    print("="*70)
    print(f"Timestamp: {datetime.now().isoformat()}\n")
    
    # Show current stats
    get_database_stats()
    
    # Cleanup jobs (30 days retention)
    print("🗑️  Cleaning up inactive jobs (30-day retention)...")
    deleted_jobs = cleanup_old_jobs(days=30)
    
    # Cleanup applications (90 days retention)
    print("🗑️  Cleaning up old applications (90-day retention)...")
    deleted_apps = cleanup_old_applications(days=90)
    
    # Show updated stats
    get_database_stats()
    
    # Summary
    print("="*70)
    print("✅ CLEANUP COMPLETE")
    print("="*70)
    print(f"Jobs deleted: {deleted_jobs}")
    print(f"Applications deleted: {deleted_apps}")
    print(f"Total deleted: {deleted_jobs + deleted_apps}")
    print("="*70)
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
