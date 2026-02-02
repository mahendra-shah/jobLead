#!/usr/bin/env python3
"""
Pipeline Health Monitor
=======================
Checks the health of the job processing pipeline and reports any issues.

Usage:
    python scripts/monitor_pipeline.py

Exit Codes:
    0 = All healthy
    1 = Warning (some issues but not critical)
    2 = Critical (pipeline broken)
"""

import sys
from datetime import datetime, timedelta
from pymongo import MongoClient
from sqlalchemy import text, select
from app.db.session import get_sync_db
from app.models.job import Job
import os
from dotenv import load_dotenv

load_dotenv()


def check_mongodb():
    """Check MongoDB connection and recent activity"""
    try:
        mongo_uri = os.getenv("MONGODB_URI")
        client = MongoClient(mongo_uri)
        db = client.placement_db  # Changed from placement_scraper
        
        # Test connection
        client.admin.command('ping')
        
        # Count messages
        total_messages = db.raw_messages.count_documents({})
        
        # Check recent messages (last 24 hours)
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_messages = db.raw_messages.count_documents({
            "fetched_at": {"$gte": yesterday}
        })
        
        # Check classified messages (last 7 days)
        last_week = datetime.utcnow() - timedelta(days=7)
        classified = db.raw_messages.count_documents({
            "classification.is_job": True,
            "fetched_at": {"$gte": last_week}
        })
        
        status = {
            "healthy": True,
            "total_messages": total_messages,
            "recent_messages": recent_messages,
            "classified_last_week": classified,
            "warning": None
        }
        
        # Warnings
        if recent_messages == 0:
            status["warning"] = "⚠️  No messages scraped in last 24 hours"
            status["healthy"] = False
        elif recent_messages < 10:
            status["warning"] = f"⚠️  Only {recent_messages} messages scraped in last 24 hours (usually 100+)"
        
        return status
        
    except Exception as e:
        return {
            "healthy": False,
            "error": f"MongoDB Error: {str(e)}"
        }


def check_postgresql():
    """Check PostgreSQL connection and recent jobs"""
    try:
        db = next(get_sync_db())
        
        # Total jobs
        total_jobs = db.execute(text("SELECT COUNT(*) FROM jobs")).scalar()
        
        # Jobs created today
        today = datetime.now().date()
        jobs_today = db.execute(
            text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) = :today"),
            {"today": today}
        ).scalar()
        
        # Jobs created yesterday
        yesterday = today - timedelta(days=1)
        jobs_yesterday = db.execute(
            text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) = :yesterday"),
            {"yesterday": yesterday}
        ).scalar()
        
        # Jobs last 7 days
        last_week = today - timedelta(days=7)
        jobs_last_week = db.execute(
            text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) >= :last_week"),
            {"last_week": last_week}
        ).scalar()
        
        # Latest job
        latest_job = db.execute(
            select(Job.created_at).order_by(Job.created_at.desc()).limit(1)
        ).scalar_one_or_none()
        
        status = {
            "healthy": True,
            "total_jobs": total_jobs,
            "jobs_today": jobs_today,
            "jobs_yesterday": jobs_yesterday,
            "jobs_last_week": jobs_last_week,
            "latest_job": latest_job.isoformat() if latest_job else None,
            "warning": None
        }
        
        # Warnings
        if latest_job:
            hours_since_last = (datetime.now() - latest_job.replace(tzinfo=None)).total_seconds() / 3600
            if hours_since_last > 48:
                status["warning"] = f"⚠️  No jobs created in {int(hours_since_last)} hours"
                status["healthy"] = False
            elif hours_since_last > 24:
                status["warning"] = f"⚠️  Last job created {int(hours_since_last)} hours ago"
        else:
            status["warning"] = "❌ No jobs in database"
            status["healthy"] = False
        
        return status
        
    except Exception as e:
        return {
            "healthy": False,
            "error": f"PostgreSQL Error: {str(e)}"
        }


def check_gap():
    """Check for gaps between MongoDB classifications and PostgreSQL jobs"""
    try:
        # MongoDB classified as jobs (last 7 days)
        mongo_uri = os.getenv("MONGODB_URI")
        client = MongoClient(mongo_uri)
        db = client.placement_db  # Changed from placement_scraper
        
        last_week = datetime.utcnow() - timedelta(days=7)
        classified = db.raw_messages.count_documents({
            "classification.is_job": True,
            "fetched_at": {"$gte": last_week}
        })
        
        # PostgreSQL jobs created (last 7 days)
        pg_db = next(get_sync_db())
        last_week_date = datetime.now().date() - timedelta(days=7)
        jobs_created = pg_db.execute(
            text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) >= :last_week"),
            {"last_week": last_week_date}
        ).scalar()
        
        gap = classified - jobs_created
        
        status = {
            "healthy": True,
            "classified_last_week": classified,
            "jobs_created_last_week": jobs_created,
            "gap": gap,
            "warning": None
        }
        
        # Check for gaps
        if gap > 50:
            status["warning"] = f"⚠️  Large gap: {gap} classified messages not converted to jobs"
            status["healthy"] = False
        elif gap > 10:
            status["warning"] = f"⚠️  Small gap: {gap} classified messages not converted to jobs"
        
        return status
        
    except Exception as e:
        return {
            "healthy": False,
            "error": f"Gap Check Error: {str(e)}"
        }


def print_status(label, status, indent=0):
    """Print status with formatting"""
    prefix = "  " * indent
    
    if "error" in status:
        print(f"{prefix}❌ {label}: {status['error']}")
        return False
    
    if status.get("healthy"):
        print(f"{prefix}✅ {label}")
    else:
        print(f"{prefix}⚠️  {label}")
    
    # Print details
    for key, value in status.items():
        if key not in ["healthy", "warning", "error"]:
            print(f"{prefix}   {key}: {value}")
    
    # Print warning
    if status.get("warning"):
        print(f"{prefix}   {status['warning']}")
    
    return status.get("healthy", False)


def main():
    """Run all health checks"""
    print("\n" + "="*70)
    print("🏥 PIPELINE HEALTH CHECK")
    print("="*70)
    print(f"Timestamp: {datetime.now().isoformat()}\n")
    
    # Run checks
    mongo_status = check_mongodb()
    pg_status = check_postgresql()
    gap_status = check_gap()
    
    # Print results
    mongo_ok = print_status("MongoDB", mongo_status)
    print()
    pg_ok = print_status("PostgreSQL", pg_status)
    print()
    gap_ok = print_status("Gap Check", gap_status)
    
    # Overall status
    print("\n" + "="*70)
    
    if mongo_ok and pg_ok and gap_ok:
        print("✅ OVERALL STATUS: HEALTHY")
        print("="*70)
        return 0
    elif not mongo_ok or not pg_ok:
        print("❌ OVERALL STATUS: CRITICAL")
        print("="*70)
        print("\n⚠️  ACTION REQUIRED:")
        if not mongo_ok:
            print("   - Check Lambda scraper (AWS CloudWatch)")
            print("   - Verify MongoDB connection")
        if not pg_ok:
            print("   - Check ML processor (run scripts/run_ml_pipeline.py)")
            print("   - Verify PostgreSQL connection")
        return 2
    else:
        print("⚠️  OVERALL STATUS: WARNING")
        print("="*70)
        print("\n💡 RECOMMENDATIONS:")
        if not gap_ok:
            print("   - Run ML processor: python scripts/run_ml_pipeline.py")
            print("   - Check logs for errors")
            print("   - Review classification quality")
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
