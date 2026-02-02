"""
Monitoring API Router
====================
Provides API endpoints for pipeline monitoring and health checks.

Endpoints:
- GET /api/monitoring/pipeline-health?days=1
- GET /api/monitoring/daily-stats?days=7
- GET /api/monitoring/health
"""

from fastapi import APIRouter, Query
from datetime import datetime, timedelta
from pymongo import MongoClient
from sqlalchemy import text
from app.db.session import get_sync_db
import os

router = APIRouter(prefix="/api/monitoring", tags=["monitoring"])


def get_mongodb_client():
    """Get MongoDB client"""
    mongo_uri = os.getenv("MONGODB_URI")
    return MongoClient(mongo_uri)


def check_mongodb_stats(days: int = 1):
    """Check MongoDB statistics for given days"""
    try:
        client = get_mongodb_client()
        db = client.placement_db
        
        stats = []
        for i in range(days):
            date = datetime.utcnow() - timedelta(days=i)
            start_of_day = datetime(date.year, date.month, date.day, 0, 0, 0)
            end_of_day = start_of_day + timedelta(days=1)
            
            # Messages scraped that day
            messages_scraped = db.raw_messages.count_documents({
                "fetched_at": {"$gte": start_of_day, "$lt": end_of_day}
            })
            
            # Messages classified as jobs
            jobs_classified = db.raw_messages.count_documents({
                "fetched_at": {"$gte": start_of_day, "$lt": end_of_day},
                "classification.is_job": True
            })
            
            stats.append({
                "date": start_of_day.strftime("%Y-%m-%d"),
                "messages_scraped": messages_scraped,
                "jobs_classified": jobs_classified,
                "classification_rate": round(jobs_classified / messages_scraped * 100, 2) if messages_scraped > 0 else 0
            })
        
        return {
            "status": "healthy",
            "total_messages": db.raw_messages.count_documents({}),
            "daily_stats": stats
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


def check_postgresql_stats(days: int = 1):
    """Check PostgreSQL statistics for given days"""
    try:
        db = next(get_sync_db())
        
        stats = []
        for i in range(days):
            date = datetime.now().date() - timedelta(days=i)
            
            # Jobs created that day
            jobs_created = db.execute(
                text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) = :date"),
                {"date": date}
            ).scalar()
            
            # Active jobs
            active_jobs = db.execute(
                text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) = :date AND is_active = true"),
                {"date": date}
            ).scalar()
            
            stats.append({
                "date": date.strftime("%Y-%m-%d"),
                "jobs_created": jobs_created,
                "active_jobs": active_jobs
            })
        
        # Total stats
        total_jobs = db.execute(text("SELECT COUNT(*) FROM jobs")).scalar()
        active_total = db.execute(text("SELECT COUNT(*) FROM jobs WHERE is_active = true")).scalar()
        
        # Latest job
        latest_job = db.execute(
            text("SELECT created_at FROM jobs ORDER BY created_at DESC LIMIT 1")
        ).scalar()
        
        return {
            "status": "healthy",
            "total_jobs": total_jobs,
            "active_jobs": active_total,
            "latest_job": latest_job.isoformat() if latest_job else None,
            "daily_stats": stats
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


def check_gap_stats(days: int = 1):
    """Check gap between classified and created jobs"""
    try:
        client = get_mongodb_client()
        db_mongo = client.placement_db
        db_pg = next(get_sync_db())
        
        stats = []
        for i in range(days):
            date = datetime.utcnow() - timedelta(days=i)
            start_of_day = datetime(date.year, date.month, date.day, 0, 0, 0)
            end_of_day = start_of_day + timedelta(days=1)
            
            # MongoDB classified
            classified = db_mongo.raw_messages.count_documents({
                "fetched_at": {"$gte": start_of_day, "$lt": end_of_day},
                "classification.is_job": True
            })
            
            # PostgreSQL created
            pg_date = date.date()
            created = db_pg.execute(
                text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) = :date"),
                {"date": pg_date}
            ).scalar()
            
            gap = classified - created
            gap_percentage = round(gap / classified * 100, 2) if classified > 0 else 0
            
            stats.append({
                "date": start_of_day.strftime("%Y-%m-%d"),
                "classified": classified,
                "created": created,
                "gap": gap,
                "gap_percentage": gap_percentage,
                "status": "healthy" if abs(gap) < 10 else "warning" if abs(gap) < 50 else "critical"
            })
        
        return {
            "status": "healthy",
            "daily_stats": stats
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@router.get("/pipeline-health")
async def get_pipeline_health(
    days: int = Query(default=1, ge=1, le=7, description="Number of days to retrieve (max 7, default 1)")
):
    """
    Get pipeline health statistics for the last N days (max 7)
    
    **Parameters:**
    - days: Number of days to retrieve (1-7, default: 1)
    
    **Returns:**
    - MongoDB stats (messages scraped, jobs classified)
    - PostgreSQL stats (jobs created, active jobs)
    - Gap analysis (discrepancies between classified and created)
    - Overall health status
    """
    
    mongodb_stats = check_mongodb_stats(days)
    postgresql_stats = check_postgresql_stats(days)
    gap_stats = check_gap_stats(days)
    
    # Determine overall health
    overall_status = "healthy"
    warnings = []
    
    # Check MongoDB
    if mongodb_stats["status"] == "error":
        overall_status = "critical"
        warnings.append(f"MongoDB error: {mongodb_stats.get('error')}")
    elif mongodb_stats["daily_stats"] and mongodb_stats["daily_stats"][0]["messages_scraped"] == 0:
        overall_status = "critical"
        warnings.append("No messages scraped in last 24 hours")
    
    # Check PostgreSQL
    if postgresql_stats["status"] == "error":
        overall_status = "critical"
        warnings.append(f"PostgreSQL error: {postgresql_stats.get('error')}")
    elif postgresql_stats["latest_job"]:
        latest_job_time = datetime.fromisoformat(postgresql_stats["latest_job"])
        hours_since = (datetime.now() - latest_job_time.replace(tzinfo=None)).total_seconds() / 3600
        if hours_since > 48:
            overall_status = "critical"
            warnings.append(f"No jobs created in {int(hours_since)} hours")
        elif hours_since > 24 and overall_status != "critical":
            overall_status = "warning"
            warnings.append(f"Last job created {int(hours_since)} hours ago")
    
    # Check gaps
    if gap_stats["status"] != "error":
        for day_stat in gap_stats["daily_stats"]:
            if day_stat["status"] == "critical" and overall_status != "critical":
                overall_status = "critical"
                warnings.append(f"Large gap on {day_stat['date']}: {day_stat['gap']} jobs")
            elif day_stat["status"] == "warning" and overall_status == "healthy":
                overall_status = "warning"
    
    return {
        "timestamp": datetime.now().isoformat(),
        "days_requested": days,
        "overall_status": overall_status,
        "warnings": warnings,
        "mongodb": mongodb_stats,
        "postgresql": postgresql_stats,
        "gap_analysis": gap_stats
    }


@router.get("/daily-stats")
async def get_daily_stats(
    days: int = Query(default=7, ge=1, le=7, description="Number of days to retrieve (max 7, default 7)")
):
    """
    Get simplified daily statistics for the last N days
    
    **Parameters:**
    - days: Number of days to retrieve (1-7, default: 7)
    
    **Returns:**
    - Daily breakdown of messages, classifications, and job creations
    """
    
    client = get_mongodb_client()
    db_mongo = client.placement_db
    db_pg = next(get_sync_db())
    
    daily_data = []
    
    for i in range(days):
        date = datetime.utcnow() - timedelta(days=i)
        start_of_day = datetime(date.year, date.month, date.day, 0, 0, 0)
        end_of_day = start_of_day + timedelta(days=1)
        
        # MongoDB data
        messages_scraped = db_mongo.raw_messages.count_documents({
            "fetched_at": {"$gte": start_of_day, "$lt": end_of_day}
        })
        
        jobs_classified = db_mongo.raw_messages.count_documents({
            "fetched_at": {"$gte": start_of_day, "$lt": end_of_day},
            "classification.is_job": True
        })
        
        # PostgreSQL data
        pg_date = date.date()
        jobs_created = db_pg.execute(
            text("SELECT COUNT(*) FROM jobs WHERE DATE(created_at) = :date"),
            {"date": pg_date}
        ).scalar()
        
        daily_data.append({
            "date": start_of_day.strftime("%Y-%m-%d"),
            "day_of_week": start_of_day.strftime("%A"),
            "messages_scraped": messages_scraped,
            "jobs_classified": jobs_classified,
            "jobs_created": jobs_created,
            "classification_rate": f"{round(jobs_classified / messages_scraped * 100, 1)}%" if messages_scraped > 0 else "0%",
            "conversion_rate": f"{round(jobs_created / jobs_classified * 100, 1)}%" if jobs_classified > 0 else "0%"
        })
    
    return {
        "timestamp": datetime.now().isoformat(),
        "days": days,
        "daily_stats": daily_data,
        "summary": {
            "total_messages": sum(d["messages_scraped"] for d in daily_data),
            "total_classified": sum(d["jobs_classified"] for d in daily_data),
            "total_created": sum(d["jobs_created"] for d in daily_data),
            "avg_messages_per_day": round(sum(d["messages_scraped"] for d in daily_data) / days, 1),
            "avg_jobs_per_day": round(sum(d["jobs_created"] for d in daily_data) / days, 1)
        }
    }


@router.get("/health")
async def health_check():
    """
    Simple health check endpoint
    
    Returns basic status without heavy queries
    """
    try:
        # Quick MongoDB check
        client = get_mongodb_client()
        client.admin.command('ping')
        mongo_status = "ok"
    except Exception as e:
        mongo_status = f"error: {str(e)}"
    
    try:
        # Quick PostgreSQL check
        db = next(get_sync_db())
        db.execute(text("SELECT 1"))
        pg_status = "ok"
    except Exception as e:
        pg_status = f"error: {str(e)}"
    
    overall_status = "healthy" if mongo_status == "ok" and pg_status == "ok" else "unhealthy"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now().isoformat(),
        "components": {
            "mongodb": mongo_status,
            "postgresql": pg_status
        }
    }
