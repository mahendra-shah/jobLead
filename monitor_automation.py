"""
Monitor Automation Results

This script monitors the automation pipeline results in real-time.
Shows messages fetched, jobs processed, and location filtering results.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import select, func, desc
from motor.motor_asyncio import AsyncIOMotorClient

from app.db.session import AsyncSessionLocal
from app.models.job import Job
from app.config import settings

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def get_recent_stats(minutes_ago: int = 30):
    """
    Get statistics from the last N minutes.
    
    Args:
        minutes_ago: Look back this many minutes
    """
    cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_ago)
    
    logger.info("=" * 80)
    logger.info(f"📊 AUTOMATION RESULTS - Last {minutes_ago} Minutes")
    logger.info("=" * 80)
    logger.info(f"Checking data since: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("")
    
    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient(settings.MONGODB_URI)
    mongo_db = mongo_client[settings.MONGODB_DB_NAME]
    raw_messages_collection = mongo_db['raw_messages']
    
    # PostgreSQL session
    async with AsyncSessionLocal() as db:
        try:
            # 1. MongoDB - Raw Messages Fetched
            logger.info("📥 1. RAW MESSAGES FETCHED (MongoDB)")
            logger.info("-" * 80)
            
            total_messages = await raw_messages_collection.count_documents({})
            recent_messages = await raw_messages_collection.count_documents({
                'fetched_at': {'$gte': cutoff_time}
            })
            
            logger.info(f"   Total messages in DB: {total_messages:,}")
            logger.info(f"   New messages (last {minutes_ago}min): {recent_messages}")
            
            if recent_messages > 0:
                # Get channel breakdown
                pipeline = [
                    {'$match': {'fetched_at': {'$gte': cutoff_time}}},
                    {'$group': {
                        '_id': '$channel_name',
                        'count': {'$sum': 1}
                    }},
                    {'$sort': {'count': -1}},
                    {'$limit': 10}
                ]
                
                channel_stats = []
                async for doc in raw_messages_collection.aggregate(pipeline):
                    channel_stats.append(doc)
                
                if channel_stats:
                    logger.info(f"\n   Top Channels (last {minutes_ago}min):")
                    for stat in channel_stats:
                        logger.info(f"      • {stat['_id']}: {stat['count']} messages")
            
            logger.info("")
            
            # 2. PostgreSQL - Jobs Processed
            logger.info("💼 2. JOBS PROCESSED (PostgreSQL)")
            logger.info("-" * 80)
            
            # Total jobs
            total_jobs = await db.execute(
                select(func.count(Job.id))
                .where(Job.is_active == True)
            )
            total_count = total_jobs.scalar()
            
            # Recent jobs
            recent_jobs = await db.execute(
                select(func.count(Job.id))
                .where(Job.is_active == True)
                .where(Job.created_at >= cutoff_time)
            )
            recent_count = recent_jobs.scalar()
            
            logger.info(f"   Total active jobs in DB: {total_count:,}")
            logger.info(f"   New jobs created (last {minutes_ago}min): {recent_count}")
            
            if recent_count > 0:
                # Get recent job samples
                result = await db.execute(
                    select(Job)
                    .where(Job.is_active == True)
                    .where(Job.created_at >= cutoff_time)
                    .order_by(desc(Job.created_at))
                    .limit(5)
                )
                recent_job_samples = result.scalars().all()
                
                logger.info(f"\n   Recent Jobs Sample:")
                for i, job in enumerate(recent_job_samples, 1):
                    logger.info(f"      {i}. {job.title}")
                    logger.info(f"          Location: {job.location or 'N/A'}")
                    logger.info(f"          Quality Score: {job.quality_score:.1f}")
                    logger.info(f"          Created: {job.created_at.strftime('%H:%M:%S')}")
            
            logger.info("")
            
            # 3. Location Filtering Results
            logger.info("🌍 3. LOCATION FILTERING (Recent Jobs)")
            logger.info("-" * 80)
            
            if recent_count > 0:
                # Get location breakdown from recent jobs
                result = await db.execute(
                    select(Job)
                    .where(Job.is_active == True)
                    .where(Job.created_at >= cutoff_time)
                )
                recent_jobs_list = result.scalars().all()
                
                india_remote = 0
                india_hybrid = 0
                india_office = 0
                intl_remote = 0
                intl_onsite = 0
                unspecified = 0
                
                for job in recent_jobs_list:
                    if job.quality_breakdown:
                        import json
                        breakdown = json.loads(job.quality_breakdown)
                        loc_score = breakdown.get('location_compatibility', 60)
                        
                        if loc_score == 100:
                            india_remote += 1
                        elif loc_score == 95:
                            india_hybrid += 1
                        elif loc_score == 90:
                            intl_remote += 1
                        elif loc_score == 70:
                            india_office += 1
                        elif loc_score == 0:
                            intl_onsite += 1
                        else:
                            unspecified += 1
                
                logger.info(f"   ✅ India Remote: {india_remote} jobs (score: 100)")
                logger.info(f"   ✅ India Hybrid: {india_hybrid} jobs (score: 95)")
                logger.info(f"   ✅ International Remote: {intl_remote} jobs (score: 90)")
                logger.info(f"   ✅ India Office: {india_office} jobs (score: 70)")
                logger.info(f"   ❌ International Onsite (Filtered): {intl_onsite} jobs (score: 0)")
                logger.info(f"   ⚪ Unspecified: {unspecified} jobs")
                
                # Show international onsite samples (filtered)
                if intl_onsite > 0:
                    logger.info(f"\n   Filtered International Onsite Jobs:")
                    filtered_count = 0
                    for job in recent_jobs_list:
                        if job.quality_breakdown:
                            breakdown = json.loads(job.quality_breakdown)
                            if breakdown.get('location_compatibility') == 0:
                                filtered_count += 1
                                if filtered_count <= 3:
                                    logger.info(f"      • {job.title}")
                                    logger.info(f"        Location: {job.location}")
                                    logger.info(f"        Quality: {job.quality_score:.1f}")
            else:
                logger.info(f"   No jobs created in last {minutes_ago} minutes")
            
            logger.info("")
            
            # 4. ML Processing Stats
            logger.info("🤖 4. ML PROCESSING")
            logger.info("-" * 80)
            
            if recent_count > 0:
                result = await db.execute(
                    select(func.avg(Job.ml_confidence), func.avg(Job.quality_score))
                    .where(Job.is_active == True)
                    .where(Job.created_at >= cutoff_time)
                )
                avg_ml, avg_quality = result.one()
                
                logger.info(f"   Average ML Confidence: {avg_ml:.1f}%")
                logger.info(f"   Average Quality Score: {avg_quality:.1f}")
            else:
                logger.info(f"   No recent jobs to analyze")
            
            logger.info("")
            logger.info("=" * 80)
            
            # Summary
            if recent_messages > 0 or recent_count > 0:
                logger.info("✅ AUTOMATION IS WORKING!")
                logger.info(f"   • Fetched {recent_messages} new messages")
                logger.info(f"   • Processed {recent_count} new jobs")
                logger.info(f"   • Location filtering active")
            else:
                logger.info("ℹ️  No new activity in the last {minutes_ago} minutes")
                logger.info("   This is normal between scheduled runs (every 4 hours)")
            
            logger.info("=" * 80)
            
        finally:
            mongo_client.close()


async def monitor_continuously(interval_minutes: int = 5, duration_minutes: int = 60):
    """
    Continuously monitor the automation pipeline.
    
    Args:
        interval_minutes: Check every N minutes
        duration_minutes: Monitor for this many minutes total
    """
    logger.info("=" * 80)
    logger.info("🔍 CONTINUOUS MONITORING")
    logger.info("=" * 80)
    logger.info(f"Checking every {interval_minutes} minutes")
    logger.info(f"Total monitoring duration: {duration_minutes} minutes")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    logger.info("")
    
    checks = 0
    max_checks = duration_minutes // interval_minutes
    
    try:
        while checks < max_checks:
            await get_recent_stats(minutes_ago=interval_minutes * 2)
            
            checks += 1
            
            if checks < max_checks:
                logger.info(f"\n⏰ Next check in {interval_minutes} minutes...")
                logger.info(f"   ({checks}/{max_checks} checks completed)")
                logger.info("")
                await asyncio.sleep(interval_minutes * 60)
    
    except KeyboardInterrupt:
        logger.info("\n⚠️  Monitoring stopped by user")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ MONITORING COMPLETE")
    logger.info("=" * 80)


async def main():
    """Main function."""
    import sys
    
    print("\n" + "=" * 80)
    print("AUTOMATION MONITORING TOOL")
    print("=" * 80)
    print("\nOptions:")
    print("  1. Quick check (last 30 minutes)")
    print("  2. Monitor continuously (60 minutes)")
    print("  3. Custom time range")
    print("")
    
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = input("Enter choice (1-3) [default: 1]: ").strip() or "1"
    
    if choice == "1":
        await get_recent_stats(minutes_ago=30)
    elif choice == "2":
        await monitor_continuously(interval_minutes=5, duration_minutes=60)
    elif choice == "3":
        minutes = int(input("Enter minutes to look back: "))
        await get_recent_stats(minutes_ago=minutes)
    else:
        logger.error("Invalid choice")


if __name__ == "__main__":
    asyncio.run(main())
