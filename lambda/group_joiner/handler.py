"""
Lambda 1: Group Joiner
Joins 2 new groups per account per day with proper delays
"""
import asyncio
import logging
import random
import os
from datetime import datetime
from typing import Dict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
import sys
sys.path.insert(0, '/opt/python')  # Lambda layer path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.models.telegram_account import TelegramAccount
from app.models.telegram_group import TelegramGroup
from app.models.scraping_log import ScrapingLog
from app.services.telegram_service import TelegramService
from app.config import settings


# Database setup
engine = create_async_engine(
    os.environ.get('DATABASE_URL', settings.DATABASE_URL),
    pool_pre_ping=True,
    pool_size=5
)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def is_working_hours() -> bool:
    """Check if current time is within working hours"""
    now = datetime.now()
    start_hour = settings.WORKING_HOURS_START
    end_hour = settings.WORKING_HOURS_END
    
    return start_hour <= now.hour < end_hour


async def get_delay() -> int:
    """Get random delay between min and max"""
    min_delay = settings.MIN_DELAY_BETWEEN_JOINS
    max_delay = settings.MAX_DELAY_BETWEEN_JOINS
    return random.randint(min_delay, max_delay)


async def join_groups(db: AsyncSession, log: ScrapingLog) -> Dict:
    """Main group joining logic"""
    telegram_service = TelegramService(db)
    
    accounts_used = 0
    groups_joined = 0
    errors = []
    
    try:
        # Check working hours
        if not await is_working_hours():
            logger.warning(f"Outside working hours (current hour: {datetime.now().hour})")
            log.notes = f"Skipped: Outside working hours ({settings.WORKING_HOURS_START}-{settings.WORKING_HOURS_END})"
            return {"status": "skipped", "reason": "outside_working_hours"}
        
        # Get unjoined groups
        result = await db.execute(
            select(TelegramGroup).where(
                TelegramGroup.is_joined == False,
                TelegramGroup.is_active == True
            ).order_by(TelegramGroup.created_at.asc()).limit(50)  # Get 50 candidates
        )
        unjoined_groups = result.scalars().all()
        
        if not unjoined_groups:
            logger.info("No unjoined groups available")
            log.notes = "No unjoined groups found"
            return {"status": "success", "groups_joined": 0, "reason": "no_groups"}
        
        logger.info(f"Found {len(unjoined_groups)} unjoined groups")
        
        # Process each account
        for group_idx, group in enumerate(unjoined_groups):
            # Get available account
            account_info = await telegram_service.get_available_account_for_joining()
            
            if not account_info:
                logger.warning("No accounts available for joining more groups today")
                break
            
            account, client = account_info
            accounts_used += 1
            
            # Join group
            logger.info(f"Attempting to join group {group.username} ({group_idx + 1}/{len(unjoined_groups)})")
            
            success = await telegram_service.join_group(group, account, client)
            
            if success:
                groups_joined += 1
                log.groups_processed += 1
                
                logger.info(f"✅ Successfully joined {group.username}")
                
                # Add delay between joins (except for last one)
                if group_idx < len(unjoined_groups) - 1:
                    delay = await get_delay()
                    logger.info(f"Waiting {delay} seconds before next join...")
                    await asyncio.sleep(delay)
                
                # Check if account has reached daily limit
                # If yes, next iteration will get a different account
            else:
                errors.append({
                    "group": group.username,
                    "account": account.phone,
                    "error": "Join failed"
                })
                logger.warning(f"❌ Failed to join {group.username}")
        
        # Update log
        log.accounts_used = accounts_used
        log.groups_processed = groups_joined
        log.errors = errors if errors else None
        log.errors_count = len(errors)
        
        return {
            "status": "success",
            "accounts_used": accounts_used,
            "groups_joined": groups_joined,
            "errors": len(errors)
        }
        
    except Exception as e:
        logger.error(f"Error in join_groups: {e}", exc_info=True)
        log.status = "failed"
        log.errors = [{"error": str(e)}]
        log.errors_count = 1
        raise
    finally:
        await telegram_service.cleanup()


async def main_handler(event: Dict, context) -> Dict:
    """Main Lambda handler"""
    execution_id = context.request_id if context else "local"
    
    logger.info(f"Group Joiner Lambda started (execution_id: {execution_id})")
    
    async with AsyncSessionLocal() as db:
        # Create scraping log
        log = ScrapingLog(
            lambda_function="group_joiner",
            execution_id=execution_id,
            started_at=datetime.now(),
            status="running"
        )
        db.add(log)
        await db.commit()
        
        try:
            # Run main logic
            result = await join_groups(db, log)
            
            # Update log
            log.status = "success"
            log.completed_at = datetime.now()
            log.calculate_duration()
            
            await db.commit()
            
            logger.info(f"Group Joiner completed: {result}")
            
            return {
                "statusCode": 200,
                "body": result
            }
            
        except Exception as e:
            logger.error(f"Lambda execution failed: {e}", exc_info=True)
            
            log.status = "failed"
            log.completed_at = datetime.now()
            log.calculate_duration()
            
            await db.commit()
            
            return {
                "statusCode": 500,
                "body": {"status": "error", "error": str(e)}
            }


def lambda_handler(event, context):
    """AWS Lambda entry point"""
    return asyncio.run(main_handler(event, context))


# For local testing
if __name__ == "__main__":
    class MockContext:
        request_id = "local-test"
    
    result = asyncio.run(main_handler({}, MockContext()))
    print(result)
