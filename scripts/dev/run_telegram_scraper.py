#!/usr/bin/env python3
"""
Telegram Scraper Runner Script

Standalone script to run the Telegram scraper service.
Can be executed directly or via cron job.

Usage:
    python scripts/run_telegram_scraper.py
    
Cron example (daily at 6 AM IST = 12:30 AM UTC):
    30 0 * * * cd /path/to/project && .venv/bin/python scripts/run_telegram_scraper.py >> logs/telegram_scraper.log 2>&1

Author: Backend Team
Date: 2026-02-10
"""

import sys
import os
import asyncio
import logging
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.services.telegram_scraper_service import TelegramScraperService
from app.config import settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


async def main():
    """
    Main function to run the Telegram scraper.
    
    Initializes the scraper service, runs scraping for all channels,
    and reports results.
    """
    logger.info("=" * 70)
    logger.info("TELEGRAM SCRAPER - STANDALONE EXECUTION")
    logger.info("=" * 70)
    logger.info(f"Started at: {datetime.utcnow()} UTC")
    logger.info(f"MongoDB URI: {settings.MONGODB_URI[:50]}...")
    logger.info(f"Telegram API ID: {settings.TELEGRAM_API_ID}")
    logger.info("=" * 70)
    
    scraper = None
    
    try:
        # Create and initialize scraper
        logger.info("\n🔧 Initializing Telegram Scraper Service...")
        scraper = TelegramScraperService()
        await scraper.initialize()
        logger.info("✅ Service initialized successfully")
        
        # Run scraping
        logger.info("\n🚀 Starting scraping process...")
        result = await scraper.scrape_all_channels()
        
        # Log results
        logger.info("\n" + "=" * 70)
        logger.info("SCRAPING COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"Total Channels: {result['total_channels']}")
        logger.info(f"Successful: {result['successful']}")
        logger.info(f"Failed: {result['failed']}")
        logger.info(f"Total Messages: {result['total_messages']}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info(f"Completed at: {datetime.utcnow()} UTC")
        
        # Log per-account stats
        if result['account_stats']:
            logger.info("\n📊 Per-Account Statistics:")
            for account_id, stats in sorted(result['account_stats'].items()):
                logger.info(f"  Account {account_id}:")
                logger.info(f"    Channels scraped: {stats['channels_scraped']}")
                logger.info(f"    Messages found: {stats['messages_found']}")
                logger.info(f"    Rate limits hit: {stats['rate_limits']}")
                logger.info(f"    Errors: {stats['errors']}")
        
        # Log failed channels if any
        failed_channels = [r for r in result['results'] if not r['success']]
        if failed_channels:
            logger.warning(f"\n⚠️  {len(failed_channels)} channels failed:")
            for channel_result in failed_channels[:10]:  # Show first 10
                logger.warning(
                    f"  - @{channel_result['channel']}: {channel_result['error']}"
                )
            if len(failed_channels) > 10:
                logger.warning(f"  ... and {len(failed_channels) - 10} more")
        
        logger.info("=" * 70)
        
        return 0  # Success exit code
    
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Scraping interrupted by user")
        return 130  # Standard exit code for Ctrl+C
    
    except Exception as e:
        logger.error("\n" + "=" * 70)
        logger.error("SCRAPING FAILED")
        logger.error("=" * 70)
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 70)
        return 1  # Error exit code
    
    finally:
        # Cleanup
        if scraper:
            logger.info("\n🧹 Cleaning up...")
            try:
                await scraper.cleanup()
                logger.info("✅ Cleanup complete")
            except Exception as e:
                logger.warning(f"⚠️  Cleanup error: {e}")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
