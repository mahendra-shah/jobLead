"""
Telegram Scraper Service - Multi-Account Message Fetcher

Integrated from Lambda function into monolithic backend.
Fetches messages from Telegram channels using multiple accounts for rate limiting.

Key Features:
- Multi-account support (5 Telegram accounts)
- Incremental fetching (only new messages)
- First-time fetch limit (10 messages to avoid overload)
- Rate limiting (500ms between channels)
- FloodWait error handling
- Connection pooling
- MongoDB storage

Author: Migration from lambda/telegram_scraper/lambda_function.py
Date: 2026-02-10
"""

import asyncio
import time
import random
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict
from pathlib import Path

import structlog
import sentry_sdk
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError
)
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    UsernameInvalidError,
    AuthKeyError,
    ServerError,
    TimeoutError as TelethonTimeoutError,
)
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from sqlalchemy.orm import Session

from app.config import settings
from app.utils.cloudwatch_metrics import cloudwatch_metrics
from app.utils.slack_notifier import slack_notifier
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.db.session import SyncSessionLocal

logger = structlog.get_logger(__name__)


class TelegramScraperService:
    """
    Telegram message scraper with multi-account support.
    
    This service manages multiple Telegram accounts for distributed scraping,
    implementing rate limiting, incremental fetching, and error handling.
    
    Attributes:
        session_dir: Directory containing session files (default: app/sessions)
        clients: Dictionary mapping account_id to TelegramClient instances
        account_stats: Statistics per account (channels scraped, messages, rate limits)
        mongo_client: MongoDB client for storing raw messages
    """
    
    # Configuration
    MAX_MESSAGES_PER_CHANNEL = 100
    RATE_LIMIT_DELAY_MIN = 0.5  # Minimum 500ms between channels
    RATE_LIMIT_DELAY_MAX = 2.0  # Maximum 2s between channels (human-like randomness)
    ACCOUNTS_AVAILABLE = 5  # Total number of Telegram accounts
    FIRST_TIME_FETCH_LIMIT = 10  # Only 10 messages on first fetch
    
    def __init__(self, session_dir: Optional[str] = None):
        """
        Initialize the Telegram Scraper Service.
        
        Args:
            session_dir: Path to directory containing session files (default: app/sessions)
        """
        if session_dir is None:
            # Use absolute path from project root
            import os
            project_root = Path(__file__).parent.parent.parent
            session_dir = str(project_root / "app" / "sessions")
        
        self.session_dir = Path(session_dir)
        self.clients: Dict[int, TelegramClient] = {}
        self.account_stats = defaultdict(
            lambda: {
                'channels_scraped': 0,
                'messages_found': 0,
                'rate_limits': 0,
                'errors': 0
            }
        )
        self.mongo_client: Optional[MongoClient] = None
        self._initialized = False
        
        logger.info(f"Initialized TelegramScraperService with session_dir: {self.session_dir}")
    
    async def initialize(self) -> None:
        """
        Initialize MongoDB connection.
        Should be called before using the service.
        """
        if self._initialized:
            return
        
        try:
            # Initialize MongoDB client
            self.mongo_client = MongoClient(
                settings.MONGODB_URI,
                maxPoolSize=10,
                minPoolSize=2,
                maxIdleTimeMS=30000,
                serverSelectionTimeoutMS=5000
            )
            # Test connection
            start_time = time.time()
            self.mongo_client.admin.command('ping')
            latency_ms = (time.time() - start_time) * 1000
            
            logger.info(
                "mongodb_connected",
                latency_ms=round(latency_ms, 2)
            )
            
            self._initialized = True
        except Exception as e:
            logger.error(
                "mongodb_connection_failed",
                error=str(e),
                error_type=type(e).__name__
            )
            
            # Send Slack alert for MongoDB failure
            await slack_notifier.send_database_error_alert(
                database="MongoDB",
                error=str(e)
            )
            
            # Capture in Sentry
            sentry_sdk.capture_exception(e)
            
            raise
    
    def get_session_path(self, account_id: int) -> str:
        """
        Get session file path for a specific account.
        
        Args:
            account_id: Account number (1-5)
        
        Returns:
            Path to session file (without .session extension)
        """
        return str(self.session_dir / f"session_account{account_id}")
    
    async def get_telegram_client(self, account_id: int) -> TelegramClient:
        """
        Get or create Telegram client for specific account.
        
        Reuses existing client if already connected, otherwise creates new one.
        Verifies session file exists and account is authorized.
        
        Args:
            account_id: Account number (1-5)
        
        Returns:
            TelegramClient instance for that account
        
        Raises:
            FileNotFoundError: If session file doesn't exist
            RuntimeError: If account is not authorized
        """
        # Bind account context for logging
        log = logger.bind(account_id=account_id)
        
        if account_id in self.clients:
            return self.clients[account_id]
        
        # Verify session file exists
        session_path = self.get_session_path(account_id)
        session_file = f"{session_path}.session"
        
        # Create directory if it doesn't exist
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        if not Path(session_file).exists():
            error_msg = (
                f"Session file not found: {session_file}. "
                f"Run 'python generate_telegram_session.py' to create session files."
            )
            log.error("session_file_not_found", session_file=session_file)
            
            # Send Slack alert
            await slack_notifier.send_session_error_alert(
                account_id=account_id,
                error_message="Session file not found"
            )
            
            raise FileNotFoundError(error_msg)
        
        # Check session file health
        session_stat = Path(session_file).stat()
        log.info(
            "session_file_found",
            size_bytes=session_stat.st_size,
            modified_days_ago=(time.time() - session_stat.st_mtime) / 86400
        )
        
        try:
            # Create client
            client = TelegramClient(
                session_path,
                int(settings.TELEGRAM_API_ID),
                settings.TELEGRAM_API_HASH,
                connection_retries=3,
                retry_delay=3,
                timeout=120
            )
            
            await client.connect()
            
            # Verify authorization
            if not await client.is_user_authorized():
                error_msg = f"Account {account_id} not authorized"
                log.error("account_not_authorized")
                
                # Update account health in database
                await self._update_account_health(
                    account_id,
                    success=False,
                    error="Not authorized - session expired"
                )
                
                raise RuntimeError(error_msg)
            
            # Get account info for logging
            me = await client.get_me()
            log.info(
                "telegram_client_connected",
                phone=me.phone,
                username=me.username
            )
            
            # Add Sentry breadcrumb
            sentry_sdk.add_breadcrumb(
                category="telegram",
                message=f"Connected to account {account_id}",
                level="info"
            )
            
            self.clients[account_id] = client
            return client
            
        except AuthKeyError as e:
            log.error(
                "auth_key_error",
                error=str(e),
                error_type="AuthKeyError"
            )
            
            # Mark account as banned
            await self._update_account_health(
                account_id,
                success=False,
                error=f"AuthKeyError: {str(e)}",
                mark_banned=True
            )
            
            # Publish metric
            cloudwatch_metrics.publish_error_metric(
                error_type="AuthKeyError",
                account_id=account_id
            )
            
            # Capture in Sentry
            sentry_sdk.capture_exception(e)
            
            raise
    
    async def scrape_channel(
        self,
        channel: Dict,
        mongo_db
    ) -> Dict:
        """
        Scrape a single channel using its assigned account.
        
        Implements incremental fetching: only fetches messages newer than
        last_message_id if it exists, otherwise fetches first 10 messages.
        
        Args:
            channel: Channel document from MongoDB with fields:
                - username: Channel username (without @)
                - joined_by_account_id: Account ID to use (1-5)
                - last_message_id: Last fetched message ID (optional)
            mongo_db: MongoDB database instance
        
        Returns:
            Dict with scraping statistics:
                - channel: Channel username
                - account_id: Account used
                - messages_fetched: Number of messages stored
                - success: Whether scraping succeeded
                - error: Error message if failed
        """
        username = channel.get('username', '').lstrip('@')
        account_id = channel.get('joined_by_account_id', 1)
        
        stats = {
            'channel': username,
            'account_id': account_id,
            'messages_fetched': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Get client for this channel's account
            client = await self.get_telegram_client(account_id)
            
            logger.info(f"📱 Account {account_id} → scraping @{username}")
            
            # Incremental fetching logic
            last_message_id = channel.get('last_message_id')
            
            if last_message_id:
                # Incremental fetch: Get messages after last_message_id
                logger.info(
                    f"   📥 Incremental fetch: messages newer than ID {last_message_id}"
                )
                messages = await client.get_messages(
                    username,
                    limit=self.MAX_MESSAGES_PER_CHANNEL,
                    min_id=last_message_id
                )
            else:
                # First time fetch: Get last 10 messages only
                logger.info(f"   📥 First time fetch: last {self.FIRST_TIME_FETCH_LIMIT} messages")
                messages = await client.get_messages(username, limit=self.FIRST_TIME_FETCH_LIMIT)
                # Explicitly slice to ensure limit (Telethon sometimes returns more)
                messages = messages[:self.FIRST_TIME_FETCH_LIMIT] if len(messages) > self.FIRST_TIME_FETCH_LIMIT else messages
                logger.info(
                    f"   📊 Fetched {len(messages)} messages "
                    f"(first-time limit: {self.FIRST_TIME_FETCH_LIMIT})"
                )
            
            if not messages:
                logger.info(f"   ℹ️  No new messages in @{username}")
                stats['success'] = True
                return stats
            
            # Process and store messages
            raw_messages_collection = mongo_db['raw_messages']
            channels_collection = mongo_db['channels']
            
            stored_count = 0
            for msg in messages:
                if msg.text:
                    # Create document
                    doc = {
                        'message_id': msg.id,
                        'channel_username': username,
                        'channel_id': channel.get('_id'),
                        'text': msg.text,
                        'date': msg.date,
                        'sender_id': msg.sender_id if hasattr(msg, 'sender_id') else None,
                        'views': msg.views if hasattr(msg, 'views') else None,
                        'forwards': msg.forwards if hasattr(msg, 'forwards') else None,
                        'fetched_at': datetime.utcnow(),
                        'fetched_by_account': account_id,
                        'is_processed': False
                    }
                    
                    # Upsert (avoid duplicates)
                    raw_messages_collection.update_one(
                        {'message_id': msg.id, 'channel_username': username},
                        {'$set': doc},
                        upsert=True
                    )
                    stored_count += 1
            
            # Update channel metadata
            last_message = messages[0] if messages else None
            channels_collection.update_one(
                {'username': username},
                {
                    '$set': {
                        'last_scraped_at': datetime.utcnow(),
                        'last_scraped_by_account': account_id,
                        'last_message_id': last_message.id if last_message else None,
                        'last_message_date': last_message.date if last_message else None,
                        'total_messages_scraped': channel.get('total_messages_scraped', 0) + stored_count
                    }
                }
            )
            
            stats['messages_fetched'] = stored_count
            stats['success'] = True
            
            # Update account stats
            self.account_stats[account_id]['channels_scraped'] += 1
            self.account_stats[account_id]['messages_found'] += stored_count
            
            logger.info(f"   ✅ Stored {stored_count} messages from @{username}")
            
            # Rate limiting between channels - random delay for human-like behavior
            delay = random.uniform(self.RATE_LIMIT_DELAY_MIN, self.RATE_LIMIT_DELAY_MAX)
            await asyncio.sleep(delay)
            
        except FloodWaitError as e:
            error_msg = f"Rate limited: wait {e.seconds}s"
            logger.warning(
                "flood_wait_error",
                account_id=account_id,
                channel=username,
                wait_seconds=e.seconds
            )
            stats['error'] = error_msg
            self.account_stats[account_id]['rate_limits'] += 1
            
            # Publish CloudWatch metric
            cloudwatch_metrics.publish_flood_wait_metric(
                wait_seconds=e.seconds,
                account_id=account_id
            )
            
            # Wait if reasonable time (under 60 seconds)
            if e.seconds < 60:
                await asyncio.sleep(e.seconds)
            else:
                # Log for manual intervention
                logger.error(
                    "flood_wait_too_long",
                    account_id=account_id,
                    channel=username,
                    wait_seconds=e.seconds
                )
        
        except (ChannelPrivateError, UsernameInvalidError) as e:
            error_msg = f"Channel access error: {str(e)}"
            error_type = type(e).__name__
            
            logger.error(
                "channel_access_error",
                account_id=account_id,
                channel=username,
                error=str(e),
                error_type=error_type
            )
            
            stats['error'] = error_msg
            self.account_stats[account_id]['errors'] += 1
            
            # Publish metric
            cloudwatch_metrics.publish_error_metric(
                error_type=error_type,
                account_id=account_id,
                channel=username
            )
        
        except AuthKeyError as e:
            error_msg = f"Auth key error: {str(e)}"
            
            logger.error(
                "auth_key_error_in_scrape",
                account_id=account_id,
                channel=username,
                error=str(e)
            )
            
            stats['error'] = error_msg
            self.account_stats[account_id]['errors'] += 1
            
            # Mark account as banned
            await self._update_account_health(
                account_id,
                success=False,
                error=error_msg,
                mark_banned=True
            )
            
            # Publish metric and capture in Sentry
            cloudwatch_metrics.publish_error_metric(
                error_type="AuthKeyError",
                account_id=account_id,
                channel=username
            )
            sentry_sdk.capture_exception(e)
        
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            error_type = type(e).__name__
            
            logger.error(
                "unexpected_scrape_error",
                account_id=account_id,
                channel=username,
                error=str(e),
                error_type=error_type,
                exc_info=True
            )
            
            stats['error'] = error_msg
            self.account_stats[account_id]['errors'] += 1
            
            # Publish metric
            cloudwatch_metrics.publish_error_metric(
                error_type=error_type,
                account_id=account_id,
                channel=username
            )
            
            # Capture in Sentry with context
            with sentry_sdk.push_scope() as scope:
                scope.set_tag("component", "telegram_scraper")
                scope.set_tag("account_id", account_id)
                scope.set_tag("channel", username)
                scope.set_context("channel", {
                    "username": username,
                    "account_id": account_id,
                })
                sentry_sdk.capture_exception(e)
        
        return stats
    
    async def _update_account_health(
        self,
        account_id: int,
        success: bool,
        error: Optional[str] = None,
        mark_banned: bool = False
    ) -> None:
        """
        Update account health status in database.
        
        Args:
            account_id: Account ID
            success: Whether operation was successful
            error: Error message if failed
            mark_banned: Whether to mark account as banned
        """
        try:
            db = SyncSessionLocal()
            
            # Find account by phone number (assuming account_id maps to phone)
            # Note: This is a simplified lookup - adjust based on your actual data model
            account = db.query(TelegramAccount).filter(
                TelegramAccount.phone.like(f"%{account_id}%")
            ).first()
            
            if not account:
                logger.warning(
                    "account_not_found_in_db",
                    account_id=account_id
                )
                db.close()
                return
            
            if success:
                account.mark_success()
            else:
                if error:
                    account.mark_error(error)
                    
                if mark_banned:
                    account.health_status = HealthStatus.BANNED
                    account.is_banned = True
                    account.is_active = False
            
            db.commit()
            db.close()
            
            logger.info(
                "account_health_updated",
                account_id=account_id,
                health_status=account.health_status.value if account else None,
                consecutive_errors=account.consecutive_errors if account else None
            )
            
        except Exception as e:
            logger.error(
                "failed_to_update_account_health",
                account_id=account_id,
                error=str(e)
            )
    
    async def _check_and_report_account_health(self) -> None:
        """
        Check health of all accounts and send alerts if issues detected.
        """
        try:
            db = SyncSessionLocal()
            accounts = db.query(TelegramAccount).all()
            
            active_count = sum(1 for a in accounts if a.is_healthy())
            degraded_count = sum(1 for a in accounts if a.health_status == HealthStatus.DEGRADED)
            banned_count = sum(1 for a in accounts if a.health_status == HealthStatus.BANNED)
            
            db.close()
            
            # Publish account health metrics
            cloudwatch_metrics.publish_account_health(
                active_accounts=active_count,
                degraded_accounts=degraded_count,
                banned_accounts=banned_count
            )
            
            # Send Slack alert if critical health issues
            if active_count == 0 or active_count <= 2:
                account_details = [
                    {
                        "id": a.phone,
                        "status": a.health_status.value,
                        "error": a.last_error_message or "N/A"
                    }
                    for a in accounts
                ]
                
                await slack_notifier.send_account_health_alert(
                    active_accounts=active_count,
                    degraded_accounts=degraded_count,
                    banned_accounts=banned_count,
                    details=account_details
                )
            
            logger.info(
                "account_health_summary",
                active=active_count,
                degraded=degraded_count,
                banned=banned_count
            )
            
        except Exception as e:
            logger.error(
                "failed_to_check_account_health",
                error=str(e)
            )
    
    async def get_channels_to_scrape(self) -> List[Dict]:
        """
        Get list of active channels from MongoDB that need scraping.
        
        Returns:
            List of channel documents with username and joined_by_account_id
        """
        if not self._initialized:
            await self.initialize()
        
        mongo_db = self.mongo_client[settings.MONGODB_DATABASE]
        channels_collection = mongo_db['channels']
        
        # Get all active channels
        channels = list(channels_collection.find(
            {'is_active': True},
            {
                '_id': 1,
                'username': 1,
                'joined_by_account_id': 1,
                'last_message_id': 1,
                'total_messages_scraped': 1
            }
        ))
        
        logger.info(f"Found {len(channels)} active channels to scrape")
        return channels
    
    async def scrape_all_channels(self) -> Dict:
        """
        Scrape all active channels using assigned accounts.
        
        Main entry point for scraping operation. Fetches channels from MongoDB,
        distributes them across accounts, and scrapes them all.
        
        Returns:
            Dict with overall statistics:
                - total_channels: Total channels attempted
                - successful: Number of successful scrapes
                - failed: Number of failed scrapes
                - total_messages: Total messages fetched
                - account_stats: Per-account statistics
                - results: Detailed results for each channel
                - started_at: Timestamp when scraping started
                - completed_at: Timestamp when scraping completed
        """
        started_at = datetime.utcnow()
        
        try:
            # Initialize if not already done
            if not self._initialized:
                await self.initialize()
            
            # Get channels to scrape
            channels = await self.get_channels_to_scrape()
            
            if not channels:
                logger.warning("No active channels found to scrape")
                return {
                    'total_channels': 0,
                    'successful': 0,
                    'failed': 0,
                    'total_messages': 0,
                    'account_stats': {},
                    'results': [],
                    'started_at': started_at,
                    'completed_at': datetime.utcnow()
                }
            
            # Reset account stats
            self.account_stats.clear()
            
            logger.info(f"\n🚀 Starting scrape: {len(channels)} channels")
            
            # Group channels by account for logging
            channels_by_account = defaultdict(list)
            for channel in channels:
                account_id = channel.get('joined_by_account_id', 1)
                channels_by_account[account_id].append(channel['username'])
            
            logger.info("\n📊 Channel distribution:")
            for account_id in sorted(channels_by_account.keys()):
                channel_list = channels_by_account[account_id]
                logger.info(f"   Account {account_id}: {len(channel_list)} channels")
            
            # Scrape all channels
            mongo_db = self.mongo_client[settings.MONGODB_DATABASE]
            results = []
            
            for channel in channels:
                result = await self.scrape_channel(channel, mongo_db)
                results.append(result)
            
            # Calculate summary statistics
            total_messages = sum(r['messages_fetched'] for r in results)
            successful = sum(1 for r in results if r['success'])
            failed = len(results) - successful
            completed_at = datetime.utcnow()
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)
            
            summary = {
                'total_channels': len(channels),
                'successful': successful,
                'failed': failed,
                'total_messages': total_messages,
                'account_stats': dict(self.account_stats),
                'results': results,
                'started_at': started_at,
                'completed_at': completed_at,
                'duration_seconds': (completed_at - started_at).total_seconds()
            }
            
            # Log summary
            logger.info(
                "scraping_complete",
                total_channels=len(channels),
                successful=successful,
                failed=failed,
                total_messages=total_messages,
                duration_seconds=round(summary['duration_seconds'], 2)
            )
            
            # Log per-account stats
            for account_id in sorted(self.account_stats.keys()):
                stats = self.account_stats[account_id]
                logger.info(
                    "account_scrape_stats",
                    account_id=account_id,
                    channels_scraped=stats['channels_scraped'],
                    messages_found=stats['messages_found'],
                    rate_limits=stats['rate_limits'],
                    errors=stats['errors']
                )
                
                # Publish CloudWatch metrics per account
                cloudwatch_metrics.publish_scrape_metrics(
                    account_id=account_id,
                    messages_processed=stats['messages_found'],
                    channels_scraped=stats['channels_scraped'],
                    duration_ms=duration_ms,
                    errors_count=stats['errors']
                )
            
            # Check account health and publish metrics
            await self._check_and_report_account_health()
            
            # Send Slack alert if zero messages fetched
            if total_messages == 0:
                await slack_notifier.send_zero_messages_alert(
                    last_successful_fetch=completed_at
                )
            
            # Add Sentry breadcrumb for successful scrape
            sentry_sdk.add_breadcrumb(
                category="telegram_scraper",
                message=f"Scraped {total_messages} messages from {successful} channels",
                level="info",
                data=summary
            )
            
            return summary
        
        except Exception as e:
            logger.error(f"❌ Error in scrape_all_channels: {e}", exc_info=True)
            raise
    
    async def scrape_single_channel(self, channel_username: str) -> Dict:
        """
        Scrape a single channel by username.
        
        Useful for manual/on-demand scraping of specific channels.
        
        Args:
            channel_username: Channel username (with or without @)
        
        Returns:
            Dict with scraping statistics
        """
        if not self._initialized:
            await self.initialize()
        
        username = channel_username.lstrip('@')
        mongo_db = self.mongo_client[settings.MONGODB_DATABASE]
        channels_collection = mongo_db['channels']
        
        # Get channel document
        channel = channels_collection.find_one({'username': username})
        
        if not channel:
            raise ValueError(f"Channel @{username} not found in database")
        
        if not channel.get('is_active', True):
            raise ValueError(f"Channel @{username} is not active")
        
        # Scrape the channel
        result = await self.scrape_channel(channel, mongo_db)
        
        return result
    
    async def cleanup(self) -> None:
        """
        Cleanup resources: disconnect all Telegram clients and close MongoDB.
        
        Should be called when shutting down the service.
        """
        logger.info("🧹 Cleaning up Telegram Scraper Service...")
        
        # Disconnect Telegram clients
        for account_id, client in self.clients.items():
            try:
                await client.disconnect()
                logger.info(f"✅ Disconnected Account {account_id}")
            except Exception as e:
                logger.warning(f"⚠️  Error disconnecting Account {account_id}: {e}")
        
        self.clients.clear()
        
        # Close MongoDB client
        if self.mongo_client:
            try:
                self.mongo_client.close()
                logger.info("✅ Closed MongoDB connection")
            except Exception as e:
                logger.warning(f"⚠️  Error closing MongoDB: {e}")
        
        self._initialized = False
        logger.info("✅ Cleanup complete")
    
    def get_stats(self) -> Dict:
        """
        Get current statistics for all accounts.
        
        Returns:
            Dict with account statistics
        """
        return {
            'accounts': dict(self.account_stats),
            'total_clients': len(self.clients)
        }


# Global service instance (singleton pattern)
_scraper_service: Optional[TelegramScraperService] = None


def get_scraper_service() -> TelegramScraperService:
    """
    Get the global scraper service instance (singleton).
    
    Returns:
        TelegramScraperService instance
    """
    global _scraper_service
    
    if _scraper_service is None:
        _scraper_service = TelegramScraperService()
    
    return _scraper_service
