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
import logging
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameInvalidError
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.config import settings

logger = logging.getLogger(__name__)


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
    RATE_LIMIT_DELAY = 0.5  # 500ms between channels
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
            self.mongo_client.admin.command('ping')
            logger.info("✅ MongoDB connection established")
            
            self._initialized = True
        except Exception as e:
            logger.error(f"❌ Failed to initialize MongoDB: {e}")
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
        if account_id in self.clients:
            return self.clients[account_id]
        
        # Verify session file exists
        session_path = self.get_session_path(account_id)
        session_file = f"{session_path}.session"
        
        if not Path(session_file).exists():
            raise FileNotFoundError(
                f"Session file not found: {session_file}. "
                f"Please ensure session files exist in {self.session_dir}/"
            )
        
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
            raise RuntimeError(f"Account {account_id} not authorized")
        
        # Get account info for logging
        me = await client.get_me()
        logger.info(f"✅ Account {account_id} connected: {me.phone or me.username}")
        
        self.clients[account_id] = client
        return client
    
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
            
            # Rate limiting between channels
            await asyncio.sleep(self.RATE_LIMIT_DELAY)
            
        except FloodWaitError as e:
            error_msg = f"Rate limited on Account {account_id}: wait {e.seconds}s"
            logger.warning(f"   ⚠️  {error_msg}")
            stats['error'] = error_msg
            self.account_stats[account_id]['rate_limits'] += 1
            
            # Wait if reasonable time (under 60 seconds)
            if e.seconds < 60:
                await asyncio.sleep(e.seconds)
        
        except (ChannelPrivateError, UsernameInvalidError) as e:
            error_msg = f"Channel access error: {str(e)}"
            logger.error(f"   ❌ {error_msg}")
            stats['error'] = error_msg
            self.account_stats[account_id]['errors'] += 1
        
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"   ❌ {error_msg}", exc_info=True)
            stats['error'] = error_msg
            self.account_stats[account_id]['errors'] += 1
        
        return stats
    
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
            logger.info(f"\n✅ Scraping complete:")
            logger.info(f"   Channels: {successful}/{len(channels)} successful")
            logger.info(f"   Messages: {total_messages} total")
            logger.info(f"   Duration: {summary['duration_seconds']:.2f}s")
            
            logger.info(f"\n📊 Per-account stats:")
            for account_id in sorted(self.account_stats.keys()):
                stats = self.account_stats[account_id]
                logger.info(f"   Account {account_id}:")
                logger.info(f"      Channels scraped: {stats['channels_scraped']}")
                logger.info(f"      Messages found: {stats['messages_found']}")
                logger.info(f"      Rate limits hit: {stats['rate_limits']}")
                logger.info(f"      Errors: {stats['errors']}")
            
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
