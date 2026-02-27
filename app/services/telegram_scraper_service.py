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
import base64
from datetime import datetime, timedelta, timezone
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
    ChannelInvalidError,
    UsernameInvalidError,
    AuthKeyError,
    ServerError,
    TimeoutError as TelethonTimeoutError,
)
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from sqlalchemy.orm import Session
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.config import settings
from app.utils.slack_notifier import slack_notifier
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.models.telegram_group import TelegramGroup
from app.db.session import SyncSessionLocal
from sqlalchemy import select

logger = structlog.get_logger(__name__)

# Decryption for API credentials
def get_encryption_key():
    """Derive Fernet key from SECRET_KEY"""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'telegram_account_salt',
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(settings.SECRET_KEY.encode()))
    return Fernet(key)

cipher = get_encryption_key()

def decrypt_credential(encrypted):
    """Decrypt API credentials"""
    return cipher.decrypt(encrypted.encode()).decode()


class TelegramScraperService:
    """
    Telegram message scraper with multi-account support.
    
    This service manages multiple Telegram accounts for distributed scraping,
    implementing rate limiting, incremental fetching, and error handling.
    
    Uses joined_by_phone to determine which account should scrape each group.
    
    Attributes:
        session_dir: Directory containing session files (default: sessions/)
        clients: Dictionary mapping phone_number to TelegramClient instances
        account_credentials: Cached account credentials from database
        account_stats: Statistics per phone (channels scraped, messages, rate limits)
        mongo_client: MongoDB client for storing raw messages
    """
    
    # Configuration
    MAX_MESSAGES_PER_CHANNEL = 100
    RATE_LIMIT_DELAY_MIN = 0.5  # Minimum 500ms between channels
    RATE_LIMIT_DELAY_MAX = 2.0  # Maximum 2s between channels (human-like randomness)
    FIRST_TIME_FETCH_LIMIT = 10  # Only 10 messages on first fetch
    FIRST_SCRAPE_DAYS = 2  # For first-time scrape, fetch messages from last N days
    
    # Phone to Account ID mapping (1-5)
    ACCOUNT_PHONE_MAPPING = {
        1: "+919794670665",
        2: "+917398227455",
        3: "+919140057096",
        4: "+917828629905",
        5: "+919329796819",
    }
    
    def __init__(self, session_dir: Optional[str] = None):
        """
        Initialize the Telegram Scraper Service.
        
        Args:
            session_dir: Path to directory containing session files (default: sessions/)
        """
        if session_dir is None:
            # Use sessions directory from project root
            import os
            project_root = Path(__file__).parent.parent.parent
            session_dir = str(project_root / "sessions")
        
        self.session_dir = Path(session_dir)
        self.clients: Dict[str, TelegramClient] = {}  # phone -> client
        self.account_credentials: Dict[str, Dict] = {}  # phone -> {api_id, api_hash}
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
    
    def _get_account_id_from_phone(self, phone: Optional[str]) -> Optional[int]:
        """
        Map phone number to account_id (1-5) using ACCOUNT_PHONE_MAPPING.
        
        Args:
            phone: Phone number string (e.g., "+919794670665")
        
        Returns:
            Account ID (1-5) or None if not found
        """
        if not phone:
            return None
        
        # Reverse lookup from mapping
        for account_id, mapped_phone in self.ACCOUNT_PHONE_MAPPING.items():
            if phone == mapped_phone:
                return account_id
        
        return None
    
    def _get_account_uuid_from_phone(self, phone: Optional[str]) -> Optional[str]:
        """
        Get telegram_account UUID from phone number.
        
        Args:
            phone: Phone number (e.g., "+919794670665")
        
        Returns:
            UUID string of the telegram_account or None
        """
        if not phone:
            return None
        
        db = SyncSessionLocal()
        try:
            account = db.execute(
                select(TelegramAccount).where(TelegramAccount.phone == phone)
            ).scalar_one_or_none()
            
            return str(account.id) if account else None
        finally:
            db.close()
    
    def get_session_path(self, phone: str) -> str:
        """
        Get session file path for a specific phone number.
        
        Args:
            phone: Phone number (e.g., "+917398227455")
        
        Returns:
            Path to session file (without .session extension)
        """
        return str(self.session_dir / phone)
    
    async def load_account_credentials(self, phone: str) -> Dict:
        """
        Load and decrypt account credentials from database.
        
        Args:
            phone: Phone number
        
        Returns:
            Dict with api_id and api_hash (decrypted)
        """
        if phone in self.account_credentials:
            return self.account_credentials[phone]
        
        db = SyncSessionLocal()
        try:
            account = db.query(TelegramAccount).filter(
                TelegramAccount.phone == phone
            ).first()
            
            if not account:
                raise ValueError(f"Account {phone} not found in database")
            
            # Decrypt credentials
            api_id = int(decrypt_credential(account.api_id))
            api_hash = decrypt_credential(account.api_hash)
            
            credentials = {
                "api_id": api_id,
                "api_hash": api_hash
            }
            
            # Cache it
            self.account_credentials[phone] = credentials
            return credentials
            
        finally:
            db.close()
    
    async def get_telegram_client(self, phone: str) -> TelegramClient:
        """
        Get or create Telegram client for specific phone number.
        
        Reuses existing client if already connected, otherwise creates new one.
        Verifies session file exists and account is authorized.
        
        Args:
            phone: Phone number (e.g., "+917955507455")
        
        Returns:
            TelegramClient instance for that account
        
        Raises:
            FileNotFoundError: If session file doesn't exist
            RuntimeError: If  account is not authorized
        """
        # Bind phone context for logging
        log = logger.bind(phone=phone)
        
        if phone in self.clients:
            return self.clients[phone]
        
        # Load credentials from database
        credentials = await self.load_account_credentials(phone)
        api_id = credentials["api_id"]
        api_hash = credentials["api_hash"]
        
        # Verify session file exists
        session_path = self.get_session_path(phone)
        session_file = f"{session_path}.session"
        
        # Create directory if it doesn't exist
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        if not Path(session_file).exists():
            error_msg = (
                f"Session file not found: {session_file}. "
                f"Run 'python3 telegram_account_manager.py' to login and create session."
            )
            log.error("session_file_not_found", session_file=session_file)
            
            # Send Slack alert
            await slack_notifier.send_session_error_alert(
                account_id=phone,
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
            # Create client with account-specific API credentials
            client = TelegramClient(
                session_path,
                api_id,
                api_hash,
                connection_retries=3,
                retry_delay=3,
                timeout=120
            )
            
            await client.connect()
            
            # Verify authorization
            if not await client.is_user_authorized():
                error_msg = f"Account {phone} not authorized"
                log.error("account_not_authorized")
                
                # Update account health in database
                await self._update_account_health(
                    phone,
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
                message=f"Connected to account {phone}",
                level="info"
            )
            
            self.clients[phone] = client
            return client
            
        except AuthKeyError as e:
            log.error(
                "auth_key_error",
                error=str(e),
                error_type="AuthKeyError"
            )
            
            # Mark account as banned
            await self._update_account_health(
                phone,
                success=False,
                error=f"AuthKeyError: {str(e)}",
                mark_banned=True
            )
            
            # Publish metric
# REMOVED (no AWS):             cloudwatch_metrics.publish_error_metric(
            
            # Capture in Sentry
            sentry_sdk.capture_exception(e)
            
            raise
    
    async def scrape_channel(
        self,
        channel: Dict,
        mongo_db
    ) -> Dict:
        """
        Scrape a single channel using the phone number that joined it.
        
        Implements incremental fetching: only fetches messages newer than
        last_message_id if it exists, otherwise fetches first 10 messages.
        
        Args:
            channel: Channel document from MongoDB with fields:
                - username: Channel username (without @)
                - joined_by_phone: Phone number that joined this group
                - last_message_id: Last fetched message ID (optional)
            mongo_db: MongoDB database instance
        
        Returns:
            Dict with scraping statistics:
                - channel: Channel username
                - phone: Phone number used
                - messages_fetched: Number of messages stored
                - success: Whether scraping succeeded
                - error: Error message if failed
        """
        username = channel.get('username', '').lstrip('@')
        phone = channel.get('joined_by_phone')
        
        # Ensure we use the SAME phone that joined the group
        if not phone:
            logger.warning(
                f"⚠️  @{username} has no joined_by_phone! Skipping scrape."
            )
            return {
                'channel': username,
                'phone': None,
                'messages_fetched': 0,
                'success': False,
                'error': 'No joined_by_phone - group not joined properly'
            }
        
        # Initialize account_id before try block for error handling
        account_id = channel.get('joined_by_account_id', 'unknown')  # For tracking/stats
        
        stats = {
            'channel': username,
            'phone': phone,
            'messages_fetched': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Get client for the phone that joined this channel
            client = await self.get_telegram_client(phone)
            
            logger.info(f"📱 {phone} → scraping @{username}")
            
            # Determine if this is first scrape (last_scraped_at is NULL)
            is_first_scrape = (channel.get('last_scraped_at') is None)
            
            # Fetch messages based on scrape history
            last_message_id = channel.get('last_message_id')
            
            if is_first_scrape:
                # 🆕 FIRST SCRAPE: Get last 2 days of messages
                from datetime import timedelta
                two_days_ago = datetime.now(timezone.utc) - timedelta(days=self.FIRST_SCRAPE_DAYS)
                logger.info(
                    f"   🆕 FIRST SCRAPE for @{username} (last_scraped_at=NULL) → fetching last {self.FIRST_SCRAPE_DAYS} days"
                )
                
                # TODO: Remove this 2-day logic after initial deployment (2026-03-05)
                # After all channels have been scraped once, use standard incremental logic
                
                messages = []
                async for message in client.iter_messages(username, offset_date=two_days_ago, limit=500):
                    # message.date is timezone-aware, two_days_ago is now also timezone-aware
                    if message.date < two_days_ago:
                        break
                    if message.text:  # Only text messages
                        messages.append(message)
                
                logger.info(f"   📥 FIRST SCRAPE result: {len(messages)} messages from last {self.FIRST_SCRAPE_DAYS} days for @{username}")
                if len(messages) == 0:
                    logger.warning(f"   ⚠️  Channel @{username} has NO messages in last {self.FIRST_SCRAPE_DAYS} days (inactive or new channel)")
            
            elif last_message_id:
                # Incremental fetch: Get messages after last_message_id
                logger.info(
                    f"   📥 INCREMENTAL fetch for @{username} (has last_message_id={last_message_id}) → fetching newer messages only"
                )
                messages = await client.get_messages(
                    username,
                    limit=self.MAX_MESSAGES_PER_CHANNEL,
                    min_id=last_message_id
                )
                messages = [m for m in messages if m and m.text]
            
            else:
                # Fallback: Channel was scraped but has no last_message_id (got 0 messages before)
                logger.warning(f"   ⚠️  FALLBACK for @{username} (last_scraped_at exists but no last_message_id) → fetching last {self.FIRST_TIME_FETCH_LIMIT} messages")
                messages = await client.get_messages(username, limit=self.FIRST_TIME_FETCH_LIMIT)
                messages = messages[:self.FIRST_TIME_FETCH_LIMIT] if len(messages) > self.FIRST_TIME_FETCH_LIMIT else messages
                messages = [m for m in messages if m and m.text]
                logger.info(f"   📥 FALLBACK result: {len(messages)} messages for @{username}")
            
            
            # IMPORTANT: Update PostgreSQL even if no messages fetched
            # This prevents channels from being stuck in "never scraped" state
            last_message = messages[0] if messages else None
            account_uuid = self._get_account_uuid_from_phone(phone)
            
            try:
                pg_session = SyncSessionLocal()
                pg_group = pg_session.execute(
                    select(TelegramGroup).where(TelegramGroup.username == username)
                ).scalar_one_or_none()
                
                if pg_group and account_uuid:
                    # Always update scrape timestamp, even with 0 messages
                    pg_group.last_scraped_at = datetime.utcnow()
                    pg_group.last_scraped_by_account = account_uuid
                    
                    # Update message info only if we got messages
                    if messages:
                        stored_count = self.store_messages_to_mongodb(messages, username, account_id, mongo_db)
                        pg_group.last_message_id = str(last_message.id)
                        pg_group.last_message_date = last_message.date
                        pg_group.total_messages_scraped = (pg_group.total_messages_scraped or 0) + stored_count
                        stats['messages_fetched'] = stored_count
                        logger.info(f"   ✅ Updated @{username}: {stored_count} messages stored")
                    else:
                        stats['messages_fetched'] = 0
                        logger.info(f"   ✅ Updated @{username}: 0 messages (scrape timestamp recorded)")
                    
                    pg_session.commit()
                elif not account_uuid:
                    logger.warning(f"   ⚠️  Could not get account UUID for phone {phone}")
                
                pg_session.close()
            except Exception as pg_error:
                logger.error(
                    f"   ❌ Failed to update PostgreSQL for @{username}: {pg_error}",
                    exc_info=True
                )
                if 'pg_session' in locals():
                    pg_session.rollback()
                    pg_session.close()
            
            stats['success'] = True
            
            # Update account stats
            self.account_stats[account_id]['channels_scraped'] += 1
            self.account_stats[account_id]['messages_found'] += stats.get('messages_fetched', 0)
            
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
# REMOVED (no AWS):             cloudwatch_metrics.publish_flood_wait_metric(
            
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
            
            # Update PostgreSQL: Mark channel as not joined or inactive
            try:
                pg_session = SyncSessionLocal()
                pg_group = pg_session.execute(
                    select(TelegramGroup).where(TelegramGroup.username == username)
                ).scalar_one_or_none()
                
                if pg_group:
                    pg_group.is_joined = False
                    pg_group.is_active = False
                    pg_group.deactivated_at = datetime.utcnow()
                    pg_group.deactivation_reason = f"{error_type}: {str(e)}"
                    pg_session.commit()
                    logger.info(f"   🚫 Marked @{username} as inactive in PostgreSQL (kicked or private)")
                
                pg_session.close()
            except Exception as pg_err:
                logger.warning(f"   ⚠️ Failed to update channel status: {pg_err}")
                if 'pg_session' in locals():
                    pg_session.close()
            
            # Publish metric
# REMOVED (no AWS):             cloudwatch_metrics.publish_error_metric(
        
        except ChannelInvalidError as e:
            error_msg = f"Channel invalid or deleted: {str(e)}"
            
            logger.error(
                "channel_invalid",
                account_id=account_id,
                channel=username,
                error=str(e)
            )
            
            stats['error'] = error_msg
            self.account_stats[account_id]['errors'] += 1
            
            # Update PostgreSQL: Mark channel as inactive (deleted or invalid)
            try:
                pg_session = SyncSessionLocal()
                pg_group = pg_session.execute(
                    select(TelegramGroup).where(TelegramGroup.username == username)
                ).scalar_one_or_none()
                
                if pg_group:
                    pg_group.is_active = False
                    pg_group.deactivated_at = datetime.utcnow()
                    pg_group.deactivation_reason = f"Channel deleted or invalid: {str(e)}"
                    pg_session.commit()
                    logger.info(f"   🗑️ Marked @{username} as inactive (channel invalid/deleted)")
                
                pg_session.close()
            except Exception as pg_err:
                logger.warning(f"   ⚠️ Failed to update channel status: {pg_err}")
                if 'pg_session' in locals():
                    pg_session.close()
        
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
# REMOVED (no AWS):             cloudwatch_metrics.publish_error_metric(
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
# REMOVED (no AWS):             cloudwatch_metrics.publish_error_metric(
            
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
    
    def store_messages_to_mongodb(
        self,
        messages: List,
        channel_username: str,
        account_id: int,
        mongo_db
    ) -> int:
        """
        Store messages to MongoDB raw_messages with retry logic.
        
        Args:
            messages: List of Telegram message objects
            channel_username: Channel username
            account_id: Account ID for tracking
            mongo_db: MongoDB database instance
        
        Returns:
            int: Number of messages successfully stored
        """
        from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, DuplicateKeyError
        
        raw_messages_collection = mongo_db['raw_messages']
        max_retries = 3
        retry_delay = 2  # seconds
        stored_count = 0
        skipped_duplicates = 0
        
        for attempt in range(max_retries):
            try:
                for msg in messages:
                    if msg.text:
                        doc = {
                            'message_id': msg.id,
                            'channel_username': channel_username,
                            'text': msg.text,
                            'date': msg.date,
                            'sender_id': msg.sender_id if hasattr(msg, 'sender_id') else None,
                            'views': msg.views if hasattr(msg, 'views') else None,
                            'forwards': msg.forwards if hasattr(msg, 'forwards') else None,
                            'fetched_at': datetime.utcnow(),
                            'fetched_by_account': account_id,
                            'is_processed': False
                        }
                        
                        try:
                            raw_messages_collection.update_one(
                                {'message_id': msg.id, 'channel_username': channel_username},
                                {'$set': doc},
                                upsert=True
                            )
                            stored_count += 1
                        except DuplicateKeyError:
                            # Message already exists (likely from different channel with same message_id)
                            # This happens because MongoDB has unique index on message_id alone
                            # TODO: Fix MongoDB index to be compound (message_id, channel_username)
                            skipped_duplicates += 1
                            continue
                
                if skipped_duplicates > 0:
                    logger.info(f"   💾 Stored {stored_count} messages, skipped {skipped_duplicates} duplicates for @{channel_username}")
                else:
                    logger.info(f"   💾 Stored {stored_count} messages to MongoDB for @{channel_username}")
                return stored_count
                
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                logger.warning(
                    f"   ⚠️ MongoDB connection lost (attempt {attempt + 1}/{max_retries}): {e}"
                )
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(
                        f"   ❌ Failed to store messages after {max_retries} attempts. Saving to fallback."
                    )
                    self._fallback_store_messages(messages, channel_username, account_id)
                    return 0
            
            except Exception as e:
                logger.error(f"   ❌ Unexpected error storing messages to MongoDB: {e}", exc_info=True)
                self._fallback_store_messages(messages, channel_username, account_id)
                return 0
        
        return stored_count
    
    def _fallback_store_messages(
        self,
        messages: List,
        channel_username: str,
        account_id: int
    ) -> None:
        """
        Fallback: Store messages to local JSON file when MongoDB is down.
        Background job can process these later.
        
        Args:
            messages: List of Telegram message objects
            channel_username: Channel username
            account_id: Account ID
        """
        import json
        from pathlib import Path
        
        fallback_dir = Path("./failed_messages")
        fallback_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = fallback_dir / f"{channel_username}_{timestamp}_acc{account_id}.json"
        
        messages_data = [
            {
                'message_id': msg.id,
                'channel_username': channel_username,
                'text': msg.text or '',
                'date': msg.date.isoformat() if msg.date else None,
                'sender_id': msg.sender_id if hasattr(msg, 'sender_id') else None,
                'views': getattr(msg, 'views', 0),
                'forwards': getattr(msg, 'forwards', 0),
                'fetched_by_account': account_id
            }
            for msg in messages if msg and msg.text
        ]
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(messages_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"   💾 FALLBACK: Saved {len(messages_data)} messages to {filename}")
        except Exception as e:
            logger.error(f"   ❌ Failed to save fallback file: {e}", exc_info=True)
    
    async def _update_account_health(
        self,
        phone: str,
        success: bool,
        error: Optional[str] = None,
        mark_banned: bool = False
    ) -> None:
        """
        Update account health status in database.
        
        Args:
            phone: Phone number
            success: Whether operation was successful
            error: Error message if failed
            mark_banned: Whether to mark account as banned
        """
        try:
            db = SyncSessionLocal()
            
            # Find account by phone number
            account = db.query(TelegramAccount).filter(
                TelegramAccount.phone == phone
            ).first()
            
            if not account:
                logger.warning(
                    "account_not_found_in_db",
                    phone=phone
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
                phone=phone,
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
# REMOVED (no AWS):             cloudwatch_metrics.publish_account_health(
            
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
        Get list of active, joined channels from PostgreSQL telegram_groups table.
        
        Returns:
            List of channel dictionaries compatible with existing scrape logic.
            Each dict contains: username, joined_by_phone, joined_by_account_id (for compatibility),
            last_message_id, total_messages_scraped
        """
        if not self._initialized:
            await self.initialize()
        
        db = SyncSessionLocal()
        try:
            # Query PostgreSQL for active, joined channels with assigned accounts
            channels_orm = db.execute(
                select(TelegramGroup)
                .where(
                    TelegramGroup.is_active == True,
                    TelegramGroup.is_joined == True,
                    TelegramGroup.telegram_account_id.isnot(None)  # Must have account assigned
                )
            ).scalars().all()
            
            # Convert ORM objects to dicts for backward compatibility
            channels = []
            for ch in channels_orm:
                # Map phone back to account_id (1-5) for logging compatibility
                account_id = self._get_account_id_from_phone(ch.joined_by_phone)
                if not account_id:
                    logger.warning(
                        f"⚠️  Channel @{ch.username} has invalid phone {ch.joined_by_phone}, skipping"
                    )
                    continue
                
                channels.append({
                    'id': str(ch.id),  # UUID string
                    'username': ch.username,
                    'joined_by_phone': ch.joined_by_phone,  # Used for getting Telegram client
                    'joined_by_account_id': account_id,  # Integer 1-5 (for compatibility with stats)
                    'last_message_id': int(ch.last_message_id) if ch.last_message_id else 0,
                    'total_messages_scraped': ch.total_messages_scraped or 0,
                    'last_scraped_at': ch.last_scraped_at  # For first-scrape detection
                })
            
            logger.info(f"Found {len(channels)} channels to scrape from PostgreSQL (telegram_groups)")
            return channels
            
        except Exception as e:
            logger.error(f"❌ Error querying channels from PostgreSQL: {e}", exc_info=True)
            return []
        finally:
            db.close()
    
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
                account_id = channel.get('joined_by_account_id')
                if account_id:  # Only count properly joined channels
                    channels_by_account[account_id].append(channel['username'])
                else:
                    channels_by_account['unassigned'].append(channel['username'])
            
            logger.info("\n📊 Channel distribution:")
            for key in sorted(channels_by_account.keys(), key=lambda x: x if isinstance(x, int) else 999):
                channel_list = channels_by_account[key]
                if key == 'unassigned':
                    logger.warning(f"   ⚠️  Unassigned: {len(channel_list)} channels (not properly joined)")
                else:
                    logger.info(f"   Account {key}: {len(channel_list)} channels")
            
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
# REMOVED (no AWS):                 cloudwatch_metrics.publish_scrape_metrics(
            
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
        
        # Get channel from PostgreSQL
        db = SyncSessionLocal()
        try:
            pg_channel = db.execute(
                select(TelegramGroup).where(TelegramGroup.username == username)
            ).scalar_one_or_none()
            
            if not pg_channel:
                raise ValueError(f"Channel @{username} not found in database")
            
            if not pg_channel.is_active:
                raise ValueError(f"Channel @{username} is not active")
            
            if not pg_channel.telegram_account_id:
                raise ValueError(f"Channel @{username} has no assigned Telegram account")
            
            # Convert to dict for compatibility with scrape_channel()
            account_id = self._get_account_id_from_phone(pg_channel.joined_by_phone)
            channel = {
                'id': str(pg_channel.id),
                'username': pg_channel.username,
                'joined_by_phone': pg_channel.joined_by_phone,
                'joined_by_account_id': account_id,
                'last_message_id': int(pg_channel.last_message_id) if pg_channel.last_message_id else 0,
                'total_messages_scraped': pg_channel.total_messages_scraped or 0,
                'last_scraped_at': pg_channel.last_scraped_at
            }
            
        finally:
            db.close()
        
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
