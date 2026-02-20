"""
Telegram Service - Legacy Lambda Service

⚠️  WARNING: DO NOT USE FOR MAIN SCRAPING ⚠️

This service saves raw messages to PostgreSQL (incorrect architecture).
For main Telegram scraping, use: app/services/telegram_scraper_service.py

CORRECT ARCHITECTURE:
- Raw messages → MongoDB only (via telegram_scraper_service.py)
- Classified jobs → PostgreSQL (via ML classifier)

This service is ONLY used by:
- lambda/group_joiner (legacy Lambda for joining groups)

See: ARCHITECTURE_STORAGE.md for detailed explanation
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import UUID

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UserBannedInChannelError,
    ChannelInvalidError, AuthKeyError, UsernameInvalidError,
    UsernameNotOccupiedError, UserAlreadyParticipantError
)
from telethon.tl.types import Channel
from telethon.tl.functions.channels import JoinChannelRequest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.models.telegram_account import TelegramAccount
from app.models.telegram_group import TelegramGroup
# RawTelegramMessage removed - use MongoDB for raw messages
from app.config import settings

logger = logging.getLogger(__name__)


class TelegramService:
    """
    Telegram service for Lambda functions
    Handles account rotation, group joining, and message fetching
    """
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.clients: Dict[str, TelegramClient] = {}
    
    async def initialize_account(self, account: TelegramAccount) -> Optional[TelegramClient]:
        """
        Initialize a single Telegram account
        
        Args:
            account: TelegramAccount model instance
            
        Returns:
            TelegramClient instance or None if failed
        """
        if not account.is_active or account.is_banned:
            logger.warning(f"Account {account.phone} is not active or banned")
            return None
        
        try:
            # Session file path - use account phone to find matching session
            # Format: app/sessions/session_account2.session for +919329796819
            import os
            sessions_dir = "app/sessions"
            
            # First try to find existing session file by phone
            session_path = None
            if os.path.exists(sessions_dir):
                for file in os.listdir(sessions_dir):
                    if file.endswith('.session'):
                        # Use first available session file
                        session_path = os.path.join(sessions_dir, file.replace('.session', ''))
                        logger.info(f"Using session file: {file}")
                        break
            
            if not session_path:
                # Fallback to creating new session
                session_path = f"{sessions_dir}/session_{account.phone.replace('+', '')}"
                logger.info(f"No existing session found, will create: {session_path}")
            
            # Create client
            client = TelegramClient(
                session_path,  # Session file path
                account.api_id,
                account.api_hash,
                connection_retries=3,
                retry_delay=3,
                timeout=120,
                flood_sleep_threshold=0  # Handle manually
            )
            
            # Connect with timeout
            await asyncio.wait_for(client.connect(), timeout=150)
            
            # Check authorization
            if not await client.is_user_authorized():
                logger.error(f"Account {account.phone} needs authorization")
                await client.disconnect()
                return None
            
            # Test connection
            me = await asyncio.wait_for(client.get_me(), timeout=30)
            logger.info(f"✅ Initialized account {account.phone} ({me.first_name})")
            
            # Cache client
            self.clients[account.phone] = client
            
            # Update last used
            account.last_used_at = datetime.now()
            await self.db.commit()
            
            return client
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout initializing account {account.phone}")
            return None
        except AuthKeyError as e:
            logger.error(f"Auth key error for {account.phone}: {e}")
            # Mark account as needing re-authorization
            account.is_active = False
            account.notes = f"Auth key error: {e}"
            await self.db.commit()
            return None
        except Exception as e:
            logger.error(f"Failed to initialize {account.phone}: {e}")
            return None
    
    async def get_available_account_for_joining(self) -> Optional[Tuple[TelegramAccount, TelegramClient]]:
        """
        Get an available account that can join more groups today
        
        Returns:
            Tuple of (TelegramAccount, TelegramClient) or None
        """
        # Get all active accounts
        result = await self.db.execute(
            select(TelegramAccount).where(
                TelegramAccount.is_active == True,
                TelegramAccount.is_banned == False
            ).order_by(TelegramAccount.last_join_at.asc())
        )
        accounts = result.scalars().all()
        
        if not accounts:
            logger.warning("No active accounts available")
            return None
        
        # Check each account's daily limit
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        for account in accounts:
            # Count groups joined today
            result = await self.db.execute(
                select(TelegramGroup).where(
                    TelegramGroup.joined_by_account_id == account.id,
                    TelegramGroup.joined_at >= today_start
                )
            )
            groups_joined_today = len(result.scalars().all())
            
            # Check if under limit
            if groups_joined_today < settings.MAX_GROUPS_JOIN_PER_DAY:
                logger.info(f"✅ Account {account.phone} available: {groups_joined_today}/{settings.MAX_GROUPS_JOIN_PER_DAY} joined today")
                
                # Initialize client if not cached
                if account.phone not in self.clients:
                    client = await self.initialize_account(account)
                    if not client:
                        continue
                else:
                    client = self.clients[account.phone]
                
                return account, client
            else:
                logger.debug(f"⏭️  Account {account.phone} exhausted: {groups_joined_today}/{settings.MAX_GROUPS_JOIN_PER_DAY}")
        
        logger.warning("All accounts have reached daily join limit")
        return None
    
    async def join_group(self, group: TelegramGroup, account: TelegramAccount, client: TelegramClient) -> bool:
        """
        Join a Telegram group/channel
        
        Args:
            group: TelegramGroup model instance
            account: TelegramAccount to use
            client: Initialized TelegramClient
            
        Returns:
            True if successful, False otherwise
        """
        try:
            username = group.username.lstrip('@')
            
            logger.info(f"Joining group {username} with account {account.phone}...")
            
            # Get entity
            try:
                entity = await asyncio.wait_for(
                    client.get_entity(username),
                    timeout=60
                )
            except (UsernameInvalidError, UsernameNotOccupiedError) as e:
                logger.warning(f"Invalid username: {username} - {e}")
                return False
            
            # Join if it's a channel
            if isinstance(entity, Channel):
                try:
                    await asyncio.wait_for(
                        client(JoinChannelRequest(entity)),
                        timeout=60
                    )
                    logger.info(f"✅ Join request sent for {username}")
                except UserAlreadyParticipantError:
                    logger.info(f"Already a member of {username}")
            
            # Update group record
            group.is_joined = True
            group.joined_by_account_id = account.id  # UUID for legacy compatibility
            group.joined_by_phone = account.phone  # Store phone number for tracking
            group.joined_at = datetime.now()
            group.title = entity.title if hasattr(entity, 'title') else username
            
            if hasattr(entity, 'participants_count'):
                group.members_count = entity.participants_count
            
            # Update account
            account.last_join_at = datetime.now()
            account.groups_joined_count += 1
            
            await self.db.commit()
            
            logger.info(f"✅ Successfully joined {group.username} using {account.phone}")
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout joining {group.username}")
            return False
        except FloodWaitError as e:
            logger.warning(f"FloodWait: need to wait {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return False
        except (ChannelPrivateError, UserBannedInChannelError, ChannelInvalidError) as e:
            logger.warning(f"Cannot join {group.username}: {e}")
            group.is_active = False
            group.deactivation_reason = str(e)
            await self.db.commit()
            return False
        except Exception as e:
            logger.error(f"Error joining {group.username}: {e}")
            return False
    
    async def fetch_messages_from_group(
        self,
        group: TelegramGroup,
        client: TelegramClient,
        limit: Optional[int] = None
    ) -> int:
        """
        Fetch messages from a joined group.
        
        NOTE: This is a legacy service. Use telegram_scraper_service.py instead.
        This service no longer saves raw messages to PostgreSQL.
        
        Args:
            group: TelegramGroup model instance
            client: Initialized TelegramClient
            limit: Max messages to fetch (None = use config default)
            
        Returns:
            Number of new messages found
        """
        if limit is None:
            # Use incremental limit if we have last_message_id, else initial limit
            limit = settings.INCREMENTAL_FETCH_LIMIT if group.last_message_id else settings.MESSAGES_FETCH_LIMIT
        
        try:
            username = group.username.lstrip('@')
            
            # Get entity
            entity = await asyncio.wait_for(
                client.get_entity(username),
                timeout=60
            )
            
            # Determine starting point
            min_id = int(group.last_message_id) if group.last_message_id else 0
            
            logger.info(f"Fetching messages from {username} (min_id={min_id}, limit={limit})")
            
            messages_checked = 0
            new_messages = 0
            latest_message_id = min_id
            
            # Fetch messages
            async for message in client.iter_messages(entity, limit=limit, min_id=min_id):
                messages_checked += 1
                
                # Skip if no text
                if not message.text or len(message.text.strip()) < 10:
                    continue
                
                # Update latest message ID
                if message.id > latest_message_id:
                    latest_message_id = message.id
                
                # NOTE: RawTelegramMessage table removed from PostgreSQL
                # If this service is used, implement MongoDB storage instead
                # For now, just count messages without storing them
                new_messages += 1
                
                # Legacy code removed (was saving to PostgreSQL raw_telegram_messages)
                # Use telegram_scraper_service.py which saves to MongoDB instead
            
            # Update group metadata
            group.last_scraped_at = datetime.now()
            group.last_message_id = str(latest_message_id)
            group.last_message_date = datetime.now()
            group.messages_fetched_total += new_messages
            group.total_messages_scraped += messages_checked
            
            await self.db.commit()
            
            logger.info(f"✅ Fetched {new_messages} new messages from {username} (checked {messages_checked})")
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching from {group.username}")
        except FloodWaitError as e:
            logger.warning(f"FloodWait: {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error fetching from {group.username}: {e}")
        
        return new_messages
    
    async def cleanup(self):
        """Disconnect all clients"""
        for phone, client in self.clients.items():
            try:
                if client.is_connected():
                    await client.disconnect()
                    logger.info(f"Disconnected client for {phone}")
            except Exception as e:
                logger.debug(f"Cleanup error for {phone}: {e}")
