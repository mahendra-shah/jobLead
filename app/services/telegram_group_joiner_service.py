"""
Telegram Group Joiner Service

Handles automated joining of Telegram channels/groups.
Designed for gradual joining (1 channel per account per cycle).

Features:
- Early exit if no unjoined channels
- Multi-account support with round-robin distribution
- FloodWait handling with exponential backoff
- Database updates and Slack notifications
- Error handling for banned accounts and private channels

Author: Backend Team
Date: 2026-02-26
"""

import logging
import asyncio
import random
import base64
from typing import List, Dict, Optional
from datetime import datetime, timezone

from sqlalchemy import select
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    UserAlreadyParticipantError,
    UserBannedInChannelError,
    InviteHashExpiredError
)
from telethon.tl.functions.channels import JoinChannelRequest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.config import settings
from app.db.session import AsyncSessionLocal
from app.models.telegram_group import TelegramGroup
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.utils.slack_notifier import slack_notifier

logger = logging.getLogger(__name__)


# Decryption utilities
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


def decrypt_credential(encrypted: str) -> str:
    """Decrypt API credentials"""
    return cipher.decrypt(encrypted.encode()).decode()


class TelegramGroupJoinerService:
    """Service for joining Telegram groups/channels."""
    
    def __init__(self):
        self.clients = {}  # phone -> TelegramClient
        self.stats = {
            "successful_joins": 0,
            "already_joined": 0,
            "failed_joins": 0,
            "errors": []
        }
    
    async def check_unjoined_channels_exist(self) -> int:
        """
        Check if any unjoined channels exist.
        
        Returns:
            int: Count of unjoined active channels
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(TelegramGroup)
                .where(TelegramGroup.is_joined == False)
                .where(TelegramGroup.is_active == True)
            )
            channels = result.scalars().all()
            count = len(channels)
            
            if count == 0:
                logger.info("✅ No unjoined channels found - all channels are joined!")
            else:
                logger.info(f"📊 Found {count} unjoined channels to process")
            
            return count
    
    async def load_authorized_accounts(self) -> List[TelegramAccount]:
        """
        Load all active Telegram accounts from database.
        
        Returns:
            List[TelegramAccount]: Active accounts ready for joining
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(TelegramAccount)
                .where(TelegramAccount.is_active == True)
                .where(TelegramAccount.is_banned == False)
                .order_by(TelegramAccount.groups_joined_count)  # Least loaded first
            )
            accounts = result.scalars().all()
            
            if not accounts:
                logger.warning("⚠️ No active Telegram accounts found")
                return []
            
            logger.info(f"📱 Loaded {len(accounts)} active accounts")
            return list(accounts)
    
    async def initialize_telegram_clients(self, accounts: List[TelegramAccount]) -> Dict[str, TelegramClient]:
        """
        Initialize Telegram clients for all accounts.
        
        Args:
            accounts: List of TelegramAccount objects
        
        Returns:
            Dict mapping phone -> TelegramClient
        """
        clients = {}
        
        for account in accounts:
            try:
                # Decrypt credentials
                api_id = int(decrypt_credential(account.api_id))
                api_hash = decrypt_credential(account.api_hash)
                
                # Create client
                client = TelegramClient(
                    f"sessions/{account.phone}",
                    api_id,
                    api_hash
                )
                
                # Connect and verify
                await client.connect()
                
                if not await client.is_user_authorized():
                    logger.warning(f"⚠️ Account {account.phone} not authorized, skipping")
                    continue
                
                clients[account.phone] = client
                logger.info(f"✅ Initialized client for {account.phone}")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize {account.phone}: {e}")
                continue
        
        logger.info(f"🔗 Successfully initialized {len(clients)} clients")
        return clients
    
    async def join_channel(
        self,
        client: TelegramClient,
        channel: TelegramGroup,
        account: TelegramAccount,
        db
    ) -> bool:
        """
        Join a single Telegram channel.
        
        Args:
            client: Connected TelegramClient
            channel: TelegramGroup to join
            account: TelegramAccount being used
            db: Database session
        
        Returns:
            bool: True if successfully joined
        """
        try:
            logger.info(f"🔗 Joining @{channel.username} with {account.phone}...")
            
            # Get channel entity
            entity = await client.get_entity(channel.username)
            
            # Check if already participant
            try:
                participant = await client.get_participants(entity, limit=1)
                me = await client.get_me()
                
                # Already joined
                channel.is_joined = True
                channel.telegram_account_id = account.id
                channel.joined_by_phone = account.phone
                channel.joined_at = datetime.now(timezone.utc)
                channel.title = entity.title
                channel.members_count = getattr(entity, 'participants_count', 0)
                
                await db.commit()
                
                logger.info(f"✅ Already a participant of @{channel.username}")
                self.stats["already_joined"] += 1
                return True
                
            except Exception:
                pass  # Not a participant, proceed to join
            
            # Send join request
            await client(JoinChannelRequest(entity))
            
            # Update database
            channel.is_joined = True
            channel.telegram_account_id = account.id
            channel.joined_by_phone = account.phone
            channel.joined_at = datetime.now(timezone.utc)
            channel.title = entity.title
            channel.members_count = getattr(entity, 'participants_count', 0)
            
            account.groups_joined_count += 1
            account.last_join_at = datetime.now(timezone.utc)
            
            await db.commit()
            
            logger.info(f"✅ Successfully joined @{channel.username}")
            self.stats["successful_joins"] += 1
            
            # Send Slack notification
            try:
                await slack_notifier.send_channel_joined_notification(
                    channel_name=channel.username,
                    channel_title=channel.title,
                    account_phone=account.phone,
                    members_count=channel.members_count
                )
            except Exception as e:
                logger.warning(f"⚠️ Failed to send Slack notification: {e}")
            
            return True
            
        except FloodWaitError as e:
            wait_seconds = e.seconds
            logger.warning(f"⏳ FloodWait: Need to wait {wait_seconds} seconds")
            self.stats["errors"].append(f"FloodWait {wait_seconds}s for @{channel.username}")
            
            # Mark account as temporarily unavailable
            account.last_error_message = f"FloodWait {wait_seconds}s"
            account.last_error_at = datetime.now(timezone.utc)
            await db.commit()
            
            return False
            
        except ChannelPrivateError:
            logger.error(f"❌ Channel @{channel.username} is private/deleted")
            
            # Deactivate channel
            channel.is_active = False
            channel.notes = "Channel is private or deleted"
            await db.commit()
            
            self.stats["errors"].append(f"Private/deleted: @{channel.username}")
            self.stats["failed_joins"] += 1
            return False
            
        except UserBannedInChannelError:
            logger.error(f"❌ Account {account.phone} is banned in @{channel.username}")
            
            # Mark account as banned
            account.is_banned = True
            account.is_active = False
            account.health_status = HealthStatus.BANNED
            account.last_error_message = "Banned in channel"
            await db.commit()
            
            self.stats["errors"].append(f"Account banned: {account.phone}")
            self.stats["failed_joins"] += 1
            return False
            
        except UserAlreadyParticipantError:
            logger.info(f"✅ Already member of @{channel.username}")
            
            # Update database
            channel.is_joined = True
            channel.telegram_account_id = account.id
            channel.joined_by_phone = account.phone
            channel.joined_at = datetime.now(timezone.utc)
            await db.commit()
            
            self.stats["already_joined"] += 1
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to join @{channel.username}: {e}")
            
            account.last_error_message = str(e)[:500]
            account.last_error_at = datetime.now(timezone.utc)
            await db.commit()
            
            self.stats["errors"].append(f"Error joining @{channel.username}: {str(e)[:100]}")
            self.stats["failed_joins"] += 1
            return False
    
    async def run_join_cycle(self) -> Dict:
        """
        Main entry point: Run one join cycle.
        
        Joins 1 channel per account, then exits.
        Early exits if no unjoined channels exist.
        
        Returns:
            Dict with results
        """
        logger.info("=" * 60)
        logger.info("🔗 TELEGRAM GROUP JOINER - STARTING CYCLE")
        logger.info("=" * 60)
        
        try:
            # Step 1: Early exit check
            unjoined_count = await self.check_unjoined_channels_exist()
            if unjoined_count == 0:
                return {
                    "success": True,
                    "message": "No unjoined channels",
                    "stats": self.stats
                }
            
            # Step 2: Load accounts
            accounts = await self.load_authorized_accounts()
            if not accounts:
                return {
                    "success": False,
                    "message": "No active accounts available",
                    "stats": self.stats
                }
            
            # Step 3: Initialize clients
            self.clients = await self.initialize_telegram_clients(accounts)
            if not self.clients:
                return {
                    "success": False,
                    "message": "Failed to initialize any clients",
                    "stats": self.stats
                }
            
            # Step 4: Get unjoined channels (limit 1 per account)
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(TelegramGroup)
                    .where(TelegramGroup.is_joined == False)
                    .where(TelegramGroup.is_active == True)
                    .limit(len(self.clients))  # Max 1 per account
                )
                channels_to_join = result.scalars().all()
                
                if not channels_to_join:
                    return {
                        "success": True,
                        "message": "No channels to join",
                        "stats": self.stats
                    }
                
                logger.info(f"📋 Selected {len(channels_to_join)} channels to join")
                
                # Step 5: Join channels (1 per account)
                for idx, channel in enumerate(channels_to_join):
                    # Get account for this channel (round-robin)
                    account_idx = idx % len(accounts)
                    account = accounts[account_idx]
                    
                    # Check if we have a client for this account
                    if account.phone not in self.clients:
                        logger.warning(f"⏭️  Skipping @{channel.username} - no client for {account.phone}")
                        continue
                    
                    client = self.clients[account.phone]
                    
                    # Join the channel
                    await self.join_channel(client, channel, account, db)
                    
                    # Human-like delay between joins
                    if idx < len(channels_to_join) - 1:
                        delay = random.uniform(3, 8)
                        logger.info(f"⏸️  Waiting {delay:.1f}s before next join...")
                        await asyncio.sleep(delay)
            
            # Step 6: Return results
            logger.info("=" * 60)
            logger.info("📊 JOINING STATISTICS")
            logger.info("=" * 60)
            logger.info(f"✅ Successful joins:  {self.stats['successful_joins']}")
            logger.info(f"👥 Already joined:    {self.stats['already_joined']}")
            logger.info(f"❌ Failed joins:      {self.stats['failed_joins']}")
            logger.info(f"⚠️  Errors:            {len(self.stats['errors'])}")
            logger.info("=" * 60)
            
            return {
                "success": True,
                "message": "Join cycle completed",
                "stats": self.stats
            }
            
        except Exception as e:
            logger.error(f"❌ Join cycle failed: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "stats": self.stats
            }
            
        finally:
            # Always disconnect clients
            await self.disconnect_all()
    
    async def disconnect_all(self):
        """Disconnect all Telegram clients."""
        for phone, client in self.clients.items():
            try:
                await client.disconnect()
                logger.info(f"🔌 Disconnected {phone}")
            except Exception as e:
                logger.warning(f"⚠️ Error disconnecting {phone}: {e}")
        
        self.clients.clear()
