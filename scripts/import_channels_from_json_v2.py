#!/usr/bin/env python3
"""
Import channels from data1.json to PostgreSQL
Assigns channels to correct Telegram accounts (1-5)

Usage:
    python scripts/import_channels_from_json_v2.py                 # Import only (assign accounts)
    python scripts/import_channels_from_json_v2.py --join          # Join 1 group per AUTHORIZED account
"""

import sys
import json
import asyncio
import argparse
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import time
import random

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.models.telegram_group import TelegramGroup
from app.models.telegram_account import TelegramAccount

# Telegram imports (only if --join flag is used)
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UserBannedInChannelError,
    ChannelInvalidError, UserAlreadyParticipantError
)
from telethon.tl.types import Channel
from telethon.tl.functions.channels import JoinChannelRequest


class ChannelImporter:
    """Import channels from data1.json"""
    
    def __init__(self, json_file: str = "data1.json", join_groups: bool = False, limit: Optional[int] = None):
        self.json_file = Path(project_root) / json_file
        self.join_groups = join_groups
        self.limit = limit
        self.account_map = {}  # Maps account_number (1-5) -> phone/client
        self.clients = {}  # Telegram clients
        self.joined_count = 0  # Track successful joins
        self.stats = {
            "total": 0,
            "imported": 0,
            "skipped": 0,
            "updated": 0,
            "errors": 0,
            "joined": 0,
            "already_joined": 0,
            "join_failed": 0,
            "by_account": {}
        }
    
    def parse_account_number(self, account_str: Optional[str]) -> int:
        """
        Extract account number from "Account 1", "Account 2", etc.
        Returns 1 as default if not specified.
        """
        if not account_str:
            return 1  # Default to Account 1
        
        try:
            # Extract number from "Account 3" -> 3
            parts = account_str.strip().split()
            if len(parts) >= 2 and parts[0].lower() == "account":
                return int(parts[1])
            return 1
        except (ValueError, IndexError):
            return 1
    
    def extract_telegram_id(self, link: str) -> str:
        """
        Extract telegram username/ID from link
        https://t.me/bangalore_jobs -> bangalore_jobs
        https://t.me/s/cs_algo -> cs_algo
        """
        if not link:
            return ""
        
        # Remove trailing slashes
        link = link.rstrip("/")
        
        # Extract last part
        parts = link.split("/")
        telegram_id = parts[-1]
        
        # Handle /s/ links (public channels)
        if telegram_id == "s" and len(parts) >= 2:
            telegram_id = parts[-2] if parts[-2] != "s" else parts[-1]
        
        return telegram_id
    
    def normalize_category(self, category: Optional[str]) -> str:
        """Normalize category name"""
        if not category:
            return "general"
        
        category = category.lower().strip()
        
        # Map categories
        category_mapping = {
            "jobs": "jobs",
            "it": "tech",
            "tech": "tech",
            "ai": "tech",
            "programming": "tech",
            "design": "design",
            "internships": "internships",
            "recruitment": "recruitment",
            "remote": "remote",
            "general": "general"
        }
        
        return category_mapping.get(category, "general")
    
    async def channel_exists(self, session: AsyncSession, username: str) -> Optional[TelegramGroup]:
        """Check if channel already exists"""
        result = await session.execute(
            select(TelegramGroup).where(TelegramGroup.username == username)
        )
        return result.scalar_one_or_none()
    
    async def load_telegram_accounts(self, session: AsyncSession):
        """Load Telegram accounts from database for joining"""
        if not self.join_groups:
            return
        
        print("📋 Loading Telegram accounts for joining...")
        
        result = await session.execute(
            select(TelegramAccount)
            .where(
                and_(
                    TelegramAccount.is_active == True,
                    TelegramAccount.is_banned == False
                )
            )
            .order_by(TelegramAccount.phone)
        )
        accounts = result.scalars().all()
        
        if not accounts:
            print("⚠️  No active Telegram accounts found! Joining disabled.")
            self.join_groups = False
            return
        
        # Map account numbers 1-5 to account data (sorted by phone for consistency)
        sorted_accounts = sorted(accounts[:5], key=lambda x: x.phone)
        for idx, account in enumerate(sorted_accounts, 1):
            self.account_map[idx] = {
                "phone": account.phone,
                "api_id": account.api_id,
                "api_hash": account.api_hash
            }
            print(f"   Account {idx}: {account.phone}")
        
        print(f"✅ Loaded {len(self.account_map)} accounts")
        print()
    
    async def initialize_telegram_clients(self):
        """Initialize Telegram clients for joining"""
        if not self.join_groups or not self.account_map:
            return
        
        print("🔌 Initializing Telegram clients...")
        
        for account_num, account_data in self.account_map.items():
            phone = account_data["phone"]
            api_id = int(account_data["api_id"])
            api_hash = account_data["api_hash"]
            
            session_path = f"sessions/{phone}"
            session_file = Path(f"{session_path}.session")
            
            if not session_file.exists():
                print(f"⚠️  Session not found for Account {account_num} ({phone})")
                continue
            
            try:
                client = TelegramClient(session_path, api_id, api_hash)
                await client.connect()
                
                if not await client.is_user_authorized():
                    print(f"❌ Account {account_num} not authorized")
                    await client.disconnect()
                    continue
                
                me = await client.get_me()
                print(f"✅ Account {account_num}: {me.first_name} ({phone})")
                
                self.clients[account_num] = {
                    "client": client,
                    "phone": phone
                }
                
            except Exception as e:
                print(f"❌ Error initializing Account {account_num}: {e}")
        
        if self.clients:
            print(f"✅ {len(self.clients)} client(s) ready")
        else:
            print("⚠️  No clients initialized. Run: python3 login_telegram_accounts.py")
            self.join_groups = False
        
        print()
    
    async def join_telegram_group(self, group: TelegramGroup, session: AsyncSession) -> bool:
        """Actually join a Telegram group"""
        if not self.join_groups:
            return False
        
        account_num = group.joined_by_account_id
        
        # Skip if account not initialized
        if account_num not in self.clients:
            return False
        
        client_data = self.clients[account_num]
        client = client_data["client"]
        phone = client_data["phone"]
        username = group.username.lstrip('@')
        
        try:
            # Get entity
            entity = await asyncio.wait_for(
                client.get_entity(username),
                timeout=30
            )
            
            # Join if it's a channel
            if isinstance(entity, Channel):
                try:
                    await asyncio.wait_for(
                        client(JoinChannelRequest(entity)),
                        timeout=30
                    )
                except UserAlreadyParticipantError:
                    self.stats["already_joined"] += 1
                    group.is_joined = True
                    group.joined_by_account_id = account_num  # Set account ONLY when actually joined
                    group.joined_by_phone = phone
                    group.joined_at = datetime.utcnow()
                    await session.commit()
                    self.joined_count += 1  # Track already joined as successful
                    return True
            
            # Update group as joined
            group.is_joined = True
            group.joined_by_account_id = account_num  # Set account ONLY when actually joined
            group.joined_by_phone = phone
            group.joined_at = datetime.utcnow()
            group.title = entity.title if hasattr(entity, 'title') else username
            
            if hasattr(entity, 'participants_count'):
                group.members_count = entity.participants_count
            
            await session.commit()
            
            self.stats["joined"] += 1
            self.joined_count += 1  # Track successful join
            
            # Human-like rate limiting: 3-8 seconds between joins
            delay = random.uniform(3, 8)
            await asyncio.sleep(delay)
            
            return True
            
        except FloodWaitError as e:
            print(f"   ⏳ FloodWait {e.seconds}s for @{username}")
            self.stats["join_failed"] += 1
            return False
            
        except (ChannelPrivateError, UserBannedInChannelError, ChannelInvalidError) as e:
            group.is_active = False
            group.deactivation_reason = str(e)
            await session.commit()
            self.stats["join_failed"] += 1
            return False
            
        except Exception as e:
            self.stats["join_failed"] += 1
            return False
    
    async def import_channel(self, session: AsyncSession, channel_data: Dict, index: int = 0) -> bool:
        """Import single channel"""
        try:
            # Extract data
            name = channel_data.get("name", "")
            link = channel_data.get("link", "")
            category = channel_data.get("category", "general")
            account_used = channel_data.get("account_used", "")
            
            # Round-robin account assignment if account_used not specified
            if not account_used:
                # Distribute channels across 5 accounts (1-5)
                account_number = (index % 5) + 1
            else:
                account_number = self.parse_account_number(account_used)
            
            # Skip if missing critical data
            if not name or not link:
                print(f"⚠️  Skipping: Missing name or link - {channel_data}")
                self.stats["skipped"] += 1
                return False
            
            # Parse data
            telegram_id = self.extract_telegram_id(link)
            normalized_category = self.normalize_category(category)
            
            if not telegram_id:
                print(f"⚠️  Skipping {name}: Could not extract telegram ID from {link}")
                self.stats["skipped"] += 1
                return False
            
            # Check if exists
            existing = await self.channel_exists(session, telegram_id)
            
            if existing:
                # Update existing channel
                existing.title = name
                existing.url = link
                existing.category = normalized_category
                # DO NOT set joined_by_account_id here - only set when actually joined
                existing.is_active = True
                existing.updated_at = datetime.utcnow()
                
                print(f"🔄 Updated: {name} (Account {account_number})", end="")
                self.stats["updated"] += 1
                
                # Join group if --join flag is set AND account has client ready
                if self.join_groups and account_number in self.clients:
                    print(f" → [{account_number}] Joining...", end=" ", flush=True)
                    # Set account_id BEFORE attempting join
                    existing.joined_by_account_id = account_number
                    await session.commit()
                    
                    if await self.join_telegram_group(existing, session):
                        print("✅ Joined")
                    else:
                        # Join failed - clear account_id AND phone
                        existing.joined_by_account_id = None
                        existing.joined_by_phone = None
                        existing.is_joined = False
                        await session.commit()
                        print("❌ Failed")
                elif self.join_groups and account_number not in self.clients:
                    print()  # New line if account not available
                else:
                    print()
                    
            else:
                # Create new channel
                new_channel = TelegramGroup(
                    username=telegram_id,
                    title=name,
                    url=link,
                    category=normalized_category,
                    # DO NOT set joined_by_account_id here - only set when actually joined
                    is_active=True,
                    is_joined=False,
                    members_count=0,
                    description="",
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(new_channel)
                
                print(f"✅ Imported: {name} → Account {account_number} ({normalized_category})", end="")
                self.stats["imported"] += 1
                
                # Commit to get ID before joining
                await session.commit()
                
                # Join group if --join flag is set AND account has client ready
                if self.join_groups and account_number in self.clients:
                    print(f" → [{account_number}] Joining...", end=" ", flush=True)
                    # Set account_id BEFORE attempting join
                    new_channel.joined_by_account_id = account_number
                    await session.commit()
                    
                    if await self.join_telegram_group(new_channel, session):
                        print("✅ Joined")
                    else:
                        # Join failed - clear account_id AND phone, reset is_joined
                        new_channel.joined_by_account_id = None
                        new_channel.joined_by_phone = None
                        new_channel.is_joined = False
                        await session.commit()
                        print("❌ Failed")
                elif self.join_groups and account_number not in self.clients:
                    print()  # New line if account not available
                else:
                    print()
            
            # Track by account
            account_key = f"Account {account_number}"
            self.stats["by_account"][account_key] = self.stats["by_account"].get(account_key, 0) + 1
            
            return True
            
        except Exception as e:
            print(f"❌ Error importing {channel_data.get('name', 'unknown')}: {str(e)}")
            self.stats["errors"] += 1
            return False
    
    async def run(self):
        """Main import logic"""
        start_time = time.time()
        
        async with AsyncSessionLocal() as session:
            # If --join flag, join 1 group per AUTHORIZED account
            if self.join_groups:
                print("=" * 70)
                print("📥 Joining Groups (1 per Authorized Account)")
                print("=" * 70)
                print()
                
                # Load Telegram accounts if joining
                await self.load_telegram_accounts(session)
                
                # Initialize Telegram clients if joining
                if self.join_groups:
                    await self.initialize_telegram_clients()
                
                if not self.clients:
                    print("⚠️  No authorized accounts! Run: python3 login_telegram_accounts.py")
                    return
                
                print(f"🔗 Ready to join with {len(self.clients)} authorized accounts")
                print()
                
                print("🔄 Fetching unjoined groups from database...")
                result = await session.execute(
                    select(TelegramGroup)
                    .where(TelegramGroup.is_joined == False)
                    .order_by(TelegramGroup.id)
                )
                unjoined_groups = result.scalars().all()
                
                if not unjoined_groups:
                    print("✅ All groups are already joined!")
                    return
                
                # Shuffle for randomness
                random.shuffle(unjoined_groups)
                
                print(f"📊 Found {len(unjoined_groups)} unjoined groups")
                print()
                
                # Join 1 group per authorized account
                joined_count = 0
                for account_num in sorted(self.clients.keys()):
                    if not unjoined_groups:
                        print("✅ No more unjoined groups")
                        break
                    
                    # Pick random unjoined group for this account
                    group_to_join = unjoined_groups.pop(0)
                    phone = self.clients[account_num]["phone"]
                    
                    print(f"[Account {account_num} | {phone}]")
                    print(f"  🎯 Selected: @{group_to_join.username}")
                    print(f"  🔗 Attempting join...", end=" ", flush=True)
                    
                    group_to_join.joined_by_account_id = account_num
                    
                    if await self.join_telegram_group(group_to_join, session):
                        print(f"✅ Joined")
                        joined_count += 1
                    else:
                        group_to_join.joined_by_account_id = None
                        await session.commit()
                        print(f"❌ Failed")
                    
                    print()
                
                print("=" * 70)
                print(f"✅ Summary: Successfully joined {joined_count}/{len(self.clients)} groups")
                print("=" * 70)
                print()
                print("🕐 Run again tomorrow to join more groups!")
                print()
                
                await session.commit()
                return
        
        # Original import logic if NOT --join flag
        print("=" * 70)
        print("📥 Channel Import from data1.json")
        print("=" * 70)
        print()
        
        # Load JSON
        if not self.json_file.exists():
            print(f"❌ File not found: {self.json_file}")
            return
        
        print(f"📂 Loading: {self.json_file}")
        with open(self.json_file, 'r', encoding='utf-8') as f:
            channels_data = json.load(f)
        
        print(f"📊 Found {len(channels_data)} channels in JSON")
        
        self.stats["total"] = len(channels_data)
        print()
        
        # Import to PostgreSQL
        async with AsyncSessionLocal() as session:
            try:
                print("🔄 Importing to PostgreSQL...")
                print()
                
                for idx, channel_data in enumerate(channels_data):
                    await self.import_channel(session, channel_data, idx)
                    
                    # Progress indicator every 5 channels (more frequent for joining)
                    if self.join_groups and (idx + 1) % 5 == 0:
                        elapsed = time.time() - start_time
                        avg_time = elapsed / (idx + 1) if idx > 0 else 0
                        remaining = (self.stats['total'] - (idx + 1)) * avg_time
                        print(f"   Progress: {idx + 1}/{self.stats['total']} | ~{remaining/60:.1f} min remaining")
                    elif not self.join_groups and (idx + 1) % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = (idx + 1) / elapsed if elapsed > 0 else 0
                        print(f"   Progress: {idx + 1}/{self.stats['total']} | {rate:.1f}/sec")
                
                # Commit all changes
                await session.commit()
                print()
                print("✅ All changes committed to PostgreSQL!")
                
            except Exception as e:
                await session.rollback()
                print(f"\n❌ Error during import: {str(e)}")
                import traceback
                traceback.print_exc()
                raise
            finally:
                # Disconnect Telegram clients
                if self.clients:
                    print("\n🔌 Disconnecting Telegram clients...")
                    for account_num, client_data in self.clients.items():
                        await client_data["client"].disconnect()
        
        # Print summary
        self.print_summary(time.time() - start_time)
    
    def print_summary(self, duration: float):
        """Print import summary"""
        print()
        print("=" * 70)
        print("📊 IMPORT SUMMARY")
        print("=" * 70)
        print()
        print(f"⏱️  Duration: {duration:.1f} seconds")
        print()
        print(f"Total channels processed: {self.stats['total']}")
        print(f"✅ Imported (new):        {self.stats['imported']}")
        print(f"🔄 Updated (existing):    {self.stats['updated']}")
        print(f"⚠️  Skipped:              {self.stats['skipped']}")
        print(f"❌ Errors:                {self.stats['errors']}")
        print()
        
        if self.join_groups:
            print("🔗 Joining Stats:")
            total_joined = self.stats['joined'] + self.stats['already_joined']
            print(f"   ✅ Successfully joined:  {self.stats['joined']}")
            print(f"   ✓  Already members:      {self.stats['already_joined']}")
            print(f"   ❌ Join failed:          {self.stats['join_failed']}")
            print(f"   📊 Total joined:         {total_joined}/{self.stats['total']}")
            if self.stats['total'] > 0:
                success_rate = (total_joined / self.stats['total']) * 100
                print(f"   📈 Success rate:         {success_rate:.1f}%")
            print()
        
        if self.stats["by_account"]:
            print("📱 Channels by Account:")
            print("-" * 40)
            for account, count in sorted(self.stats["by_account"].items()):
                print(f"  {account}: {count} channels")
            print()
        
        # Next steps
        print("=" * 70)
        print()
        
        if self.join_groups:
            total_joined = self.stats['joined'] + self.stats['already_joined']
            if total_joined > 0:
                print("✅ Next Steps:")
                print()
                print("1. Verify joined groups:")
                print("   SELECT username, joined_by_account_id, joined_by_phone, is_joined")
                print("   FROM telegram_groups WHERE is_joined = true LIMIT 10;")
                print()
                print("2. Start scraping:")
                print("   python3 scripts/run_telegram_scraper.py")
                print()
        else:
            print("✅ Next Step: Join groups with:")
            print("   python3 scripts/import_channels_from_json_v2.py --join")
            print()
            print("   Or use dedicated joining script:")
            print("   python3 join_telegram_groups.py")
            print()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Import Telegram channels")
    parser.add_argument(
        "--join",
        action="store_true",
        help="Actually join the groups via Telegram API"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of channels to process"
    )
    
    args = parser.parse_args()
    
    importer = ChannelImporter(
        json_file="data1.json",
        join_groups=args.join,
        limit=args.limit
    )
    
    asyncio.run(importer.run())


if __name__ == "__main__":
    main()
