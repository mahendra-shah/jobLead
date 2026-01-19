#!/usr/bin/env python3
"""
Import channels from data1.json to PostgreSQL
Assigns channels to correct Telegram accounts (1-6)

Usage:
    python scripts/import_channels_from_json.py
"""

import sys
import json
import asyncio
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.models.telegram_group import TelegramGroup


class ChannelImporter:
    """Import channels from data1.json"""
    
    def __init__(self, json_file: str = "data1.json"):
        self.json_file = Path(project_root) / json_file
        self.stats = {
            "total": 0,
            "imported": 0,
            "skipped": 0,
            "updated": 0,
            "errors": 0,
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
    
    async def import_channel(self, session: AsyncSession, channel_data: Dict) -> bool:
        """Import single channel"""
        try:
            # Extract data
            name = channel_data.get("name", "")
            link = channel_data.get("link", "")
            category = channel_data.get("category", "general")
            account_used = channel_data.get("account_used", "")
            
            # Skip if missing critical data
            if not name or not link:
                print(f"⚠️  Skipping: Missing name or link - {channel_data}")
                self.stats["skipped"] += 1
                return False
            
            # Parse data
            telegram_id = self.extract_telegram_id(link)
            account_number = self.parse_account_number(account_used)
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
                existing.joined_by_account_id = None  # Will set account ID later
                existing.is_active = True
                existing.updated_at = datetime.utcnow()
                
                print(f"🔄 Updated: {name} (Account {account_number})")
                self.stats["updated"] += 1
            else:
                # Create new channel
                new_channel = TelegramGroup(
                    username=telegram_id,
                    title=name,
                    url=link,
                    category=normalized_category,
                    is_active=True,
                    is_joined=False,
                    members_count=0,
                    description="",
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(new_channel)
                
                print(f"✅ Imported: {name} → Account {account_number} ({normalized_category})")
                self.stats["imported"] += 1
            
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
        print("=" * 60)
        print("📥 Channel Import from data1.json")
        print("=" * 60)
        print()
        
        # Load JSON
        if not self.json_file.exists():
            print(f"❌ File not found: {self.json_file}")
            return
        
        print(f"📂 Loading: {self.json_file}")
        with open(self.json_file, 'r', encoding='utf-8') as f:
            channels_data = json.load(f)
        
        self.stats["total"] = len(channels_data)
        print(f"📊 Found {self.stats['total']} channels in JSON")
        print()
        
        # Import to PostgreSQL
        async with AsyncSessionLocal() as session:
            try:
                print("🔄 Importing to PostgreSQL...")
                print()
                
                for idx, channel_data in enumerate(channels_data, 1):
                    await self.import_channel(session, channel_data)
                    
                    # Progress indicator every 10 channels
                    if idx % 10 == 0:
                        print(f"   Progress: {idx}/{self.stats['total']} channels processed")
                
                # Commit all changes
                await session.commit()
                print()
                print("✅ All changes committed to PostgreSQL!")
                
            except Exception as e:
                await session.rollback()
                print(f"\n❌ Error during import: {str(e)}")
                raise
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print import summary"""
        print()
        print("=" * 60)
        print("📊 IMPORT SUMMARY")
        print("=" * 60)
        print()
        print(f"Total channels in JSON:  {self.stats['total']}")
        print(f"✅ Imported (new):       {self.stats['imported']}")
        print(f"🔄 Updated (existing):   {self.stats['updated']}")
        print(f"⚠️  Skipped:             {self.stats['skipped']}")
        print(f"❌ Errors:               {self.stats['errors']}")
        print()
        
        if self.stats["by_account"]:
            print("📱 Channels by Account:")
            print("-" * 40)
            for account, count in sorted(self.stats["by_account"].items()):
                print(f"  {account}: {count} channels")
            print()
        
        print("=" * 60)
        print()
        
        # Next steps
        total_success = self.stats['imported'] + self.stats['updated']
        if total_success > 0:
            print("✅ Next Steps:")
            print()
            print("1. Sync to MongoDB for Lambda:")
            print("   python -c \"from app.services.channel_sync_service import sync_channels_to_mongodb; import asyncio; asyncio.run(sync_channels_to_mongodb())\"")
            print()
            print("2. Copy session files to Lambda:")
            print("   cp app/sessions/*.session lambda/telegram_scraper/sessions/")
            print()
            print("3. Restart Docker to see new APIs:")
            print("   docker-compose down && docker-compose build && docker-compose up -d")
            print()
            print("4. Test Lambda scraping:")
            print("   aws lambda invoke --function-name placement-channel-batcher output.json")
            print()


async def main():
    """Main entry point"""
    importer = ChannelImporter("data1.json")
    await importer.run()


if __name__ == "__main__":
    asyncio.run(main())
