#!/usr/bin/env python3
"""
Quick sync script - Sync telegram_groups to MongoDB channels collection
"""

import asyncio
import os
import sys
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import select

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.db.session import AsyncSessionLocal
from app.models.telegram_group import TelegramGroup

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "placement_db")


async def sync_channels():
    """Sync all channels from PostgreSQL to MongoDB"""
    
    print("🔄 Quick Channel Sync to MongoDB")
    print("=" * 60)
    
    # Connect to MongoDB
    print("\n📡 Connecting to MongoDB...")
    mongo_client = AsyncIOMotorClient(MONGODB_URI)
    db = mongo_client[MONGODB_DATABASE]
    channels_collection = db["channels"]
    
    try:
        # Test connection
        await mongo_client.admin.command('ping')
        print("✅ MongoDB connected")
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return
    
    # Get channels from PostgreSQL
    print("\n📊 Fetching channels from PostgreSQL...")
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TelegramGroup).where(TelegramGroup.is_active.is_(True))
        )
        channels = result.scalars().all()
        
        print(f"✅ Found {len(channels)} active channels")
    
    # Sync to MongoDB
    print(f"\n🔄 Syncing {len(channels)} channels to MongoDB...")
    synced = 0
    failed = 0
    
    for channel in channels:
        try:
            # Create MongoDB document
            channel_doc = {
                "username": channel.username,
                "title": channel.title,
                "url": channel.url,
                "category": channel.category,
                "is_active": channel.is_active,
                "is_joined": channel.is_joined,
                "members_count": channel.members_count or 0,
                "description": channel.description or "",
                "last_scraped_at": channel.last_scraped_at,
                "last_message_id": channel.last_message_id,
                "last_message_date": channel.last_message_date,
                "health_score": channel.health_score,
                "total_messages_scraped": channel.total_messages_scraped,
                "job_messages_found": channel.job_messages_found
            }
            
            # Upsert to MongoDB
            await channels_collection.update_one(
                {"username": channel.username},
                {"$set": channel_doc},
                upsert=True
            )
            
            synced += 1
            if synced % 10 == 0:
                print(f"   Progress: {synced}/{len(channels)} channels synced")
                
        except Exception as e:
            print(f"   ❌ Failed to sync {channel.username}: {e}")
            failed += 1
    
    print("\n✅ Sync complete!")
    print(f"   Synced: {synced}")
    print(f"   Failed: {failed}")
    
    # Verify
    count = await channels_collection.count_documents({})
    print(f"\n📊 MongoDB channels collection now has: {count} documents")
    
    mongo_client.close()
    print("\n" + "=" * 60)
    print("✅ Channels are ready for Lambda scraping!")


if __name__ == "__main__":
    asyncio.run(sync_channels())
