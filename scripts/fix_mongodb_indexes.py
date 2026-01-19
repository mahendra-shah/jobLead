#!/usr/bin/env python3
"""
Fix MongoDB indexes - Drop old channel_id index and create new ones
"""

import asyncio
import os
import sys
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "placement_db")


async def fix_indexes():
    """Drop old indexes and create new ones"""
    
    print("🔧 Fixing MongoDB Indexes")
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
    
    # List existing indexes
    print("\n📋 Current indexes:")
    indexes = await channels_collection.list_indexes().to_list(length=None)
    for idx in indexes:
        print(f"   - {idx['name']}: {idx.get('key', {})}")
    
    # Drop old channel_id index
    print("\n🗑️  Dropping old channel_id index...")
    try:
        await channels_collection.drop_index("channel_id_1")
        print("✅ Dropped channel_id_1 index")
    except Exception as e:
        if "index not found" in str(e).lower():
            print("   ℹ️  Index already doesn't exist")
        else:
            print(f"   ⚠️  Could not drop: {e}")
    
    # Drop all indexes and recreate (safer approach)
    print("\n🗑️  Dropping all indexes except _id...")
    try:
        await channels_collection.drop_indexes()
        print("✅ All indexes dropped")
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
    
    # Create new indexes
    print("\n📝 Creating new indexes...")
    
    # 1. Username unique index
    try:
        await channels_collection.create_index("username", unique=True)
        print("✅ Created unique index on username")
    except Exception as e:
        print(f"   ⚠️  Username index error: {e}")
    
    # 2. Category index
    try:
        await channels_collection.create_index("category")
        print("✅ Created index on category")
    except Exception as e:
        print(f"   ⚠️  Category index error: {e}")
    
    # 3. is_active index
    try:
        await channels_collection.create_index("is_active")
        print("✅ Created index on is_active")
    except Exception as e:
        print(f"   ⚠️  is_active index error: {e}")
    
    # 4. last_scraped_at index (for scheduling)
    try:
        await channels_collection.create_index("last_scraped_at")
        print("✅ Created index on last_scraped_at")
    except Exception as e:
        print(f"   ⚠️  last_scraped_at index error: {e}")
    
    # List new indexes
    print("\n📋 New indexes:")
    indexes = await channels_collection.list_indexes().to_list(length=None)
    for idx in indexes:
        print(f"   - {idx['name']}: {idx.get('key', {})}")
    
    # Check document count
    count = await channels_collection.count_documents({})
    print(f"\n📊 Documents in collection: {count}")
    
    if count > 0:
        print("\n⚠️  Found existing documents - clearing collection...")
        result = await channels_collection.delete_many({})
        print(f"✅ Deleted {result.deleted_count} old documents")
    
    mongo_client.close()
    print("\n" + "=" * 60)
    print("✅ MongoDB indexes fixed! Ready for channel sync.")


if __name__ == "__main__":
    asyncio.run(fix_indexes())
