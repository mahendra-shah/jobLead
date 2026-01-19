"""
Channel Synchronization Service
Keeps Postg    async def sync_channel_to_mongo(
        self, 
        channel: TelegramGroup,QL and MongoDB channels in sync for Lambda access.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.telegram_group import TelegramGroup
from app.services.mongodb_storage_service import MongoDBStorageService
from app.config import settings

logger = logging.getLogger(__name__)


class ChannelSyncService:
    """Synchronizes channels between PostgreSQL (source of truth) and MongoDB (Lambda reads)."""
    
    def __init__(self):
        self.mongo = MongoDBStorageService()
        self._initialized = False
    
    async def initialize(self):
        """Initialize MongoDB connection and create indexes for channels collection."""
        if self._initialized:
            return
        
        await self.mongo.initialize()
        
        # Create indexes for channels collection
        try:
            await self.mongo.db.channels.create_index("channel_id", unique=True)
            await self.mongo.db.channels.create_index("username", unique=True)
            await self.mongo.db.channels.create_index("is_active")
            await self.mongo.db.channels.create_index("last_fetched_at")
            await self.mongo.db.channels.create_index([("is_active", 1), ("error_count", 1)])
            
            logger.info("✅ Channel sync service initialized")
            self._initialized = True
            
        except Exception as e:
            logger.warning(f"Index creation warning (may already exist): {e}")
            self._initialized = True
    
    async def sync_channel_to_mongo(
        self,
        channel: TelegramGroup,
        operation: str = "upsert"  # upsert, delete
    ) -> bool:
        """
        Sync a single channel from PostgreSQL to MongoDB.
        
        Args:
            channel: SQLAlchemy TelegramGroup model
            operation: 'upsert' or 'delete'
        
        Returns:
            bool: Success status
        """
        try:
            await self.initialize()
            
            if operation == "delete":
                result = await self.mongo.db.channels.delete_one(
                    {"channel_id": str(channel.id)}
                )
                logger.info(f"🗑️  Deleted channel {channel.username} from MongoDB")
                return result.deleted_count > 0
            
            # Upsert (insert or update)
            channel_doc = {
                "channel_id": str(channel.id),
                "telegram_id": None,  # Will be populated by Telegram API
                "name": channel.title or channel.username,
                "username": channel.username,
                "description": channel.description or "",
                "is_active": channel.is_active,
                "category": channel.category or "general",
                "last_fetched_at": channel.last_scraped_at.isoformat() if channel.last_scraped_at else None,
                "last_message_id": channel.last_message_id or 0,
                "fetch_count": channel.total_messages_scraped,
                "job_count": channel.job_messages_found,
                "score": channel.health_score,
                "created_at": channel.created_at.isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "synced_from_postgres": True,
                "error_state": None,  # For tracking Lambda errors
                "error_count": 0
            }
            
            result = await self.mongo.db.channels.update_one(
                {"channel_id": str(channel.id)},
                {"$set": channel_doc},
                upsert=True
            )
            
            action = "Updated" if result.modified_count > 0 else "Inserted"
            logger.info(f"✅ {action} channel {channel.username} in MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to sync channel {channel.username} to MongoDB: {e}")
            return False
    
    async def sync_all_channels(self, db: AsyncSession) -> Dict[str, int]:
        """
        Sync all channels from PostgreSQL to MongoDB.
        Useful for initial setup or fixing inconsistencies.
        
        Args:
            db: AsyncSession for PostgreSQL
        
        Returns:
            Dict with sync statistics
        """
        try:
            await self.initialize()
            
            # Get all channels from PostgreSQL
            result = await db.execute(select(TelegramGroup))
            channels = result.scalars().all()
            
            stats = {
                "total": len(channels),
                "synced": 0,
                "failed": 0
            }
            
            for channel in channels:
                success = await self.sync_channel_to_mongo(channel)
                if success:
                    stats["synced"] += 1
                else:
                    stats["failed"] += 1
            
            logger.info(
                f"✅ Channel sync complete: "
                f"{stats['synced']}/{stats['total']} synced, "
                f"{stats['failed']} failed"
            )
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to sync all channels: {e}")
            raise
    
    async def get_active_channels_for_lambda(self, limit: Optional[int] = None) -> list:
        """
        Get active channels for Lambda to scrape.
        Excludes channels with 5+ consecutive errors.
        
        Args:
            limit: Optional limit on number of channels
        
        Returns:
            List of channel documents
        """
        try:
            await self.initialize()
            
            query = {
                "is_active": True,
                "error_count": {"$lt": 5}  # Exclude channels with 5+ errors
            }
            
            cursor = self.mongo.db.channels.find(query)
            if limit:
                cursor = cursor.limit(limit)
            
            channels = await cursor.to_list(length=limit)
            
            logger.info(f"📋 Found {len(channels)} active channels for Lambda")
            return channels
            
        except Exception as e:
            logger.error(f"❌ Failed to get active channels: {e}")
            return []
    
    async def update_channel_fetch_status(
        self,
        channel_id: str,
        last_message_id: Optional[int] = None,
        fetch_count: int = 0,
        job_count: int = 0,
        error: Optional[str] = None
    ) -> bool:
        """
        Update channel fetch status after Lambda run.
        
        Args:
            channel_id: Channel UUID
            last_message_id: Last Telegram message ID fetched
            fetch_count: Number of messages fetched this run
            job_count: Number of jobs found this run
            error: Error message if scraping failed
        
        Returns:
            bool: Success status
        """
        try:
            await self.initialize()
            
            update_doc = {
                "last_fetched_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            inc_doc = {}
            
            if last_message_id is not None:
                update_doc["last_message_id"] = last_message_id
            
            if fetch_count > 0:
                # Increment total counts
                inc_doc["fetch_count"] = fetch_count
                inc_doc["job_count"] = job_count
            
            if error:
                update_doc["error_state"] = error
                inc_doc["error_count"] = 1
            else:
                # Clear error state on success
                update_doc["error_state"] = None
                update_doc["error_count"] = 0
            
            # Calculate score
            channel = await self.mongo.db.channels.find_one({"channel_id": channel_id})
            if channel:
                total_fetch = channel.get("fetch_count", 0) + fetch_count
                total_jobs = channel.get("job_count", 0) + job_count
                if total_fetch > 0:
                    update_doc["score"] = total_jobs / total_fetch
            
            # Build update operation
            update_operation = {"$set": update_doc}
            if inc_doc:
                update_operation["$inc"] = inc_doc
            
            result = await self.mongo.db.channels.update_one(
                {"channel_id": channel_id},
                update_operation
            )
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"❌ Failed to update channel status: {e}")
            return False
    
    async def close(self):
        """Close MongoDB connection."""
        await self.mongo.close()


# Singleton instance
channel_sync_service = ChannelSyncService()
