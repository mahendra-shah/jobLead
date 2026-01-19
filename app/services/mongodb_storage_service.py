"""MongoDB storage service for raw Telegram messages."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

from app.config import settings

logger = logging.getLogger(__name__)


class MongoDBStorageService:
    """
    MongoDB storage service for raw Telegram messages.
    
    Stores messages in MongoDB Atlas with proper indexing for:
    - Fast lookups by message_id
    - Efficient queries for pending messages
    - Time-based queries for processing
    """
    
    def __init__(self):
        """Initialize MongoDB connection."""
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None
        self.collection = None
        self._initialized = False
    
    async def initialize(self):
        """
        Connect to MongoDB and set up collection.
        Optimized for M0 free tier (100 connection limit).
        """
        if self._initialized:
            return
        
        try:
            # Build connection string with URL-encoded credentials
            # Format matches MongoDB Atlas connection string exactly
            from urllib.parse import quote_plus
            
            username = quote_plus(settings.MONGODB_USERNAME)
            password = quote_plus(settings.MONGODB_PASSWORD)
            cluster = settings.MONGODB_CLUSTER
            
            connection_string = (
                f"mongodb+srv://{username}:{password}@{cluster}/"
                f"?retryWrites=true&w=majority&appName=Cluster0"
            )
            
            # Connect to MongoDB with optimized pool settings for M0 free tier
            self.client = AsyncIOMotorClient(
                connection_string,
                maxPoolSize=10,        # Conservative for M0 free tier (100 conn limit)
                minPoolSize=2,         # Keep 2 connections warm
                maxIdleTimeMS=45000,   # Close idle connections after 45s
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=20000,
                retryWrites=True,
                w='majority'
            )
            
            # Get database and collection
            self.db = self.client[settings.MONGODB_DATABASE]
            self.collection = self.db[settings.MONGODB_COLLECTION]
            
            # Create indexes for performance
            await self._create_indexes()
            
            self._initialized = True
            logger.info(f"✅ Connected to MongoDB: {settings.MONGODB_DATABASE}.{settings.MONGODB_COLLECTION} (pool: 2-10 connections)")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
    
    async def _create_indexes(self):
        """Create indexes for efficient queries."""
        try:
            # Unique index on message_id
            await self.collection.create_index("message_id", unique=True)
            
            # Compound index for pending messages
            await self.collection.create_index([
                ("processed", 1),
                ("timestamp", -1)
            ])
            
            # Index for channel queries
            await self.collection.create_index("channel_id")
            
            # Index for time-based queries
            await self.collection.create_index("timestamp")
            
            # TTL Index: Auto-delete messages after 7 days
            await self.collection.create_index(
                "created_at",
                expireAfterSeconds=604800,  # 7 days = 604800 seconds
                background=True
            )
            
            logger.info("✅ MongoDB indexes created (including TTL: 7 days)")
            
        except Exception as e:
            logger.warning(f"Index creation error (may already exist): {e}")
    
    async def save_message(self, message_data: Dict) -> bool:
        """
        Save a raw Telegram message to MongoDB.
        
        Args:
            message_data: Dictionary containing message fields:
                - message_id (str): Unique message ID
                - channel_id (str): Telegram channel ID
                - content (str): Message text
                - timestamp (str): Message timestamp
                - processed (bool): Processing status
                - is_job (bool, optional): ML classification result
                - job_id (str, optional): Created job UUID
                - confidence (float, optional): ML confidence score
        
        Returns:
            bool: True if saved successfully, False if duplicate
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            # Add metadata
            message_data['created_at'] = datetime.utcnow()
            message_data['updated_at'] = datetime.utcnow()
            
            # Insert document
            await self.collection.insert_one(message_data)
            logger.info(f"💾 Saved message {message_data['message_id']} to MongoDB")
            return True
            
        except DuplicateKeyError:
            logger.debug(f"Message {message_data['message_id']} already exists")
            return False
        except Exception as e:
            logger.error(f"Error saving message to MongoDB: {e}")
            raise
    
    async def get_pending_messages(self, limit: int = 50) -> List[Dict]:
        """
        Get unprocessed messages from MongoDB.
        
        Args:
            limit: Maximum number of messages to retrieve
        
        Returns:
            List of message dictionaries
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            cursor = self.collection.find(
                {"processed": False}
            ).sort("timestamp", -1).limit(limit)
            
            messages = await cursor.to_list(length=limit)
            
            logger.info(f"📥 Retrieved {len(messages)} pending messages from MongoDB")
            return messages
            
        except Exception as e:
            logger.error(f"Error fetching pending messages: {e}")
            raise
    
    async def mark_as_processed(
        self,
        message_id: str,
        is_job: bool,
        job_id: Optional[str] = None,
        confidence: Optional[float] = None
    ) -> bool:
        """
        Mark a message as processed with ML results.
        
        Args:
            message_id: Unique message ID
            is_job: Whether message is a job posting
            job_id: UUID of created job (if applicable)
            confidence: ML confidence score
        
        Returns:
            bool: True if updated successfully
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            update_data = {
                "processed": True,
                "is_job": is_job,
                "updated_at": datetime.utcnow()
            }
            
            if job_id:
                update_data["job_id"] = job_id
            if confidence is not None:
                update_data["confidence"] = confidence
            
            result = await self.collection.update_one(
                {"message_id": message_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.debug(f"✅ Marked message {message_id} as processed")
                return True
            else:
                logger.warning(f"Message {message_id} not found for update")
                return False
                
        except Exception as e:
            logger.error(f"Error marking message as processed: {e}")
            raise
    
    async def mark_processed(self, message_id: str, job_id: str) -> bool:
        """
        Mark a message as processed with created job ID.
        Alias for mark_as_processed with is_job=True.
        
        Args:
            message_id: Unique message ID
            job_id: UUID of created job
        
        Returns:
            bool: True if updated successfully
        """
        return await self.mark_as_processed(message_id, is_job=True, job_id=job_id)
    
    async def mark_rejected(
        self,
        message_id: str,
        reason: str = "not_a_job",
        confidence: Optional[float] = None
    ) -> bool:
        """
        Mark a message as rejected (not a job).
        
        Args:
            message_id: Unique message ID
            reason: Reason for rejection
            confidence: ML confidence score
        
        Returns:
            bool: True if updated successfully
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            update_data = {
                "processed": True,
                "is_job": False,
                "rejection_reason": reason,
                "updated_at": datetime.utcnow()
            }
            
            if confidence is not None:
                update_data["confidence"] = confidence
            
            result = await self.collection.update_one(
                {"message_id": message_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.debug(f"✅ Marked message {message_id} as rejected ({reason})")
                return True
            else:
                logger.warning(f"Message {message_id} not found for update")
                return False
                
        except Exception as e:
            logger.error(f"Error marking message as rejected: {e}")
            raise
    
    async def mark_as_duplicate(self, message_id: str, duplicate_job_id: str) -> bool:
        """
        Mark a message as duplicate of an existing job.
        
        Args:
            message_id: Unique message ID
            duplicate_job_id: ID of the existing job this duplicates
        
        Returns:
            bool: True if updated successfully
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            update_data = {
                "processed": True,
                "is_job": True,
                "is_duplicate": True,
                "duplicate_of_job_id": duplicate_job_id,
                "updated_at": datetime.utcnow()
            }
            
            result = await self.collection.update_one(
                {"message_id": message_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.debug(f"✅ Marked message {message_id} as duplicate of job {duplicate_job_id}")
                return True
            else:
                logger.warning(f"Message {message_id} not found for update")
                return False
                
        except Exception as e:
            logger.error(f"Error marking message as duplicate: {e}")
            raise
    
    async def get_message_by_id(self, message_id: str) -> Optional[Dict]:
        """
        Get a specific message by ID.
        
        Args:
            message_id: Unique message ID
        
        Returns:
            Message dictionary or None
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            message = await self.collection.find_one({"message_id": message_id})
            return message
        except Exception as e:
            logger.error(f"Error fetching message: {e}")
            raise
    
    async def get_processing_stats(self, days: int = 7) -> Dict:
        """
        Get processing statistics for the last N days.
        
        Args:
            days: Number of days to look back
        
        Returns:
            Dictionary with statistics
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Total messages
            total = await self.collection.count_documents({
                "timestamp": {"$gte": cutoff_date.isoformat()}
            })
            
            # Processed messages
            processed = await self.collection.count_documents({
                "timestamp": {"$gte": cutoff_date.isoformat()},
                "processed": True
            })
            
            # Pending messages
            pending = await self.collection.count_documents({
                "timestamp": {"$gte": cutoff_date.isoformat()},
                "processed": False
            })
            
            # Job messages
            jobs = await self.collection.count_documents({
                "timestamp": {"$gte": cutoff_date.isoformat()},
                "is_job": True
            })
            
            # Non-job messages
            non_jobs = await self.collection.count_documents({
                "timestamp": {"$gte": cutoff_date.isoformat()},
                "is_job": False
            })
            
            return {
                "total_messages": total,
                "processed_count": processed,
                "pending_count": pending,
                "job_count": jobs,
                "non_job_count": non_jobs,
                "processing_rate": round(processed / total * 100, 1) if total > 0 else 0,
                "days": days
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            raise
    
    async def get_messages_by_channel(
        self,
        channel_id: str,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get messages from a specific channel.
        
        Args:
            channel_id: Telegram channel ID
            limit: Maximum number of messages
        
        Returns:
            List of messages
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            cursor = self.collection.find(
                {"channel_id": channel_id}
            ).sort("timestamp", -1).limit(limit)
            
            messages = await cursor.to_list(length=limit)
            return messages
            
        except Exception as e:
            logger.error(f"Error fetching channel messages: {e}")
            raise
    
    async def delete_old_messages(self, days: int = 30) -> int:
        """
        Delete processed messages older than N days.
        
        Args:
            days: Age threshold in days
        
        Returns:
            Number of deleted messages
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            result = await self.collection.delete_many({
                "timestamp": {"$lt": cutoff_date.isoformat()},
                "processed": True
            })
            
            deleted_count = result.deleted_count
            logger.info(f"🗑️  Deleted {deleted_count} old messages (>{days} days)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting old messages: {e}")
            raise
    
    async def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
