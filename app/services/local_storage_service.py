"""
Local JSON file-based storage service for raw Telegram messages
This is a development/local alternative to DynamoDB
Later we can swap this with DynamoDB service without changing the interface
"""

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from uuid import uuid4
import logging

logger = logging.getLogger(__name__)


class LocalStorageService:
    """
    Local JSON file storage that mimics DynamoDB interface
    Data stored in: data/raw_messages.json
    """
    
    def __init__(self, file_path: str = None):
        """Initialize local storage"""
        if file_path is None:
            # Default to data/raw_messages.json
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            file_path = os.path.join(data_dir, 'raw_messages.json')
        
        self.file_path = file_path
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create file if it doesn't exist"""
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                json.dump([], f)
            logger.info(f"Created storage file: {self.file_path}")
    
    def _read_data(self) -> List[Dict]:
        """Read all messages from JSON file"""
        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            logger.error(f"Corrupt JSON file: {self.file_path}, resetting...")
            return []
        except Exception as e:
            logger.error(f"Error reading storage: {e}")
            return []
    
    def _write_data(self, data: List[Dict]):
        """Write all messages to JSON file"""
        try:
            with open(self.file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error writing storage: {e}")
            raise
    
    async def put_item(self, item: Dict[str, Any]) -> bool:
        """
        Store a message
        
        Args:
            item: Message data with fields:
                - id: UUID string
                - message_id: int
                - group_username: str
                - text: str
                - message_date: ISO datetime string
                - processed: bool
                - processing_status: str
                - processing_attempts: int
                - ttl: unix timestamp
        
        Returns:
            bool: Success status
        """
        try:
            # Add created_at if not present
            if 'created_at' not in item:
                item['created_at'] = datetime.utcnow().isoformat()
            
            # Add TTL if not present (7 days)
            if 'ttl' not in item:
                ttl_date = datetime.utcnow() + timedelta(days=7)
                item['ttl'] = int(ttl_date.timestamp())
            
            # Read current data
            data = self._read_data()
            
            # Check if message already exists (update instead of duplicate)
            existing_idx = next((i for i, msg in enumerate(data) if msg.get('id') == item.get('id')), None)
            
            if existing_idx is not None:
                data[existing_idx] = item
                logger.info(f"Updated existing message: {item.get('id')}")
            else:
                data.append(item)
                logger.info(f"Stored new message: {item.get('id')}")
            
            # Write back
            self._write_data(data)
            return True
            
        except Exception as e:
            logger.error(f"Error storing message: {e}")
            return False
    
    async def get_item(self, message_id: str) -> Optional[Dict]:
        """
        Get a specific message by ID
        
        Args:
            message_id: Message UUID
        
        Returns:
            Message dict or None
        """
        try:
            data = self._read_data()
            message = next((msg for msg in data if msg.get('id') == message_id), None)
            return message
        except Exception as e:
            logger.error(f"Error getting message {message_id}: {e}")
            return None
    
    async def query_unprocessed(self, limit: int = 50) -> List[Dict]:
        """
        Get unprocessed messages (processed=False)
        Sorted by created_at (oldest first)
        
        Args:
            limit: Maximum number of messages to return
        
        Returns:
            List of message dicts
        """
        try:
            data = self._read_data()
            
            # Filter unprocessed (status='pending' or processed=False)
            unprocessed = [
                msg for msg in data 
                if not msg.get('processed', False) 
                and msg.get('processing_status', 'pending') == 'pending'
            ]
            
            # Sort by created_at (oldest first)
            unprocessed.sort(key=lambda x: x.get('created_at', ''))
            
            # Apply limit
            return unprocessed[:limit]
            
        except Exception as e:
            logger.error(f"Error querying unprocessed: {e}")
            return []
    
    async def get_pending_messages(self, limit: int = 100) -> List[Dict]:
        """
        Get messages that are ready for processing (alias for query_unprocessed)
        
        Args:
            limit: Maximum number of messages
        
        Returns:
            List of pending message dicts
        """
        return await self.query_unprocessed(limit=limit)
    
    async def mark_rejected(self, message_id: str, reason: str) -> bool:
        """
        Mark a message as rejected (not a job)
        
        Args:
            message_id: Message UUID
            reason: Rejection reason
        
        Returns:
            bool: Success status
        """
        return await self.mark_processed(
            message_id=message_id,
            status='not_a_job',
            rejection_reason=reason
        )
    
    async def query_by_status(
        self,
        status: str,
        limit: int = 50,
        since: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Query messages by processing status
        
        Args:
            status: Processing status (pending/processing/processed/error/skipped/not_a_job)
            limit: Maximum results
            since: Only return messages after this datetime
        
        Returns:
            List of message dicts
        """
        try:
            data = self._read_data()
            
            # Filter by status
            filtered = [msg for msg in data if msg.get('processing_status') == status]
            
            # Filter by date if provided
            if since:
                since_str = since.isoformat()
                filtered = [msg for msg in filtered if msg.get('created_at', '') >= since_str]
            
            # Sort by created_at
            filtered.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            # Apply limit
            return filtered[:limit]
            
        except Exception as e:
            logger.error(f"Error querying by status {status}: {e}")
            return []
    
    async def update_status(self, message_id: str, **kwargs) -> bool:
        """
        Update message fields
        
        Args:
            message_id: Message UUID
            **kwargs: Fields to update
        
        Returns:
            bool: Success status
        """
        try:
            data = self._read_data()
            
            # Find message
            message_idx = next((i for i, msg in enumerate(data) if msg.get('id') == message_id), None)
            
            if message_idx is None:
                logger.warning(f"Message not found: {message_id}")
                return False
            
            # Update fields
            for key, value in kwargs.items():
                # Convert datetime to ISO string
                if isinstance(value, datetime):
                    value = value.isoformat()
                data[message_idx][key] = value
            
            # Add updated_at timestamp
            data[message_idx]['updated_at'] = datetime.utcnow().isoformat()
            
            # Write back
            self._write_data(data)
            logger.info(f"Updated message {message_id}: {list(kwargs.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating message {message_id}: {e}")
            return False
    
    async def mark_processed(
        self,
        message_id: str,
        status: str,
        rejection_reason: Optional[str] = None,
        job_id: Optional[str] = None
    ) -> bool:
        """
        Mark message as processed
        
        Args:
            message_id: Message UUID
            status: Final status (processed/not_a_job/skipped/error)
            rejection_reason: Why it was rejected (optional)
            job_id: Created job ID if applicable (optional)
        
        Returns:
            bool: Success status
        """
        updates = {
            'processed': True,
            'processing_status': status,
            'processed_at': datetime.utcnow().isoformat()
        }
        
        if rejection_reason:
            updates['rejection_reason'] = rejection_reason
        
        if job_id:
            updates['job_id'] = job_id
        
        return await self.update_status(message_id, **updates)
    
    async def increment_attempts(self, message_id: str) -> bool:
        """
        Increment processing attempts counter
        
        Args:
            message_id: Message UUID
        
        Returns:
            bool: Success status
        """
        try:
            data = self._read_data()
            
            # Find message
            message_idx = next((i for i, msg in enumerate(data) if msg.get('id') == message_id), None)
            
            if message_idx is None:
                return False
            
            # Increment attempts
            current_attempts = data[message_idx].get('processing_attempts', 0)
            data[message_idx]['processing_attempts'] = current_attempts + 1
            data[message_idx]['last_attempt_at'] = datetime.utcnow().isoformat()
            
            # Write back
            self._write_data(data)
            return True
            
        except Exception as e:
            logger.error(f"Error incrementing attempts for {message_id}: {e}")
            return False
    
    async def get_processing_stats(self, days: int = 7) -> Dict:
        """
        Get processing statistics
        
        Args:
            days: Number of days to look back
        
        Returns:
            Dict with statistics
        """
        try:
            data = self._read_data()
            
            # Filter by date range
            cutoff = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff.isoformat()
            recent = [msg for msg in data if msg.get('created_at', '') >= cutoff_str]
            
            # Calculate stats
            stats = {
                'total_messages': len(recent),
                'processed_count': sum(1 for msg in recent if msg.get('processed')),
                'pending_count': sum(1 for msg in recent if msg.get('processing_status') == 'pending'),
                'not_a_job_count': sum(1 for msg in recent if msg.get('processing_status') == 'not_a_job'),
                'skipped_count': sum(1 for msg in recent if msg.get('processing_status') == 'skipped'),
                'error_count': sum(1 for msg in recent if msg.get('processing_status') == 'error'),
                'jobs_created': sum(1 for msg in recent if msg.get('job_id')),
                'average_attempts': sum(msg.get('processing_attempts', 0) for msg in recent) / len(recent) if recent else 0
            }
            
            # Breakdown of skip reasons
            skip_reasons = {}
            for msg in recent:
                if msg.get('processing_status') == 'skipped' and msg.get('rejection_reason'):
                    reason = msg['rejection_reason']
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            
            stats['skip_reasons'] = skip_reasons
            stats['time_range_days'] = days
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    async def cleanup_old_messages(self, days: int = 7) -> int:
        """
        Delete messages older than specified days
        
        Args:
            days: Age threshold in days
        
        Returns:
            Number of messages deleted
        """
        try:
            data = self._read_data()
            
            # Calculate cutoff
            cutoff = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff.isoformat()
            
            # Filter out old messages
            before_count = len(data)
            data = [msg for msg in data if msg.get('created_at', '') >= cutoff_str]
            after_count = len(data)
            
            deleted = before_count - after_count
            
            if deleted > 0:
                self._write_data(data)
                logger.info(f"Cleaned up {deleted} old messages")
            
            return deleted
            
        except Exception as e:
            logger.error(f"Error cleaning up: {e}")
            return 0
    
    def get_file_size(self) -> str:
        """Get human-readable file size"""
        try:
            size_bytes = os.path.getsize(self.file_path)
            for unit in ['B', 'KB', 'MB', 'GB']:
                if size_bytes < 1024:
                    return f"{size_bytes:.2f} {unit}"
                size_bytes /= 1024
            return f"{size_bytes:.2f} TB"
        except Exception:
            return "Unknown"
    
    def get_message_count(self) -> int:
        """Get total number of messages"""
        try:
            data = self._read_data()
            return len(data)
        except Exception:
            return 0


# Create singleton instance
storage_service = LocalStorageService()
