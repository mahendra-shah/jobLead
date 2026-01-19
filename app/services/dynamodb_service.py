"""
DynamoDB Service for Raw Telegram Messages
Handles all DynamoDB operations for message storage and processing
"""
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBService:
    """Service for interacting with DynamoDB for raw telegram messages."""
    
    def __init__(self):
        """Initialize DynamoDB client and table."""
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv('AWS_REGION', 'ap-south-1'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.table_name = os.getenv('DYNAMODB_TABLE_NAME', 'raw_telegram_messages')
        self.table = self.dynamodb.Table(self.table_name)
    
    async def put_item(self, item: dict) -> bool:
        """
        Store a message in DynamoDB.
        
        Args:
            item: Message data to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert datetime objects to ISO strings
            if 'created_at' in item and isinstance(item['created_at'], datetime):
                item['created_at'] = item['created_at'].isoformat()
            if 'message_date' in item and isinstance(item['message_date'], datetime):
                item['message_date'] = item['message_date'].isoformat()
            
            # DynamoDB doesn't support float, convert to Decimal
            item = self._convert_floats_to_decimal(item)
            
            self.table.put_item(Item=item)
            logger.info(f"Stored message {item['id']} in DynamoDB")
            return True
            
        except ClientError as e:
            logger.error(f"Error storing message in DynamoDB: {e}")
            return False
    
    async def query_unprocessed(self, limit: int = 50) -> List[dict]:
        """
        Query for unprocessed messages.
        
        Args:
            limit: Maximum number of messages to return
            
        Returns:
            List of unprocessed messages
        """
        try:
            response = self.table.query(
                IndexName='processed-created_at-index',
                KeyConditionExpression=Key('processed').eq(False),
                Limit=limit,
                ScanIndexForward=True  # Oldest first
            )
            
            items = response.get('Items', [])
            logger.info(f"Found {len(items)} unprocessed messages")
            return items
            
        except ClientError as e:
            logger.error(f"Error querying unprocessed messages: {e}")
            return []
    
    async def query_by_status(
        self, 
        status: str, 
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[dict]:
        """
        Query messages by processing status.
        
        Args:
            status: Processing status to filter by
            limit: Maximum number of messages
            since: Only return messages created after this time
            
        Returns:
            List of messages with given status
        """
        try:
            key_condition = Key('processing_status').eq(status)
            
            if since:
                key_condition = key_condition & Key('created_at').gte(since.isoformat())
            
            response = self.table.query(
                IndexName='processing_status-created_at-index',
                KeyConditionExpression=key_condition,
                Limit=limit,
                ScanIndexForward=False  # Newest first
            )
            
            return response.get('Items', [])
            
        except ClientError as e:
            logger.error(f"Error querying by status {status}: {e}")
            return []
    
    async def get_item(self, message_id: str) -> Optional[dict]:
        """
        Get a single message by ID.
        
        Args:
            message_id: Message ID
            
        Returns:
            Message data or None
        """
        try:
            response = self.table.get_item(Key={'id': message_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(f"Error getting message {message_id}: {e}")
            return None
    
    async def update_status(self, message_id: str, **kwargs):
        """
        Update message processing status and fields.
        
        Args:
            message_id: Message ID to update
            **kwargs: Fields to update
        """
        try:
            # Build update expression
            update_expr = "SET "
            expr_attr_names = {}
            expr_attr_values = {}
            
            for i, (key, value) in enumerate(kwargs.items()):
                # Handle reserved keywords
                attr_name = f"#attr{i}"
                attr_value = f":val{i}"
                
                update_expr += f"{attr_name} = {attr_value}, "
                expr_attr_names[attr_name] = key
                
                # Convert datetime to ISO string
                if isinstance(value, datetime):
                    value = value.isoformat()
                
                # Convert float to Decimal
                if isinstance(value, float):
                    value = Decimal(str(value))
                
                expr_attr_values[attr_value] = value
            
            # Add updated_at timestamp
            update_expr += "#updated_at = :updated_at"
            expr_attr_names['#updated_at'] = 'updated_at'
            expr_attr_values[':updated_at'] = datetime.utcnow().isoformat()
            
            self.table.update_item(
                Key={'id': message_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values
            )
            
            logger.info(f"Updated message {message_id}")
            
        except ClientError as e:
            logger.error(f"Error updating message {message_id}: {e}")
    
    async def mark_processed(
        self,
        message_id: str,
        status: str,
        rejection_reason: Optional[str] = None,
        job_id: Optional[str] = None,
        **kwargs
    ):
        """
        Mark a message as processed.
        
        Args:
            message_id: Message ID
            status: Processing status (processed, skipped, error, etc.)
            rejection_reason: Reason for rejection if skipped
            job_id: UUID of created job if successful
            **kwargs: Additional fields to update
        """
        updates = {
            'processed': True,
            'processing_status': status,
            'processing_completed_at': datetime.utcnow().isoformat(),
            **kwargs
        }
        
        if rejection_reason:
            updates['rejection_reason'] = rejection_reason
        
        if job_id:
            updates['job_id'] = job_id
        
        await self.update_status(message_id, **updates)
    
    async def increment_attempts(self, message_id: str):
        """Increment processing attempts counter."""
        try:
            self.table.update_item(
                Key={'id': message_id},
                UpdateExpression="SET processing_attempts = processing_attempts + :inc",
                ExpressionAttributeValues={':inc': 1}
            )
        except ClientError as e:
            logger.error(f"Error incrementing attempts for {message_id}: {e}")
    
    async def get_processing_stats(self, days: int = 7) -> dict:
        """
        Get processing statistics for the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with statistics
        """
        try:
            since = datetime.utcnow() - timedelta(days=days)
            
            # Scan table for stats (not ideal for production, but works for small datasets)
            response = self.table.scan(
                FilterExpression=Attr('created_at').gte(since.isoformat())
            )
            
            items = response.get('Items', [])
            
            # Calculate statistics
            stats = {
                'total': len(items),
                'processed': len([i for i in items if i.get('processing_status') == 'processed']),
                'pending': len([i for i in items if i.get('processing_status') == 'pending']),
                'processing': len([i for i in items if i.get('processing_status') == 'processing']),
                'not_a_job': len([i for i in items if i.get('processing_status') == 'not_a_job']),
                'skipped': len([i for i in items if i.get('processing_status') == 'skipped']),
                'error': len([i for i in items if i.get('processing_status') == 'error']),
                'jobs_created': len([i for i in items if i.get('job_id')]),
            }
            
            # Calculate reasons for skipping
            skipped_items = [i for i in items if i.get('processing_status') == 'skipped']
            stats['skip_reasons'] = {}
            for item in skipped_items:
                reason = item.get('rejection_reason', 'Unknown')
                stats['skip_reasons'][reason] = stats['skip_reasons'].get(reason, 0) + 1
            
            return stats
            
        except ClientError as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    async def cleanup_old_messages(self, days: int = 7):
        """
        Delete messages older than N days (backup to TTL).
        
        Args:
            days: Delete messages older than this
        """
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            
            response = self.table.scan(
                FilterExpression=Attr('created_at').lt(cutoff.isoformat())
            )
            
            items = response.get('Items', [])
            
            # Batch delete
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'id': item['id']})
            
            logger.info(f"Deleted {len(items)} old messages")
            
        except ClientError as e:
            logger.error(f"Error cleaning up old messages: {e}")
    
    @staticmethod
    def _convert_floats_to_decimal(obj):
        """Convert float values to Decimal for DynamoDB."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: DynamoDBService._convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [DynamoDBService._convert_floats_to_decimal(item) for item in obj]
        return obj
