"""CloudWatch metrics publishing utility for Telegram scraper monitoring."""

import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import structlog
import boto3
from botocore.exceptions import ClientError

from app.config import settings

logger = structlog.get_logger(__name__)


class CloudWatchMetrics:
    """Publish custom metrics to AWS CloudWatch."""

    def __init__(self):
        """Initialize CloudWatch client."""
        self.enabled = settings.CLOUDWATCH_ENABLED
        self.namespace = settings.CLOUDWATCH_NAMESPACE
        self.environment = settings.ENVIRONMENT
        
        if self.enabled:
            try:
                self.client = boto3.client(
                    'cloudwatch',
                    region_name=settings.AWS_REGION,
                    aws_access_key_id=settings.AWS_ACCESS_KEY_ID if settings.AWS_ACCESS_KEY_ID else None,
                    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY if settings.AWS_SECRET_ACCESS_KEY else None,
                )
                logger.info("cloudwatch_metrics_initialized", namespace=self.namespace)
            except Exception as e:
                logger.error("cloudwatch_metrics_init_failed", error=str(e))
                self.enabled = False
        else:
            logger.info("cloudwatch_metrics_disabled")
            self.client = None

    def _put_metric_data(self, metric_data: List[Dict[str, Any]]) -> bool:
        """
        Internal method to send metrics to CloudWatch.
        
        Args:
            metric_data: List of metric data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            # CloudWatch allows max 20 metrics per request
            for i in range(0, len(metric_data), 20):
                batch = metric_data[i:i + 20]
                self.client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
            return True
        except ClientError as e:
            logger.error("cloudwatch_put_metric_failed", error=str(e), metric_count=len(metric_data))
            return False
        except Exception as e:
            logger.error("cloudwatch_unexpected_error", error=str(e))
            return False

    def publish_scrape_metrics(
        self,
        account_id: int,
        messages_processed: int,
        channels_scraped: int,
        duration_ms: int,
        errors_count: int = 0,
    ) -> bool:
        """
        Publish scraping metrics for a single account.
        
        Args:
            account_id: Telegram account ID (1-5)
            messages_processed: Number of messages fetched
            channels_scraped: Number of channels processed
            duration_ms: Scraping duration in milliseconds
            errors_count: Number of errors encountered
            
        Returns:
            True if published successfully
        """
        timestamp = datetime.utcnow()
        
        metric_data = [
            {
                'MetricName': 'MessagesProcessed',
                'Value': messages_processed,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'AccountId', 'Value': str(account_id)},
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
            {
                'MetricName': 'ChannelsScraped',
                'Value': channels_scraped,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'AccountId', 'Value': str(account_id)},
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
            {
                'MetricName': 'ScrapeDuration',
                'Value': duration_ms,
                'Unit': 'Milliseconds',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'AccountId', 'Value': str(account_id)},
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
        ]
        
        if errors_count > 0:
            metric_data.append({
                'MetricName': 'ErrorCount',
                'Value': errors_count,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'AccountId', 'Value': str(account_id)},
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            })

        success = self._put_metric_data(metric_data)
        
        if success:
            logger.info(
                "cloudwatch_metrics_published",
                account_id=account_id,
                messages=messages_processed,
                channels=channels_scraped,
                duration_ms=duration_ms,
            )
        
        return success

    def publish_account_health(
        self,
        active_accounts: int,
        degraded_accounts: int = 0,
        banned_accounts: int = 0,
    ) -> bool:
        """
        Publish account health metrics.
        
        Args:
            active_accounts: Number of healthy accounts
            degraded_accounts: Number of degraded accounts
            banned_accounts: Number of banned accounts
            
        Returns:
            True if published successfully
        """
        timestamp = datetime.utcnow()
        
        metric_data = [
            {
                'MetricName': 'ActiveAccounts',
                'Value': active_accounts,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
            {
                'MetricName': 'DegradedAccounts',
                'Value': degraded_accounts,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
            {
                'MetricName': 'BannedAccounts',
                'Value': banned_accounts,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
        ]

        success = self._put_metric_data(metric_data)
        
        if success:
            logger.info(
                "account_health_metrics_published",
                active=active_accounts,
                degraded=degraded_accounts,
                banned=banned_accounts,
            )
        
        return success

    def publish_error_metric(
        self,
        error_type: str,
        account_id: Optional[int] = None,
        channel: Optional[str] = None,
    ) -> bool:
        """
        Publish error occurrence metric.
        
        Args:
            error_type: Type of error (FloodWait, ChannelPrivate, AuthKey, etc.)
            account_id: Optional account ID where error occurred
            channel: Optional channel name where error occurred
            
        Returns:
            True if published successfully
        """
        timestamp = datetime.utcnow()
        
        dimensions = [
            {'Name': 'ErrorType', 'Value': error_type},
            {'Name': 'Environment', 'Value': self.environment},
        ]
        
        if account_id:
            dimensions.append({'Name': 'AccountId', 'Value': str(account_id)})
        
        metric_data = [{
            'MetricName': 'ErrorRate',
            'Value': 1,
            'Unit': 'Count',
            'Timestamp': timestamp,
            'Dimensions': dimensions,
        }]

        success = self._put_metric_data(metric_data)
        
        if success:
            logger.info(
                "error_metric_published",
                error_type=error_type,
                account_id=account_id,
                channel=channel,
            )
        
        return success

    def publish_flood_wait_metric(
        self,
        wait_seconds: int,
        account_id: int,
    ) -> bool:
        """
        Publish FloodWait occurrence with wait duration.
        
        Args:
            wait_seconds: How long to wait
            account_id: Account that hit rate limit
            
        Returns:
            True if published successfully
        """
        timestamp = datetime.utcnow()
        
        metric_data = [
            {
                'MetricName': 'FloodWaitCount',
                'Value': 1,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'AccountId', 'Value': str(account_id)},
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
            {
                'MetricName': 'FloodWaitDuration',
                'Value': wait_seconds,
                'Unit': 'Seconds',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'AccountId', 'Value': str(account_id)},
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
        ]

        success = self._put_metric_data(metric_data)
        
        if success:
            logger.info(
                "flood_wait_metric_published",
                wait_seconds=wait_seconds,
                account_id=account_id,
            )
        
        return success

    def publish_storage_sync_metric(
        self,
        messages_synced: int,
        sync_duration_ms: int,
        discrepancies: int = 0,
    ) -> bool:
        """
        Publish MongoDB <-> PostgreSQL sync metrics.
        
        Args:
            messages_synced: Number of messages synchronized
            sync_duration_ms: Time taken to sync in milliseconds
            discrepancies: Number of discrepancies found
            
        Returns:
            True if published successfully
        """
        timestamp = datetime.utcnow()
        
        metric_data = [
            {
                'MetricName': 'MessagesSynced',
                'Value': messages_synced,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
            {
                'MetricName': 'SyncDuration',
                'Value': sync_duration_ms,
                'Unit': 'Milliseconds',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            },
        ]
        
        if discrepancies > 0:
            metric_data.append({
                'MetricName': 'SyncDiscrepancies',
                'Value': discrepancies,
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': self.environment},
                ]
            })

        success = self._put_metric_data(metric_data)
        
        if success:
            logger.info(
                "storage_sync_metrics_published",
                messages_synced=messages_synced,
                sync_duration_ms=sync_duration_ms,
                discrepancies=discrepancies,
            )
        
        return success


# Global instance
cloudwatch_metrics = CloudWatchMetrics()
