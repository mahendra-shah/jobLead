"""
Storage service factory
Returns MongoDB, DynamoDB, or Local JSON storage based on configuration
This allows seamless switching between local development and production
"""

import os
from typing import Union

from app.config import settings

# Try to import storage services
try:
    from .dynamodb_service import DynamoDBService
    DYNAMODB_AVAILABLE = True
except ImportError:
    DYNAMODB_AVAILABLE = False
    DynamoDBService = None

try:
    from .mongodb_storage_service import MongoDBStorageService
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    MongoDBStorageService = None

from .local_storage_service import LocalStorageService


def get_storage_service() -> Union[LocalStorageService, 'MongoDBStorageService', 'DynamoDBService']:
    """
    Get appropriate storage service based on configuration
    
    Returns:
        MongoDBStorageService for MongoDB Atlas (recommended for production)
        DynamoDBService for AWS DynamoDB
        LocalStorageService for local development
    """
    storage_type = settings.STORAGE_TYPE.lower()
    
    # MongoDB (recommended for production)
    if storage_type == 'mongodb':
        if MONGODB_AVAILABLE:
            # Verify MongoDB credentials are configured
            if all([
                settings.MONGODB_USERNAME,
                settings.MONGODB_PASSWORD,
                settings.MONGODB_CLUSTER
            ]):
                print(f"🍃 Using MongoDB Atlas storage ({settings.MONGODB_DATABASE}.{settings.MONGODB_COLLECTION})")
                return MongoDBStorageService()
            else:
                print("⚠️  MongoDB requested but credentials not configured, falling back to local storage")
                return LocalStorageService()
        else:
            print("⚠️  MongoDB not available (install motor), falling back to local storage")
            return LocalStorageService()
    
    # DynamoDB (AWS)
    elif storage_type == 'dynamodb':
        if DYNAMODB_AVAILABLE:
            # Verify AWS credentials are configured
            if all([
                settings.AWS_REGION,
                settings.DYNAMODB_TABLE_NAME,
                settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY
            ]):
                print("📊 Using DynamoDB storage")
                return DynamoDBService()
            else:
                print("⚠️  DynamoDB requested but AWS credentials not configured, falling back to local storage")
                return LocalStorageService()
        else:
            print("⚠️  DynamoDB not available (install boto3), falling back to local storage")
            return LocalStorageService()
    
    # Local JSON (default for development)
    else:
        print("💾 Using local JSON storage (data/raw_messages.json)")
        return LocalStorageService()


# Default storage instance
storage = get_storage_service()
