"""
Lambda 1: Channel Batcher (Orchestrator)
Splits channels into batches and invokes scraper Lambda for each batch.

Triggered by: EventBridge (daily at 3:00 AM)
"""

import json
import boto3
import os
from typing import List, Dict
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

# AWS clients
lambda_client = boto3.client('lambda')

# MongoDB connection (from environment variables)
MONGODB_URI = os.environ['MONGODB_URI']
MONGODB_DATABASE = os.environ.get('MONGODB_DATABASE', 'placement_db')

# Configuration
BATCH_SIZE = 50  # Channels per batch
SCRAPER_LAMBDA_NAME = os.environ['SCRAPER_LAMBDA_NAME']


async def get_active_channels() -> List[Dict]:
    """Get active channels from MongoDB."""
    client = AsyncIOMotorClient(
        MONGODB_URI,
        maxPoolSize=2,  # Minimal for M0 free tier
        minPoolSize=1,
        maxIdleTimeMS=20000
    )
    db = client[MONGODB_DATABASE]
    
    try:
        # Get active channels with error_count < 5
        channels = await db.channels.find({
            "is_active": True,
            "error_count": {"$lt": 5}
        }).to_list(length=None)
        
        # Convert ObjectIds and datetime objects to strings for JSON serialization
        for channel in channels:
            if '_id' in channel:
                channel['_id'] = str(channel['_id'])
            # Convert datetime fields to ISO format strings
            for key, value in list(channel.items()):
                if hasattr(value, 'isoformat'):  # Check if it's a datetime object
                    channel[key] = value.isoformat()
        
        print(f"✅ Found {len(channels)} active channels")
        return channels
        
    finally:
        client.close()


def create_batches(channels: List[Dict], batch_size: int) -> List[List[Dict]]:
    """Split channels into batches."""
    batches = []
    for i in range(0, len(channels), batch_size):
        batches.append(channels[i:i + batch_size])
    return batches


def invoke_scraper_lambda(batch: List[Dict], batch_number: int) -> Dict:
    """Invoke scraper Lambda with a batch of channels."""
    payload = {
        "channels": batch,
        "batch_number": batch_number
    }
    
    response = lambda_client.invoke(
        FunctionName=SCRAPER_LAMBDA_NAME,
        InvocationType='Event',  # Async invocation
        Payload=json.dumps(payload)
    )
    
    return {
        "batch_number": batch_number,
        "channel_count": len(batch),
        "status_code": response['StatusCode']
    }


def lambda_handler(event, context):
    """
    Main handler for Channel Batcher Lambda.
    
    Triggered by: EventBridge (daily at 3:00 AM)
    """
    print("🚀 Starting Channel Batcher Lambda")
    print(f"Event: {json.dumps(event)}")
    
    try:
        # Get active channels from MongoDB
        channels = asyncio.run(get_active_channels())
        
        if not channels:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No active channels found",
                    "channels_processed": 0
                })
            }
        
        # Create batches
        batches = create_batches(channels, BATCH_SIZE)
        print(f"📦 Created {len(batches)} batches of {BATCH_SIZE} channels each")
        
        # Invoke scraper Lambda for each batch
        results = []
        for i, batch in enumerate(batches, 1):
            result = invoke_scraper_lambda(batch, i)
            results.append(result)
            print(f"✅ Invoked batch {i}/{len(batches)}")
        
        response_body = {
            "message": "Successfully invoked scraper Lambdas",
            "total_channels": len(channels),
            "total_batches": len(batches),
            "batch_results": results
        }
        
        print(f"✅ Batcher complete: {json.dumps(response_body)}")
        
        return {
            "statusCode": 200,
            "body": json.dumps(response_body)
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error in Channel Batcher: {error_msg}")
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": error_msg
            })
        }
