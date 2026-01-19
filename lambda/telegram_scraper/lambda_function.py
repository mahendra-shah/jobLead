"""
Lambda Telegram Scraper - MULTI-ACCOUNT VERSION
Fetches messages from Telegram channels using assigned accounts.

Key Features:
- Reads channel's joined_by_account_id from MongoDB
- Uses correct session file (session_account1-5.session)
- Rate limiting per account
- Connection pooling for performance
"""

import json
import os
import asyncio
import shutil
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameInvalidError
from motor.motor_asyncio import AsyncIOMotorClient

# Telegram credentials
API_ID = int(os.environ['TELEGRAM_API_ID'])
API_HASH = os.environ['TELEGRAM_API_HASH']

# MongoDB connection
MONGODB_URI = os.environ['MONGODB_URI']
MONGODB_DATABASE = os.environ.get('MONGODB_DATABASE', 'placement_db')

# Configuration
MAX_MESSAGES_PER_CHANNEL = 100
RATE_LIMIT_DELAY = 0.5  # 500ms between channels
ACCOUNTS_AVAILABLE = 5  # Total number of Telegram accounts

# Global state (reused across warm invocations)
_mongo_client = None
_telegram_clients = {}  # {account_id: TelegramClient}
_account_stats = defaultdict(lambda: {'channels_scraped': 0, 'messages_found': 0, 'rate_limits': 0})


async def get_mongo_client():
    """Get or create MongoDB client (reused across invocations)."""
    global _mongo_client
    
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(
            MONGODB_URI,
            maxPoolSize=10,  # Higher pool for multiple accounts
            minPoolSize=2,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=5000
        )
        print("✅ Created MongoDB client")
    
    return _mongo_client


async def get_telegram_client(account_id: int) -> TelegramClient:
    """
    Get or create Telegram client for specific account.
    
    Args:
        account_id: Account number (1-5)
    
    Returns:
        TelegramClient instance for that account
    """
    global _telegram_clients
    
    if account_id not in _telegram_clients:
        # Session file paths
        bundled_session = f'/var/task/session_account{account_id}.session'
        tmp_session = f'/tmp/session_account{account_id}'
        
        # Copy session file to /tmp (Lambda write access)
        if os.path.exists(bundled_session):
            if not os.path.exists(tmp_session + '.session'):
                shutil.copy2(bundled_session, tmp_session + '.session')
                print(f"✅ Copied session file for Account {account_id}")
        else:
            raise FileNotFoundError(f"Session file not found: {bundled_session}")
        
        # Create client
        client = TelegramClient(tmp_session, API_ID, API_HASH)
        await client.connect()
        
        # Verify authorization
        if not await client.is_user_authorized():
            raise RuntimeError(f"Account {account_id} not authorized")
        
        # Get account info for logging
        me = await client.get_me()
        print(f"✅ Account {account_id} connected: {me.phone}")
        
        _telegram_clients[account_id] = client
    
    return _telegram_clients[account_id]


async def scrape_channel(channel: Dict, mongo_db) -> Dict:
    """
    Scrape a single channel using its assigned account.
    
    Args:
        channel: Channel document from MongoDB
        mongo_db: MongoDB database instance
    
    Returns:
        Dict with scraping statistics
    """
    username = channel.get('username', '').lstrip('@')
    account_id = channel.get('joined_by_account_id', 1)  # Default to Account 1
    
    stats = {
        'channel': username,
        'account_id': account_id,
        'messages_fetched': 0,
        'success': False,
        'error': None
    }
    
    try:
        # Get client for this channel's account
        client = await get_telegram_client(account_id)
        
        print(f"📱 Account {account_id} → scraping @{username}")
        
        # Check if this is first time fetching (incremental fetching logic)
        last_message_id = channel.get('last_message_id')
        
        if last_message_id:
            # Incremental fetch: Get messages after last_message_id
            print(f"   📥 Incremental fetch: messages newer than ID {last_message_id}")
            messages = await client.get_messages(username, limit=MAX_MESSAGES_PER_CHANNEL, min_id=last_message_id)
        else:
            # First time fetch: Get last 10 messages only
            print(f"   📥 First time fetch: last 10 messages")
            messages = await client.get_messages(username, limit=10)
            # CRITICAL: Explicitly slice to ensure we only get 10 messages
            # Telethon sometimes returns more than the limit
            messages = messages[:10] if len(messages) > 10 else messages
            print(f"   📊 Fetched {len(messages)} messages (first-time limit: 10)")
        
        if not messages:
            print(f"   No messages found in @{username}")
            stats['success'] = True
            return stats
        
        # Process and store messages
        raw_messages_collection = mongo_db['raw_messages']
        channels_collection = mongo_db['channels']
        
        stored_count = 0
        for msg in messages:
            if msg.text:
                # Create document
                doc = {
                    'message_id': msg.id,
                    'channel_username': username,
                    'channel_id': channel.get('_id'),
                    'text': msg.text,
                    'date': msg.date,
                    'sender_id': msg.sender_id if hasattr(msg, 'sender_id') else None,
                    'views': msg.views if hasattr(msg, 'views') else None,
                    'forwards': msg.forwards if hasattr(msg, 'forwards') else None,
                    'fetched_at': datetime.utcnow(),
                    'fetched_by_account': account_id,  # Track which account fetched it
                    'is_processed': False
                }
                
                # Upsert (avoid duplicates)
                await raw_messages_collection.update_one(
                    {'message_id': msg.id, 'channel_username': username},
                    {'$set': doc},
                    upsert=True
                )
                stored_count += 1
        
        # Update channel metadata
        last_message = messages[0] if messages else None
        await channels_collection.update_one(
            {'username': username},
            {
                '$set': {
                    'last_scraped_at': datetime.utcnow(),
                    'last_scraped_by_account': account_id,
                    'last_message_id': last_message.id if last_message else None,
                    'last_message_date': last_message.date if last_message else None,
                    'total_messages_scraped': channel.get('total_messages_scraped', 0) + stored_count
                }
            }
        )
        
        stats['messages_fetched'] = stored_count
        stats['success'] = True
        
        # Update account stats
        _account_stats[account_id]['channels_scraped'] += 1
        _account_stats[account_id]['messages_found'] += stored_count
        
        print(f"   ✅ {stored_count} messages from @{username}")
        
        # Rate limiting
        await asyncio.sleep(RATE_LIMIT_DELAY)
        
    except FloodWaitError as e:
        error_msg = f"Rate limited on Account {account_id}: wait {e.seconds}s"
        print(f"   ⚠️ {error_msg}")
        stats['error'] = error_msg
        _account_stats[account_id]['rate_limits'] += 1
        
        # Wait if reasonable time
        if e.seconds < 60:
            await asyncio.sleep(e.seconds)
        
    except (ChannelPrivateError, UsernameInvalidError) as e:
        error_msg = f"Channel access error: {str(e)}"
        print(f"   ❌ {error_msg}")
        stats['error'] = error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"   ❌ {error_msg}")
        stats['error'] = error_msg
    
    return stats


async def scrape_channels_batch(channel_batch: List[Dict]):
    """
    Scrape a batch of channels using their assigned accounts.
    
    Args:
        channel_batch: List of channel documents
    
    Returns:
        Dict with overall statistics
    """
    # Connect to MongoDB
    mongo_client = await get_mongo_client()
    mongo_db = mongo_client[MONGODB_DATABASE]
    
    # Initialize account stats for this batch
    global _account_stats
    _account_stats.clear()
    
    print(f"\n🚀 Starting batch scrape: {len(channel_batch)} channels")
    
    # Group channels by account for better logging
    channels_by_account = defaultdict(list)
    for channel in channel_batch:
        account_id = channel.get('joined_by_account_id', 1)
        channels_by_account[account_id].append(channel['username'])
    
    print("\n📊 Channel distribution:")
    for account_id in sorted(channels_by_account.keys()):
        channels = channels_by_account[account_id]
        print(f"   Account {account_id}: {len(channels)} channels")
    
    # Scrape all channels
    results = []
    for channel in channel_batch:
        result = await scrape_channel(channel, mongo_db)
        results.append(result)
    
    # Calculate summary statistics
    total_messages = sum(r['messages_fetched'] for r in results)
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    summary = {
        'total_channels': len(channel_batch),
        'successful': successful,
        'failed': failed,
        'total_messages': total_messages,
        'account_stats': dict(_account_stats),
        'results': results
    }
    
    print(f"\n✅ Batch complete:")
    print(f"   Channels: {successful}/{len(channel_batch)} successful")
    print(f"   Messages: {total_messages} total")
    print(f"\n📊 Per-account stats:")
    for account_id in sorted(_account_stats.keys()):
        stats = _account_stats[account_id]
        print(f"   Account {account_id}:")
        print(f"      Channels scraped: {stats['channels_scraped']}")
        print(f"      Messages found: {stats['messages_found']}")
        print(f"      Rate limits hit: {stats['rate_limits']}")
    
    return summary


def lambda_handler(event, context):
    """
    AWS Lambda handler - processes channel batch.
    
    Event format:
    {
        "channels": [
            {
                "username": "bangalore_jobs",
                "joined_by_account_id": 1,
                ...
            },
            ...
        ]
    }
    """
    print("=" * 60)
    print("TELEGRAM SCRAPER - MULTI-ACCOUNT")
    print("=" * 60)
    
    try:
        # Get channels from event
        channels = event.get('channels', [])
        
        if not channels:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No channels provided'})
            }
        
        print(f"\n📥 Received {len(channels)} channels to scrape")
        
        # Run async scraping
        loop = asyncio.get_event_loop()
        summary = loop.run_until_complete(scrape_channels_batch(channels))
        
        # Close clients if Lambda is about to terminate
        # (They'll be reused if Lambda stays warm)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'summary': summary
            }, default=str)  # Handle datetime serialization
        }
        
    except Exception as e:
        print(f"❌ Lambda error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }
    
    finally:
        print("=" * 60)


# Cleanup function (called when Lambda container terminates)
async def cleanup_clients():
    """Disconnect all Telegram clients gracefully."""
    global _telegram_clients
    
    for account_id, client in _telegram_clients.items():
        try:
            await client.disconnect()
            print(f"✅ Disconnected Account {account_id}")
        except Exception as e:
            print(f"⚠️ Error disconnecting Account {account_id}: {e}")
    
    _telegram_clients.clear()


# For local testing
if __name__ == "__main__":
    # Test event with sample channels
    test_event = {
        "channels": [
            {"username": "bangalore_jobs", "joined_by_account_id": 1},
            {"username": "tech_jobs_india", "joined_by_account_id": 2},
            {"username": "remote_ai_jobs", "joined_by_account_id": 1}
        ]
    }
    
    result = lambda_handler(test_event, None)
    print("\n" + json.dumps(result, indent=2, default=str))
