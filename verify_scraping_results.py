"""
Verify Telegram Scraping Results in MongoDB

This script checks MongoDB after scraping to verify:
- Total messages fetched
- Messages per channel
- Recent scraping activity
- Account distribution

Author: Backend Team
Date: 2026-02-13
"""

import os
from datetime import datetime, timedelta
from collections import defaultdict
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def verify_scraping_results():
    """
    Verify and report on recent Telegram scraping results.
    """
    print("\n" + "=" * 80)
    print("TELEGRAM SCRAPING VERIFICATION REPORT")
    print("=" * 80)
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n")
    
    # Connect to MongoDB
    mongo_uri = os.getenv('MONGODB_URI')
    database_name = os.getenv('MONGODB_DATABASE', 'placement_db')
    
    client = MongoClient(mongo_uri)
    db = client[database_name]
    
    # Collections
    channels_collection = db['channels']
    messages_collection = db['raw_messages']
    
    # 1. Total Statistics
    print("📊 OVERALL STATISTICS")
    print("-" * 80)
    
    total_channels = channels_collection.count_documents({'is_active': True})
    total_messages = messages_collection.count_documents({})
    
    print(f"Total Active Channels: {total_channels}")
    print(f"Total Messages in DB: {total_messages:,}")
    
    # 2. Recent Scraping Activity (last 1 hour)
    print("\n🕐 RECENT ACTIVITY (Last 1 Hour)")
    print("-" * 80)
    
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)
    
    recent_messages = messages_collection.count_documents({
        'fetched_at': {'$gte': one_hour_ago}
    })
    
    print(f"Messages Fetched (last 1 hour): {recent_messages:,}")
    
    # Get recently scraped channels
    recent_channels = channels_collection.find(
        {'last_scraped_at': {'$gte': one_hour_ago}},
        {'username': 1, 'last_scraped_at': 1, 'last_scraped_by_account': 1}
    ).sort('last_scraped_at', -1)
    
    recent_channel_count = channels_collection.count_documents(
        {'last_scraped_at': {'$gte': one_hour_ago}}
    )
    
    print(f"Channels Scraped (last 1 hour): {recent_channel_count}")
    
    # 3. Messages per Channel (recent)
    if recent_messages > 0:
        print("\n📈 MESSAGES PER CHANNEL (Recent)")
        print("-" * 80)
        
        pipeline = [
            {'$match': {'fetched_at': {'$gte': one_hour_ago}}},
            {'$group': {
                '_id': '$channel_username',
                'count': {'$sum': 1},
                'latest': {'$max': '$fetched_at'}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 20}
        ]
        
        channel_stats = list(messages_collection.aggregate(pipeline))
        
        for stat in channel_stats:
            print(f"  @{stat['_id']:<30} {stat['count']:>5} messages")
    
    # 4. Account Distribution (recent)
    if recent_messages > 0:
        print("\n👥 ACCOUNT DISTRIBUTION (Recent)")
        print("-" * 80)
        
        pipeline = [
            {'$match': {'fetched_at': {'$gte': one_hour_ago}}},
            {'$group': {
                '_id': '$fetched_by_account',
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id': 1}}
        ]
        
        account_stats = list(messages_collection.aggregate(pipeline))
        
        for stat in account_stats:
            account_id = stat['_id']
            count = stat['count']
            print(f"  Account {account_id}: {count:>6} messages")
    
    # 5. Top 10 Most Active Channels (all time)
    print("\n🔥 TOP 10 MOST ACTIVE CHANNELS (All Time)")
    print("-" * 80)
    
    pipeline = [
        {'$group': {
            '_id': '$channel_username',
            'total_messages': {'$sum': 1},
            'latest_message': {'$max': '$date'}
        }},
        {'$sort': {'total_messages': -1}},
        {'$limit': 10}
    ]
    
    top_channels = list(messages_collection.aggregate(pipeline))
    
    for i, channel in enumerate(top_channels, 1):
        latest = channel['latest_message'].strftime('%Y-%m-%d') if channel.get('latest_message') else 'N/A'
        print(f"  {i:>2}. @{channel['_id']:<30} {channel['total_messages']:>6} messages (latest: {latest})")
    
    # 6. Processing Status
    print("\n⚙️  PROCESSING STATUS")
    print("-" * 80)
    
    processed = messages_collection.count_documents({'is_processed': True})
    unprocessed = messages_collection.count_documents({'is_processed': False})
    
    print(f"Processed Messages: {processed:,}")
    print(f"Unprocessed Messages: {unprocessed:,}")
    
    if total_messages > 0:
        processed_pct = (processed / total_messages) * 100
        print(f"Processing Rate: {processed_pct:.2f}%")
    
    # 7. Recent Scraping Timeline
    print("\n📅 RECENT SCRAPING TIMELINE")
    print("-" * 80)
    
    recent_scrapes = list(recent_channels)
    
    if recent_scrapes:
        print(f"\nRecently Scraped Channels ({len(recent_scrapes)} total):")
        for channel in recent_scrapes[:10]:  # Show last 10
            scraped_time = channel['last_scraped_at'].strftime('%H:%M:%S')
            account = channel.get('last_scraped_by_account', 'N/A')
            print(f"  {scraped_time} - @{channel['username']:<30} (Account {account})")
    else:
        print("  No recent scraping activity found in the last hour.")
    
    # 8. Data Quality Check
    print("\n✅ DATA QUALITY CHECK")
    print("-" * 80)
    
    # Check for messages with text
    messages_with_text = messages_collection.count_documents({'text': {'$exists': True, '$ne': ''}})
    print(f"Messages with Text: {messages_with_text:,} ({(messages_with_text/total_messages*100):.1f}%)")
    
    # Check for duplicates
    pipeline = [
        {'$group': {
            '_id': {'message_id': '$message_id', 'channel_username': '$channel_username'},
            'count': {'$sum': 1}
        }},
        {'$match': {'count': {'$gt': 1}}}
    ]
    duplicates = len(list(messages_collection.aggregate(pipeline)))
    print(f"Duplicate Messages: {duplicates}")
    
    print("\n" + "=" * 80)
    print("END OF REPORT")
    print("=" * 80 + "\n")
    
    client.close()


if __name__ == '__main__':
    verify_scraping_results()
