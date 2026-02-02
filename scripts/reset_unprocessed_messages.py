#!/usr/bin/env python3
"""
Reset unprocessed messages in MongoDB.
Finds messages marked as processed but without ML classification,
and resets them to unprocessed so ML processor can handle them.
"""

import sys
from pymongo import MongoClient
from datetime import datetime
import os

# MongoDB connection
MONGO_URI = os.getenv('MONGODB_URI', 'mongodb+srv://assi:Upo55HF6EzKdKQYV@cluster0.apufdpu.mongodb.net/placement_db?retryWrites=true&w=majority')
MONGO_DB = 'placement_db'

def reset_messages():
    """Reset messages that were marked processed but never actually processed by ML"""
    
    print("\n" + "="*70)
    print("🔄 RESETTING UNPROCESSED MESSAGES")
    print("="*70)
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db['raw_messages']
        
        # Find messages that:
        # 1. Are marked as is_processed: True
        # 2. Were created after Jan 22 (last successful job processing)
        # 3. Don't have ml_classification field (never processed by ML)
        
        query = {
            'is_processed': True,
            'date': {'$gte': datetime(2026, 1, 23)},
            '$or': [
                {'ml_classification': {'$exists': False}},
                {'ml_classification.is_job': {'$exists': False}}
            ]
        }
        
        # Count messages to reset
        count = collection.count_documents(query)
        
        print(f"\n📊 Found {count} messages to reset")
        print(f"   Date range: Jan 23, 2026 onwards")
        print(f"   Criteria: Marked processed but no ML classification")
        
        if count == 0:
            print("\n✅ No messages need resetting!")
            return
        
        # Show sample
        sample = collection.find_one(query)
        if sample:
            print(f"\n📝 Sample message:")
            print(f"   Message ID: {sample.get('message_id')}")
            print(f"   Channel: {sample.get('channel_username')}")
            print(f"   Date: {sample.get('date')}")
            print(f"   Is Processed: {sample.get('is_processed')}")
            print(f"   Has ML Classification: {sample.get('ml_classification') is not None}")
        
        # Confirm
        print(f"\n⚠️  This will reset {count} messages to is_processed: false")
        response = input("Continue? (yes/no): ")
        
        if response.lower() not in ['yes', 'y']:
            print("\n❌ Cancelled")
            return
        
        # Reset them
        result = collection.update_many(
            query,
            {
                '$set': {
                    'is_processed': False,
                    'reset_at': datetime.utcnow(),
                    'reset_reason': 'ML processor never ran - resetting for processing'
                }
            }
        )
        
        print(f"\n✅ Successfully reset {result.modified_count} messages")
        print(f"   These messages are now ready for ML processing")
        
        # Show breakdown by date
        print("\n📅 Messages by date:")
        pipeline = [
            {'$match': {'is_processed': False, 'date': {'$gte': datetime(2026, 1, 23)}}},
            {'$group': {
                '_id': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$date'}},
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id': -1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        total_unprocessed = 0
        for r in results:
            print(f"   {r['_id']}: {r['count']} messages")
            total_unprocessed += r['count']
        
        print(f"\n📊 Total unprocessed messages ready: {total_unprocessed}")
        print(f"\n🎯 Next step: Run ML processor to process these messages")
        print(f"   Command: python scripts/run_ml_pipeline.py")
        
        client.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    reset_messages()
