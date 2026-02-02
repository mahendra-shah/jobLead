#!/usr/bin/env python3
"""
Re-process messages that were classified as jobs but never created in PostgreSQL.
This script will:
1. Find messages with ml_classification.is_job = True
2. Extract job data using the ML processor
3. Create jobs in PostgreSQL
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from datetime import datetime
from app.services.ml_processor_service import get_ml_processor

def reprocess_classified_jobs():
    """Re-process messages that were classified but not stored in PostgreSQL"""
    
    print("\n" + "="*70)
    print("🔄 RE-PROCESSING CLASSIFIED JOBS")
    print("="*70)
    
    # Get ML processor
    processor = get_ml_processor()
    
    # Find messages classified as jobs since Jan 23
    jan_23 = datetime(2026, 1, 23)
    
    query = {
        'date': {'$gte': jan_23},
        'ml_classification.is_job': True,
        'is_processed': True
    }
    
    messages = list(processor.messages_collection.find(query))
    
    print(f"\n📊 Found {len(messages)} messages classified as jobs")
    print(f"   Date range: Jan 23, 2026 onwards")
    print(f"   These were classified but never created in PostgreSQL")
    
    if len(messages) == 0:
        print("\n✅ No messages to reprocess!")
        return
    
    # Show breakdown by date
    from collections import Counter
    dates = [msg['date'].strftime('%Y-%m-%d') for msg in messages]
    date_counts = Counter(dates)
    
    print(f"\n📅 Messages by date:")
    for date in sorted(date_counts.keys(), reverse=True):
        print(f"   {date}: {date_counts[date]} jobs")
    
    # Confirm
    print(f"\n⚠️  This will attempt to create {len(messages)} jobs in PostgreSQL")
    response = input("Continue? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("\n❌ Cancelled")
        return
    
    # Process messages
    print(f"\n🚀 Processing {len(messages)} messages...")
    print("="*70)
    
    # Get DB session
    from app.db.session import get_sync_db
    db = next(get_sync_db())
    
    stats = {
        'total': len(messages),
        'jobs_created': 0,
        'already_exists': 0,
        'errors': 0,
        'low_confidence': 0
    }
    
    try:
        for idx, message in enumerate(messages, 1):
            try:
                if idx % 50 == 0:
                    print(f"Progress: {idx}/{len(messages)} ({idx/len(messages)*100:.1f}%)")
                
                # Process this single message (extract and create job)
                result = processor._process_single_message(message, db, min_confidence=0.6)
                
                if result['stored_to_postgres']:
                    stats['jobs_created'] += result['jobs_created']
                    if result['low_confidence']:
                        stats['low_confidence'] += 1
                else:
                    stats['already_exists'] += 1
                    
            except Exception as e:
                stats['errors'] += 1
                print(f"   ❌ Error processing message {message.get('message_id')}: {e}")
        
        # Commit all changes
        print("\n💾 Committing changes to database...")
        db.commit()
        print("✅ Changes committed successfully!")
        
        # Final stats
        print("\n" + "="*70)
        print("✅ REPROCESSING COMPLETE")
        print("="*70)
        print(f"Total messages: {stats['total']}")
        print(f"Jobs created: {stats['jobs_created']}")
        print(f"  └─ Low confidence: {stats['low_confidence']}")
        print(f"Already existed (duplicates): {stats['already_exists']}")
        print(f"Errors: {stats['errors']}")
        print("="*70)
        
        if stats['jobs_created'] > 0:
            print(f"\n🎉 Successfully created {stats['jobs_created']} jobs in PostgreSQL!")
            print(f"   Check the database or run sheets export to see them")
        
    finally:
        db.close()
        processor.close()


if __name__ == "__main__":
    reprocess_classified_jobs()
