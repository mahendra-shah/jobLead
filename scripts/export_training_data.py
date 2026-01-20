"""
Export Training Data from MongoDB
Exports all processed messages with ML classifications for model retraining
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from app.config import Settings
import pandas as pd
from datetime import datetime

def export_training_data():
    """Export all processed messages for training"""
    
    print("=" * 80)
    print("📊 Exporting Training Data from MongoDB")
    print("=" * 80)
    
    # Connect to MongoDB
    settings = Settings()
    client = MongoClient(settings.MONGODB_URI)
    db = client[settings.MONGODB_DB_NAME]
    collection = db['raw_messages']
    
    # Get all processed messages with ML classifications
    print("\n🔍 Fetching messages...")
    messages = list(collection.find({
        'is_processed': True,
        'ml_classification': {'$exists': True}
    }))
    
    print(f"✅ Found {len(messages)} processed messages")
    
    # Convert to training data format
    training_data = []
    for msg in messages:
        ml_class = msg.get('ml_classification', {})
        
        # Skip if no text
        text = msg.get('text', '').strip()
        if not text or len(text) < 20:
            continue
        
        training_data.append({
            'text': text,
            'is_job': ml_class.get('is_job', False),
            'confidence': ml_class.get('confidence', 0.0),
            'reason': ml_class.get('reason', ''),
            'channel': msg.get('channel_username', ''),
            'date': str(msg.get('date', '')),
            'message_id': msg.get('message_id', ''),
            'processed_at': str(msg.get('processed_at', ''))
        })
    
    # Create DataFrame
    df = pd.DataFrame(training_data)
    
    # Remove duplicates
    print(f"\n🧹 Cleaning data...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['text'])
    duplicates_removed = original_count - len(df)
    print(f"   Removed {duplicates_removed} duplicate messages")
    
    # Remove very short messages
    df = df[df['text'].str.len() >= 20]
    
    # Statistics
    jobs = df[df['is_job'] == True]
    non_jobs = df[df['is_job'] == False]
    
    print("\n📊 Dataset Statistics:")
    print(f"   Total messages: {len(df)}")
    print(f"   Jobs: {len(jobs)} ({len(jobs)/len(df)*100:.1f}%)")
    print(f"   Non-jobs: {len(non_jobs)} ({len(non_jobs)/len(df)*100:.1f}%)")
    print(f"   Avg confidence: {df['confidence'].mean():.2f}")
    print(f"   Unique channels: {df['channel'].nunique()}")
    
    # Save to CSV
    output_dir = project_root / 'app' / 'ml' / 'training' / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'messages_{timestamp}.csv'
    
    df.to_csv(output_file, index=False)
    
    # Also save as latest
    latest_file = output_dir / 'messages_latest.csv'
    df.to_csv(latest_file, index=False)
    
    print(f"\n✅ Training data exported:")
    print(f"   File: {output_file}")
    print(f"   Latest: {latest_file}")
    print(f"   Rows: {len(df)}")
    print(f"   Size: {output_file.stat().st_size / 1024:.2f} KB")
    
    print("\n" + "=" * 80)
    print("🎉 Export Complete!")
    print("=" * 80)
    print("\nNext Steps:")
    print("1. Review the data: pandas.read_csv('messages_latest.csv')")
    print("2. Retrain model: python scripts/retrain_classifier.py")
    print("3. Test new model: python scripts/run_ml_pipeline.py")
    
    return output_file

if __name__ == "__main__":
    export_training_data()
