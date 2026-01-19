#!/usr/bin/env python3
"""
Run ML Processing Pipeline
Process unprocessed Telegram messages: classify → extract → store to PostgreSQL
"""

import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.services.ml_processor_service import get_ml_processor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ml_processing.log')
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Run ML processing pipeline"""
    print("\n" + "=" * 80)
    print("🤖 ML Processing Pipeline")
    print("=" * 80)
    print("Process: MongoDB (unprocessed) → ML Classify → Extract → PostgreSQL")
    print("=" * 80 + "\n")
    
    try:
        # Get ML processor
        processor = get_ml_processor()
        
        # Process all unprocessed messages
        stats = processor.process_unprocessed_messages(
            limit=None,  # Process all
            min_confidence=0.6  # 60% confidence threshold
        )
        
        # Print results
        print("\n" + "=" * 80)
        print("📊 Processing Results")
        print("=" * 80)
        print(f"Total messages processed:  {stats['total_messages']}")
        print(f"Jobs found:                {stats['jobs_found']}")
        print(f"  ├─ Stored to PostgreSQL: {stats['stored_to_postgres']}")
        print(f"  └─ Low confidence:       {stats['low_confidence']}")
        print(f"Non-jobs:                  {stats['non_jobs']}")
        print(f"Errors:                    {stats['errors']}")
        print(f"Processing time:           {stats['processing_time_ms']:.2f}ms")
        print("=" * 80)
        
        # Get overall stats
        overall = processor.get_processing_stats()
        print("\n📈 Overall Statistics")
        print("=" * 80)
        print(f"Total messages in DB:      {overall['total_messages']}")
        print(f"Processed:                 {overall['processed']} ({overall['processing_rate']:.1f}%)")
        print(f"Unprocessed:               {overall['unprocessed']}")
        print(f"Jobs classified:           {overall['jobs_classified']}")
        print("=" * 80 + "\n")
        
        # Close connection
        processor.close()
        
        # Exit code
        if stats['errors'] > 0:
            logger.warning(f"⚠️  Completed with {stats['errors']} errors")
            sys.exit(1)
        else:
            logger.info("✅ Processing completed successfully!")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Processing interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
