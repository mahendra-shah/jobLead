#!/usr/bin/env python3
"""
Retrain ML Model with Updated Training Data.
Should be run weekly via cron: 0 2 * * 0 (Sunday 2 AM IST).
"""

import json
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.ml.sklearn_classifier import SklearnClassifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_training_data(training_data_file: Path) -> dict:
    """Load training data from JSON file."""
    
    if not training_data_file.exists():
        logger.error(f"❌ Training data file not found: {training_data_file}")
        return None
    
    try:
        with open(training_data_file, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
        
        logger.info(f"📊 Loaded training data:")
        logger.info(f"   Total examples: {len(training_data.get('examples', []))}")
        logger.info(f"   Version: {training_data.get('version', 'unknown')}")
        logger.info(f"   Last updated: {training_data.get('updated', 'unknown')}")
        
        if 'statistics' in training_data:
            stats = training_data['statistics']
            logger.info(f"   Jobs: {stats.get('jobs', 0)}")
            logger.info(f"   Non-jobs: {stats.get('non_jobs', 0)}")
            logger.info(f"   Ratio: {stats.get('ratio', 'unknown')}")
        
        return training_data
    
    except Exception as e:
        logger.error(f"❌ Error loading training data: {e}")
        return None


def retrain_model(training_data: dict) -> bool:
    """Retrain the ML model with updated training data."""
    
    examples = training_data.get('examples', [])
    
    if not examples:
        logger.error("❌ No training examples found!")
        return False
    
    if len(examples) < 20:
        logger.warning(f"⚠️  Only {len(examples)} training examples. Recommend at least 50 for good accuracy.")
    
    logger.info("🧠 Initializing Sklearn Classifier...")
    ml_classifier = SklearnClassifier()
    
    logger.info("🔄 Starting model training...")
    logger.info("   This may take a few minutes...")
    
    try:
        # Train the model
        success = ml_classifier.train(examples)
        
        if success:
            logger.info("✅ Model trained successfully!")
            return True
        else:
            logger.error("❌ Model training failed!")
            return False
    
    except Exception as e:
        logger.error(f"❌ Error during training: {e}")
        import traceback
        traceback.print_exc()
        return False


def update_training_metadata(training_data_file: Path, model_info: dict):
    """Update training data file with retraining metadata."""
    
    try:
        with open(training_data_file, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
        
        # Update metadata
        training_data['last_retrained'] = datetime.utcnow().isoformat()
        training_data['model_version'] = training_data.get('model_version', 0) + 1
        training_data['retrain_info'] = model_info
        
        # Save updated data
        with open(training_data_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Updated training data metadata (version {training_data['model_version']})")
    
    except Exception as e:
        logger.warning(f"⚠️  Could not update training metadata: {e}")


def main():
    """Main retraining workflow."""
    
    logger.info("=" * 80)
    logger.info("🔄 ML Model Retraining Script")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    # Load training data
    training_data_file = Path("app/ml/training/training_data.json")
    training_data = load_training_data(training_data_file)
    
    if not training_data:
        logger.error("❌ Failed to load training data!")
        sys.exit(1)
    
    # Retrain model
    logger.info("")
    logger.info("=" * 80)
    logger.info("🧠 Starting Model Retraining")
    logger.info("=" * 80)
    
    success = retrain_model(training_data)
    
    if success:
        # Update metadata
        model_info = {
            "trained_at": datetime.utcnow().isoformat(),
            "training_examples": len(training_data.get('examples', [])),
            "success": True
        }
        update_training_metadata(training_data_file, model_info)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Retraining Complete!")
        logger.info("=" * 80)
        logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("🎯 Model is now ready to use with updated training data")
        logger.info("")
        
        # Test the model
        logger.info("🧪 Testing model with sample predictions...")
        ml_classifier = SklearnClassifier()
        
        test_job = "We are hiring Python developers with 2 years experience. Send resume to hr@company.com"
        test_non_job = "Hi guys, looking for job opportunities. Anyone hiring?"
        
        try:
            result1 = ml_classifier.classify(test_job)
            result2 = ml_classifier.classify(test_non_job)
            
            logger.info(f"   Test 1 (Job): is_job={result1.is_job}, confidence={result1.confidence:.2f}")
            logger.info(f"   Test 2 (Non-Job): is_job={result2.is_job}, confidence={result2.confidence:.2f}")
            
            if result1.is_job and not result2.is_job:
                logger.info("   ✅ Model predictions look good!")
            else:
                logger.warning("   ⚠️  Model predictions might need review")
        
        except Exception as e:
            logger.warning(f"   ⚠️  Could not test model: {e}")
        
        logger.info("")
        sys.exit(0)
    
    else:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Retraining Failed!")
        logger.error("=" * 80)
        logger.error("Please check the error messages above")
        logger.error("")
        sys.exit(1)


if __name__ == "__main__":
    main()
