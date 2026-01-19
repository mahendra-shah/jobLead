#!/usr/bin/env python3
"""
Training script for sklearn job classifier
Run this to train the initial model or retrain after corrections
"""

import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from app.ml.ensemble_classifier import job_classifier


def load_training_data(file_path: str = None):
    """Load training data from JSON file"""
    if file_path is None:
        file_path = Path(__file__).parent / "training_data.json"
    
    print(f"Loading training data from: {file_path}")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    examples = data.get('examples', [])
    print(f"Loaded {len(examples)} examples")
    
    return examples


def main():
    """Main training function"""
    print("=" * 60)
    print("Job Classifier Training Script")
    print("=" * 60)
    print()
    
    # Load training data
    training_data = load_training_data()
    
    if not training_data:
        print("❌ No training data found!")
        return 1
    
    # Train model
    print("\nStarting training...")
    print("-" * 60)
    
    metrics = job_classifier.train(training_data)
    
    print("\n" + "=" * 60)
    
    if metrics.get('success'):
        print("✅ TRAINING SUCCESSFUL!")
        print("=" * 60)
        print(f"Model Version: {metrics['model_version']}")
        print(f"Training Samples: {metrics['training_samples']}")
        print(f"Train Accuracy: {metrics['train_accuracy']:.3f}")
        print(f"Test Accuracy: {metrics['test_accuracy']:.3f}")
        print(f"Precision: {metrics['test_precision']:.3f}")
        print(f"Recall: {metrics['test_recall']:.3f}")
        print(f"F1 Score: {metrics['test_f1']:.3f}")
        print(f"Total Features: {metrics['feature_count']}")
        print(f"  - TF-IDF: {metrics['tfidf_features']}")
        print(f"  - Hand-crafted: {metrics['handcrafted_features']}")
        print()
        print("Model ready for use! 🚀")
        return 0
    else:
        print("❌ TRAINING FAILED!")
        print("=" * 60)
        print(f"Error: {metrics.get('error', 'Unknown error')}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
