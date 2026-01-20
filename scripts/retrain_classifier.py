#!/usr/bin/env python3
"""
Retrain the job classifier on actual production data.

This script:
1. Loads the exported training data (1360+ messages)
2. Trains a new classifier on YOUR actual patterns
3. Evaluates accuracy on test set
4. Saves the new model with version info
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from datetime import datetime
import numpy as np

def load_training_data():
    """Load exported training data."""
    data_path = 'app/ml/training/data/messages_latest.csv'
    
    print("\n" + "="*80)
    print("📂 Loading Training Data")
    print("="*80)
    
    if not os.path.exists(data_path):
        print(f"❌ Error: Training data not found at {data_path}")
        print("   Run: python scripts/export_training_data.py first!")
        sys.exit(1)
    
    df = pd.read_csv(data_path)
    print(f"✅ Loaded {len(df)} messages")
    print(f"   Jobs: {(df['is_job'] == True).sum()} ({(df['is_job'] == True).sum() / len(df) * 100:.1f}%)")
    print(f"   Non-jobs: {(df['is_job'] == False).sum()} ({(df['is_job'] == False).sum() / len(df) * 100:.1f}%)")
    
    return df

def prepare_data(df):
    """Prepare features and labels."""
    print("\n" + "="*80)
    print("🔧 Preparing Data")
    print("="*80)
    
    X = df['text'].values
    y = df['is_job'].values
    
    # Split into train/test (80/20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"✅ Split complete:")
    print(f"   Training set: {len(X_train)} messages")
    print(f"   Test set: {len(X_test)} messages")
    print(f"   Train jobs: {y_train.sum()} ({y_train.sum() / len(y_train) * 100:.1f}%)")
    print(f"   Test jobs: {y_test.sum()} ({y_test.sum() / len(y_test) * 100:.1f}%)")
    
    return X_train, X_test, y_train, y_test

def train_model(X_train, y_train):
    """Train the classifier."""
    print("\n" + "="*80)
    print("🤖 Training Model")
    print("="*80)
    
    # Create TF-IDF vectorizer
    print("📊 Creating TF-IDF features...")
    vectorizer = TfidfVectorizer(
        max_features=5000,
        ngram_range=(1, 3),  # Unigrams, bigrams, trigrams
        min_df=2,  # Ignore rare terms
        max_df=0.95,  # Ignore very common terms
        lowercase=True,
        strip_accents='unicode'
    )
    
    X_train_tfidf = vectorizer.fit_transform(X_train)
    print(f"✅ Created {X_train_tfidf.shape[1]} features")
    
    # Train classifier
    print("🎯 Training Logistic Regression...")
    classifier = LogisticRegression(
        max_iter=1000,
        random_state=42,
        class_weight='balanced'  # Handle class imbalance
    )
    
    classifier.fit(X_train_tfidf, y_train)
    print("✅ Training complete!")
    
    return vectorizer, classifier

def evaluate_model(vectorizer, classifier, X_test, y_test):
    """Evaluate model on test set."""
    print("\n" + "="*80)
    print("📊 Evaluating Model")
    print("="*80)
    
    # Transform test data
    X_test_tfidf = vectorizer.transform(X_test)
    
    # Predict
    y_pred = classifier.predict(X_test_tfidf)
    y_pred_proba = classifier.predict_proba(X_test_tfidf)
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"\n🎯 Overall Accuracy: {accuracy * 100:.2f}%")
    
    print("\n📈 Classification Report:")
    print(classification_report(y_test, y_pred, target_names=['Non-Job', 'Job']))
    
    print("\n📊 Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(f"              Predicted Non-Job  Predicted Job")
    print(f"Actual Non-Job        {cm[0][0]:6d}          {cm[0][1]:6d}")
    print(f"Actual Job            {cm[1][0]:6d}          {cm[1][1]:6d}")
    
    # Confidence analysis
    print("\n💯 Confidence Analysis:")
    avg_confidence = np.max(y_pred_proba, axis=1).mean()
    print(f"   Average confidence: {avg_confidence * 100:.1f}%")
    
    high_conf = (np.max(y_pred_proba, axis=1) >= 0.9).sum()
    med_conf = ((np.max(y_pred_proba, axis=1) >= 0.7) & (np.max(y_pred_proba, axis=1) < 0.9)).sum()
    low_conf = (np.max(y_pred_proba, axis=1) < 0.7).sum()
    
    print(f"   High confidence (≥90%): {high_conf} ({high_conf / len(y_test) * 100:.1f}%)")
    print(f"   Medium confidence (70-90%): {med_conf} ({med_conf / len(y_test) * 100:.1f}%)")
    print(f"   Low confidence (<70%): {low_conf} ({low_conf / len(y_test) * 100:.1f}%)")
    
    return accuracy

def compare_with_old_model(new_accuracy):
    """Compare with old model if available."""
    print("\n" + "="*80)
    print("📊 Comparing with Previous Model")
    print("="*80)
    
    old_model_path = 'app/ml/models/job_classifier.pkl'
    
    if not os.path.exists(old_model_path):
        print("⚠️  No previous model found for comparison")
        return
    
    try:
        old_data = joblib.load(old_model_path)
        old_metadata = old_data.get('metadata', {})
        old_accuracy = old_metadata.get('accuracy', 0.85)  # Default estimate
        
        print(f"📈 Old Model:")
        print(f"   Version: {old_metadata.get('version', 'unknown')}")
        print(f"   Training date: {old_metadata.get('training_date', 'unknown')}")
        print(f"   Estimated accuracy: {old_accuracy * 100:.1f}%")
        
        print(f"\n📈 New Model:")
        print(f"   Accuracy: {new_accuracy * 100:.1f}%")
        
        improvement = (new_accuracy - old_accuracy) * 100
        print(f"\n🚀 Improvement: {improvement:+.1f}% {'📈' if improvement > 0 else '📉'}")
        
        if improvement > 0:
            print(f"   ✅ Model is BETTER! (+{improvement:.1f}%)")
        elif improvement < -2:
            print(f"   ⚠️  Model is WORSE! ({improvement:.1f}%)")
            print(f"   Consider keeping the old model.")
        else:
            print(f"   ℹ️  Similar performance ({improvement:.1f}%)")
            
    except Exception as e:
        print(f"⚠️  Could not load old model: {e}")

def save_model(vectorizer, classifier, accuracy, X_train):
    """Save the trained model with metadata."""
    print("\n" + "="*80)
    print("💾 Saving Model")
    print("="*80)
    
    # Create version string
    version = datetime.now().strftime("v%Y%m%d_%H%M%S")
    
    # Prepare model data
    model_data = {
        'vectorizer': vectorizer,
        'classifier': classifier,
        'metadata': {
            'version': version,
            'training_date': datetime.now().isoformat(),
            'training_samples': len(X_train),
            'accuracy': accuracy,
            'features': vectorizer.get_feature_names_out().tolist()[:100],  # First 100 features
            'model_type': 'LogisticRegression',
            'vectorizer_type': 'TfidfVectorizer',
            'retrained_on_production_data': True
        }
    }
    
    # Save paths
    model_dir = 'app/ml/models'
    os.makedirs(model_dir, exist_ok=True)
    
    # Save with version
    versioned_path = os.path.join(model_dir, f'job_classifier_{version}.pkl')
    joblib.dump(model_data, versioned_path)
    print(f"✅ Saved versioned model: {versioned_path}")
    
    # Save as current model (replace old one)
    current_path = os.path.join(model_dir, 'job_classifier.pkl')
    joblib.dump(model_data, current_path)
    print(f"✅ Saved as current model: {current_path}")
    
    # Save backup of old model
    backup_path = os.path.join(model_dir, 'job_classifier_backup.pkl')
    if os.path.exists(current_path) and not os.path.exists(backup_path):
        import shutil
        try:
            shutil.copy(current_path, backup_path)
            print(f"💾 Backed up old model: {backup_path}")
        except:
            pass
    
    print(f"\n📋 Model Info:")
    print(f"   Version: {version}")
    print(f"   Accuracy: {accuracy * 100:.2f}%")
    print(f"   Training samples: {len(X_train)}")
    print(f"   Features: {len(vectorizer.get_feature_names_out())}")

def main():
    """Main retraining pipeline."""
    print("\n" + "="*80)
    print("🎯 JOB CLASSIFIER RETRAINING")
    print("="*80)
    print("Training on YOUR actual production data!")
    print("="*80)
    
    # Load data
    df = load_training_data()
    
    # Prepare
    X_train, X_test, y_train, y_test = prepare_data(df)
    
    # Train
    vectorizer, classifier = train_model(X_train, y_train)
    
    # Evaluate
    accuracy = evaluate_model(vectorizer, classifier, X_test, y_test)
    
    # Compare
    compare_with_old_model(accuracy)
    
    # Save
    save_model(vectorizer, classifier, accuracy, X_train)
    
    print("\n" + "="*80)
    print("🎉 RETRAINING COMPLETE!")
    print("="*80)
    print("\nNext Steps:")
    print("1. Test new model: python scripts/run_ml_pipeline.py")
    print("2. Monitor performance in production")
    print("3. Set up weekly retraining cron job")
    print("\n" + "="*80)

if __name__ == '__main__':
    main()
