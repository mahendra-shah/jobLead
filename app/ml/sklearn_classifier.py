"""
Scikit-learn based job classifier
Uses TF-IDF + Random Forest for classification
"""

import pickle
import joblib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import numpy as np

from app.ml.base_classifier import (
    BaseClassifier,
    ClassificationResult,
    ExtractionResult
)
from app.ml.utils.text_preprocessor import text_preprocessor
from app.ml.utils.feature_extractor import feature_extractor


class SklearnClassifier(BaseClassifier):
    """
    Scikit-learn Random Forest classifier for job detection
    Uses TF-IDF for text features + hand-crafted features
    """
    
    MODEL_DIR = Path(__file__).parent / "models"
    MODEL_FILE = MODEL_DIR / "job_classifier.pkl"
    METADATA_FILE = MODEL_DIR / "model_metadata.json"
    
    def __init__(self):
        super().__init__()
        self.vectorizer: Optional[TfidfVectorizer] = None
        self.classifier: Optional[RandomForestClassifier] = None
        self.feature_names: Optional[List[str]] = None
        self.threshold = 0.5
        
        self.MODEL_DIR.mkdir(parents=True, exist_ok=True)
        
        if self.MODEL_FILE.exists():
            try:
                self.load_model(str(self.MODEL_FILE))
            except Exception as e:
                print(f"⚠️  Failed to load model: {e}")
    
    def classify(self, text: str) -> ClassificationResult:
        """Classify if text contains a job posting"""
        start_time = time.time()
        
        if not self.is_loaded:
            return ClassificationResult(
                is_job=False,
                confidence=0.0,
                reason="Model not loaded - needs training",
                processing_time_ms=0.0
            )
        
        try:
            preprocessed = text_preprocessor.preprocess_for_ml(text)
            features = feature_extractor.extract_all(text)
            
            # Early exit - clearly not a job
            if features['has_non_job_keywords'] and not features['has_job_keywords']:
                return ClassificationResult(
                    is_job=False,
                    confidence=0.9,
                    reason="Contains non-job keywords, no job keywords",
                    features_used=features,
                    processing_time_ms=(time.time() - start_time) * 1000
                )
            
            # Early exit - very likely a job
            if (features['has_job_keywords'] and 
                features['has_job_title'] and 
                features['has_tech_skills'] and
                features['has_application_method']):
                return ClassificationResult(
                    is_job=True,
                    confidence=0.95,
                    reason="Strong job signals: keywords, title, skills, application",
                    features_used=features,
                    processing_time_ms=(time.time() - start_time) * 1000
                )
            
            # Use ML model
            text_features = self.vectorizer.transform([preprocessed])
            feature_vector = feature_extractor.features_to_vector(features)
            
            tfidf_array = text_features.toarray()[0]
            combined_features = np.concatenate([tfidf_array, feature_vector])
            combined_features = combined_features.reshape(1, -1)
            
            prediction = self.classifier.predict(combined_features)[0]
            probabilities = self.classifier.predict_proba(combined_features)[0]
            confidence = float(probabilities[1] if prediction else probabilities[0])
            
            reason = f"ML prediction: {'job' if prediction else 'not a job'} (confidence: {confidence:.2f})"
            
            return ClassificationResult(
                is_job=bool(prediction),
                confidence=confidence,
                reason=reason,
                features_used=features,
                processing_time_ms=(time.time() - start_time) * 1000
            )
            
        except Exception as e:
            return ClassificationResult(
                is_job=False,
                confidence=0.0,
                reason=f"Classification error: {str(e)}",
                processing_time_ms=(time.time() - start_time) * 1000
            )
    
    def extract(self, text: str) -> ExtractionResult:
        """Extract basic job details using patterns"""
        emails = text_preprocessor.extract_emails(text)
        urls = text_preprocessor.extract_urls(text)
        
        # Extract company (capitalized words)
        company = None
        words = text.split()
        for i, word in enumerate(words):
            if word and len(word) > 1 and word[0].isupper():
                if i + 1 < len(words) and len(words[i + 1]) > 1 and words[i + 1][0].isupper():
                    company = f"{word} {words[i + 1]}"
                    break
        
        # Extract location
        location = None
        text_lower = text.lower()
        for loc in feature_extractor.LOCATIONS:
            if loc in text_lower:
                location = loc.title()
                break
        
        # Extract job type
        job_type = None
        for jt in feature_extractor.JOB_TYPES:
            if jt in text_lower:
                job_type = jt.title()
                break
        
        # Extract skills
        skills = []
        for skill in feature_extractor.TECH_SKILLS:
            if skill in text_lower:
                skills.append(skill.title())
        
        apply_link = urls[0] if urls else None
        if not apply_link and emails:
            apply_link = f"mailto:{emails[0]}"
        
        return ExtractionResult(
            company=company,
            job_title=None,
            location=location,
            skills=skills,
            job_type=job_type,
            apply_link=apply_link,
            raw_text=text,
            confidence_scores={
                'company': 0.5 if company else 0.0,
                'location': 0.7 if location else 0.0,
                'skills': 0.8 if skills else 0.0,
            }
        )
    
    def train(self, training_data: List[Dict]) -> Dict:
        """Train the classifier"""
        if not training_data or len(training_data) < 10:
            return {"success": False, "error": "Need at least 10 training examples"}
        
        try:
            texts = []
            labels = []
            for example in training_data:
                texts.append(example['text'])
                labels.append(1 if example['is_job'] else 0)
            
            print(f"Training with {len(texts)} examples...")
            print(f"  Positive (jobs): {sum(labels)}")
            print(f"  Negative (non-jobs): {len(labels) - sum(labels)}")
            
            preprocessed_texts = [text_preprocessor.preprocess_for_ml(t) for t in texts]
            
            all_features = [feature_extractor.extract_all(t) for t in texts]
            feature_vectors = [feature_extractor.features_to_vector(f) for f in all_features]
            
            print("Creating TF-IDF features...")
            self.vectorizer = TfidfVectorizer(
                max_features=500,
                min_df=2,
                max_df=0.8,
                ngram_range=(1, 2),
                stop_words='english'
            )
            
            tfidf_features = self.vectorizer.fit_transform(preprocessed_texts)
            tfidf_array = tfidf_features.toarray()
            combined_features = np.hstack([tfidf_array, feature_vectors])
            
            print(f"Feature matrix shape: {combined_features.shape}")
            
            X_train, X_test, y_train, y_test = train_test_split(
                combined_features,
                labels,
                test_size=0.2,
                random_state=42,
                stratify=labels if len(set(labels)) > 1 else None
            )
            
            print("Training Random Forest classifier...")
            self.classifier = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1
            )
            
            self.classifier.fit(X_train, y_train)
            
            print("Evaluating model...")
            train_predictions = self.classifier.predict(X_train)
            test_predictions = self.classifier.predict(X_test)
            
            train_accuracy = accuracy_score(y_train, train_predictions)
            test_accuracy = accuracy_score(y_test, test_predictions)
            test_precision = precision_score(y_test, test_predictions, zero_division=0)
            test_recall = recall_score(y_test, test_predictions, zero_division=0)
            test_f1 = f1_score(y_test, test_predictions, zero_division=0)
            
            self.is_loaded = True
            self.model_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.last_trained = datetime.now()
            
            self.save_model(str(self.MODEL_FILE))
            
            metrics = {
                "success": True,
                "model_version": self.model_version,
                "training_samples": len(texts),
                "train_accuracy": float(train_accuracy),
                "test_accuracy": float(test_accuracy),
                "test_precision": float(test_precision),
                "test_recall": float(test_recall),
                "test_f1": float(test_f1),
                "feature_count": combined_features.shape[1],
                "tfidf_features": tfidf_array.shape[1],
                "handcrafted_features": len(feature_vectors[0]),
            }
            
            print("\n✅ Training complete!")
            print(f"   Train accuracy: {train_accuracy:.3f}")
            print(f"   Test accuracy: {test_accuracy:.3f}")
            print(f"   Precision: {test_precision:.3f}")
            print(f"   Recall: {test_recall:.3f}")
            print(f"   F1 Score: {test_f1:.3f}")
            
            return metrics
            
        except Exception as e:
            print(f"❌ Training error: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def save_model(self, path: str) -> None:
        """Save trained model to disk"""
        try:
            model_data = {
                'vectorizer': self.vectorizer,
                'classifier': self.classifier,
                'feature_names': feature_extractor.get_feature_names(),
                'threshold': self.threshold,
            }
            
            with open(path, 'wb') as f:
                pickle.dump(model_data, f)
            
            metadata = {
                'model_version': self.model_version,
                'last_trained': self.last_trained.isoformat() if self.last_trained else None,
                'saved_at': datetime.now().isoformat(),
                'file_size_mb': os.path.getsize(path) / (1024 * 1024),
            }
            
            with open(self.METADATA_FILE, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"✅ Model saved to {path}")
            print(f"   Size: {metadata['file_size_mb']:.2f} MB")
            
        except Exception as e:
            print(f"❌ Failed to save model: {e}")
            raise
    
    def load_model(self, path: str) -> None:
        """Load trained model from disk"""
        try:
            # Use joblib.load for scikit-learn models (supports compression and is faster)
            model_data = joblib.load(path)
            
            self.vectorizer = model_data['vectorizer']
            self.classifier = model_data['classifier']
            self.feature_names = model_data.get('feature_names')
            self.threshold = model_data.get('threshold', 0.5)
            
            # Load metadata from the model file itself (new format)
            metadata = model_data.get('metadata', {})
            self.model_version = metadata.get('version')
            training_date_str = metadata.get('training_date')
            if training_date_str:
                self.last_trained = datetime.fromisoformat(training_date_str)
            
            self.is_loaded = True
            print(f"✅ Model loaded from {path}")
            print(f"   Version: {self.model_version}")
            print(f"   Last trained: {self.last_trained}")
            print(f"   Accuracy: {metadata.get('accuracy', 'N/A')}")
            
        except Exception as e:
            print(f"❌ Failed to load model: {e}")
            raise


# Global instance
_sklearn_classifier = None

def get_sklearn_classifier() -> SklearnClassifier:
    """Get global sklearn classifier instance"""
    global _sklearn_classifier
    if _sklearn_classifier is None:
        _sklearn_classifier = SklearnClassifier()
    return _sklearn_classifier
