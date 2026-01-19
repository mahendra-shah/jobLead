"""
Ensemble classifier - combines sklearn + spaCy
Main interface for job classification and extraction
"""

from typing import List, Dict

from app.ml.base_classifier import (
    BaseClassifier,
    ClassificationResult,
    ExtractionResult
)
from app.ml.sklearn_classifier import get_sklearn_classifier
from app.ml.spacy_extractor import get_spacy_extractor


class EnsembleClassifier(BaseClassifier):
    """
    Ensemble classifier combining:
    - Sklearn classifier for classification
    - spaCy extractor for entity extraction
    """
    
    def __init__(self):
        super().__init__()
        self.sklearn_clf = get_sklearn_classifier()
        self.spacy_ext = get_spacy_extractor()
        
        # Ensemble is loaded if sklearn is loaded
        self.is_loaded = self.sklearn_clf.is_loaded
        self.model_version = self.sklearn_clf.model_version
        self.last_trained = self.sklearn_clf.last_trained
    
    def classify(self, text: str) -> ClassificationResult:
        """
        Classify if text contains a job posting
        Uses sklearn classifier
        
        Args:
            text: Message text to classify
            
        Returns:
            ClassificationResult
        """
        return self.sklearn_clf.classify(text)
    
    def extract(self, text: str) -> ExtractionResult:
        """
        Extract job details from text
        Uses sklearn + spaCy
        
        Args:
            text: Message text to extract from
            
        Returns:
            Enhanced ExtractionResult
        """
        # Get basic extraction from sklearn
        basic_result = self.sklearn_clf.extract(text)
        
        # Enhance with spaCy if available
        if self.spacy_ext.is_loaded:
            enhanced_result = self.spacy_ext.enhance_extraction(basic_result)
            return enhanced_result
        
        return basic_result
    
    def classify_and_extract(self, text: str) -> tuple[ClassificationResult, ExtractionResult]:
        """
        Combined classification and extraction
        More efficient than calling separately
        
        Args:
            text: Message text to process
            
        Returns:
            Tuple of (ClassificationResult, ExtractionResult)
        """
        classification = self.classify(text)
        
        # Only extract if classified as job
        if classification.is_job:
            extraction = self.extract(text)
        else:
            extraction = ExtractionResult(raw_text=text)
        
        return classification, extraction
    
    def train(self, training_data: List[Dict]) -> Dict:
        """
        Train the sklearn classifier
        
        Args:
            training_data: List of labeled examples
                Each example: {"text": str, "is_job": bool}
        
        Returns:
            Training metrics
        """
        metrics = self.sklearn_clf.train(training_data)
        
        # Update ensemble info
        if metrics.get('success'):
            self.is_loaded = self.sklearn_clf.is_loaded
            self.model_version = self.sklearn_clf.model_version
            self.last_trained = self.sklearn_clf.last_trained
        
        return metrics
    
    def save_model(self, path: str) -> None:
        """Save sklearn model"""
        self.sklearn_clf.save_model(path)
    
    def load_model(self, path: str) -> None:
        """Load sklearn model"""
        self.sklearn_clf.load_model(path)
        self.is_loaded = self.sklearn_clf.is_loaded
        self.model_version = self.sklearn_clf.model_version
        self.last_trained = self.sklearn_clf.last_trained
    
    def get_model_info(self) -> Dict:
        """Get information about ensemble components"""
        return {
            "ensemble": {
                "is_loaded": self.is_loaded,
                "model_version": self.model_version,
                "last_trained": self.last_trained.isoformat() if self.last_trained else None,
            },
            "sklearn": self.sklearn_clf.get_model_info(),
            "spacy": {
                "is_loaded": self.spacy_ext.is_loaded,
            }
        }


# Global instance - this is what should be imported
job_classifier = EnsembleClassifier()
