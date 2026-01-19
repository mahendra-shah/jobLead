"""
Base classifier interface for job posting classification
Defines abstract interface that all ML classifiers must implement
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime


@dataclass
class ClassificationResult:
    """Result from job classification"""
    is_job: bool
    confidence: float  # 0.0 to 1.0
    reason: str
    features_used: Optional[Dict] = None
    processing_time_ms: float = 0.0
    
    def is_high_confidence(self, threshold: float = 0.8) -> bool:
        """Check if prediction is high confidence"""
        return self.confidence >= threshold
    
    def needs_review(self, threshold: float = 0.8) -> bool:
        """Check if prediction needs manual review"""
        return self.confidence < threshold


@dataclass
class ExtractionResult:
    """Result from entity extraction"""
    company: Optional[str] = None
    job_title: Optional[str] = None
    location: Optional[str] = None
    skills: List[str] = None
    job_type: Optional[str] = None
    experience_required: Optional[str] = None
    salary: Optional[str] = None
    apply_link: Optional[str] = None
    raw_text: str = ""
    confidence_scores: Optional[Dict[str, float]] = None
    
    def __post_init__(self):
        if self.skills is None:
            self.skills = []
        if self.confidence_scores is None:
            self.confidence_scores = {}
    
    def is_complete(self) -> bool:
        """Check if extraction has minimum required fields"""
        return bool(self.company and self.job_title)
    
    def get_missing_fields(self) -> List[str]:
        """Get list of important missing fields"""
        missing = []
        if not self.company:
            missing.append("company")
        if not self.job_title:
            missing.append("job_title")
        if not self.location:
            missing.append("location")
        if not self.skills:
            missing.append("skills")
        return missing


class BaseClassifier(ABC):
    """
    Abstract base class for all job classifiers
    Defines the interface that must be implemented
    """
    
    def __init__(self):
        self.is_loaded = False
        self.model_version: Optional[str] = None
        self.last_trained: Optional[datetime] = None
    
    @abstractmethod
    def classify(self, text: str) -> ClassificationResult:
        """
        Classify if text contains a job posting
        
        Args:
            text: Message text to classify
            
        Returns:
            ClassificationResult with is_job, confidence, reason
        """
        pass
    
    @abstractmethod
    def extract(self, text: str) -> ExtractionResult:
        """
        Extract job details from text
        
        Args:
            text: Message text to extract from
            
        Returns:
            ExtractionResult with company, title, location, etc.
        """
        pass
    
    @abstractmethod
    def train(self, training_data: List[Dict]) -> Dict:
        """
        Train or retrain the classifier
        
        Args:
            training_data: List of labeled examples
                Each example: {"text": str, "is_job": bool, "metadata": dict}
        
        Returns:
            Training metrics (accuracy, precision, recall, etc.)
        """
        pass
    
    @abstractmethod
    def save_model(self, path: str) -> None:
        """
        Save trained model to disk
        
        Args:
            path: File path to save model
        """
        pass
    
    @abstractmethod
    def load_model(self, path: str) -> None:
        """
        Load trained model from disk
        
        Args:
            path: File path to load model from
        """
        pass
    
    def get_model_info(self) -> Dict:
        """Get information about current model"""
        return {
            "is_loaded": self.is_loaded,
            "model_version": self.model_version,
            "last_trained": self.last_trained.isoformat() if self.last_trained else None,
        }
    
    def validate_classification(self, result: ClassificationResult) -> bool:
        """Validate classification result"""
        if not isinstance(result, ClassificationResult):
            return False
        if not 0.0 <= result.confidence <= 1.0:
            return False
        if result.is_job and result.confidence < 0.5:
            return False  # Job predictions should have >50% confidence
        return True
    
    def validate_extraction(self, result: ExtractionResult) -> bool:
        """Validate extraction result"""
        if not isinstance(result, ExtractionResult):
            return False
        # At minimum, should have some text
        if not result.raw_text:
            return False
        return True
