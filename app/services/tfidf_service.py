"""
TF-IDF based job matching service.
Fast pre-filtering stage for hybrid matching approach.
"""

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from typing import List, Dict, Tuple, Optional
import pickle
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class TFIDFMatchingService:
    """
    Lightweight TF-IDF based matching for fast pre-filtering.
    Used as Stage 1 in hybrid matching approach.
    """
    
    def __init__(self, cache_dir: str = "./models"):
        """
        Initialize TF-IDF service.
        
        Args:
            cache_dir: Directory to cache fitted vectorizer
        """
        self.vectorizer = TfidfVectorizer(
            max_features=500,  # Limit vocabulary size
            stop_words='english',
            ngram_range=(1, 2),  # Unigrams + bigrams for better matching
            min_df=2,  # Ignore terms appearing in < 2 documents
            max_df=0.8,  # Ignore terms appearing in > 80% documents
            lowercase=True,
            strip_accents='unicode'
        )
        self.job_vectors = None
        self.job_ids = []
        self.is_fitted = False
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
    def _prepare_job_text(self, job: Dict) -> str:
        """
        Combine all job fields into searchable text.
        
        Args:
            job: Job dictionary with fields
            
        Returns:
            Combined text for vectorization
        """
        parts = [
            job.get('title', ''),
            job.get('description', ''),
            job.get('requirements', ''),
            ' '.join(job.get('skills', [])) if isinstance(job.get('skills'), list) else job.get('skills', ''),
            job.get('company_name', ''),
            job.get('location', ''),
            job.get('job_type', ''),
            job.get('experience_level', '')
        ]
        return ' '.join(filter(None, parts))
    
    def _prepare_student_text(self, student: Dict) -> str:
        """
        Combine student profile into searchable text.
        
        Args:
            student: Student dictionary with fields
            
        Returns:
            Combined text for vectorization
        """
        parts = [
            # Skills (most important)
            ' '.join(student.get('skills', [])) if isinstance(student.get('skills'), list) else student.get('skills', ''),
            # Bio/Summary
            student.get('bio', ''),
            student.get('summary', ''),
            # Education
            ' '.join([
                f"{edu.get('degree', '')} {edu.get('field', '')} {edu.get('institution', '')}"
                for edu in student.get('education', [])
            ]),
            # Experience
            ' '.join([
                f"{exp.get('role', '')} {exp.get('company', '')} {exp.get('description', '')}"
                for exp in student.get('experience', [])
            ]),
            # Preferences
            student.get('preferences', {}).get('job_type', ''),
            student.get('preferences', {}).get('location', ''),
        ]
        return ' '.join(filter(None, parts))
    
    def fit_jobs(self, jobs: List[Dict]) -> None:
        """
        Fit vectorizer on job corpus and store vectors.
        
        Args:
            jobs: List of job dictionaries
        """
        if not jobs:
            logger.warning("No jobs provided for fitting")
            return
            
        logger.info(f"Fitting TF-IDF vectorizer on {len(jobs)} jobs")
        
        # Prepare job texts
        job_texts = [self._prepare_job_text(job) for job in jobs]
        self.job_ids = [job.get('id') for job in jobs]
        
        # Fit and transform
        self.job_vectors = self.vectorizer.fit_transform(job_texts)
        self.is_fitted = True
        
        logger.info(f"TF-IDF vocabulary size: {len(self.vectorizer.vocabulary_)}")
        logger.info(f"Job vectors shape: {self.job_vectors.shape}")
    
    def update_job(self, job: Dict) -> None:
        """
        Add or update a single job in the index.
        Note: This requires re-fitting for true TF-IDF. For efficiency,
        we transform using existing vocabulary.
        
        Args:
            job: Job dictionary
        """
        if not self.is_fitted:
            logger.warning("Vectorizer not fitted. Call fit_jobs() first.")
            return
            
        job_text = self._prepare_job_text(job)
        job_vector = self.vectorizer.transform([job_text])
        
        job_id = job.get('id')
        if job_id in self.job_ids:
            # Update existing
            idx = self.job_ids.index(job_id)
            # Note: scipy sparse matrix doesn't support item assignment
            # In production, consider using Annoy or FAISS for updates
            logger.info(f"Job {job_id} update requires re-fitting for accurate TF-IDF")
        else:
            # Add new
            self.job_ids.append(job_id)
            if self.job_vectors is not None:
                from scipy.sparse import vstack
                self.job_vectors = vstack([self.job_vectors, job_vector])
    
    def find_top_matches(
        self, 
        student: Dict, 
        top_k: int = 50,
        min_score: float = 0.0
    ) -> List[Tuple[str, float]]:
        """
        Find top K matching jobs for a student using TF-IDF similarity.
        
        Args:
            student: Student profile dictionary
            top_k: Number of top matches to return
            min_score: Minimum similarity score threshold
            
        Returns:
            List of (job_id, similarity_score) tuples, sorted by score descending
        """
        if not self.is_fitted or self.job_vectors is None:
            logger.warning("Vectorizer not fitted. Returning empty results.")
            return []
        
        # Prepare student text and vectorize
        student_text = self._prepare_student_text(student)
        student_vector = self.vectorizer.transform([student_text])
        
        # Calculate cosine similarities
        similarities = cosine_similarity(student_vector, self.job_vectors)[0]
        
        # Filter by minimum score
        valid_indices = np.where(similarities >= min_score)[0]
        
        if len(valid_indices) == 0:
            logger.info(f"No jobs found above threshold {min_score}")
            return []
        
        # Get top K indices
        top_indices = valid_indices[similarities[valid_indices].argsort()[-top_k:][::-1]]
        
        # Return (job_id, score) pairs
        results = [(self.job_ids[i], float(similarities[i])) for i in top_indices]
        
        logger.info(
            f"Found {len(results)} matches for student. "
            f"Top score: {results[0][1]:.3f}, Lowest: {results[-1][1]:.3f}"
        )
        
        return results
    
    def batch_match(
        self, 
        students: List[Dict], 
        top_k: int = 50
    ) -> Dict[str, List[Tuple[str, float]]]:
        """
        Batch matching for multiple students.
        More efficient than calling find_top_matches multiple times.
        
        Args:
            students: List of student profile dictionaries
            top_k: Number of matches per student
            
        Returns:
            Dictionary mapping student_id to list of (job_id, score) tuples
        """
        if not self.is_fitted or self.job_vectors is None:
            logger.warning("Vectorizer not fitted. Returning empty results.")
            return {}
        
        # Prepare all student texts
        student_texts = [self._prepare_student_text(s) for s in students]
        student_ids = [s.get('id') for s in students]
        
        # Vectorize all students at once
        student_vectors = self.vectorizer.transform(student_texts)
        
        # Calculate all similarities at once (efficient matrix operation)
        similarities = cosine_similarity(student_vectors, self.job_vectors)
        
        # Extract top K for each student
        results = {}
        for i, student_id in enumerate(student_ids):
            top_indices = similarities[i].argsort()[-top_k:][::-1]
            results[student_id] = [
                (self.job_ids[idx], float(similarities[i][idx])) 
                for idx in top_indices
            ]
        
        logger.info(f"Batch matched {len(students)} students")
        return results
    
    def save_model(self, filepath: str = "tfidf_model.pkl") -> None:
        """
        Save fitted vectorizer and job vectors to disk.
        
        Args:
            filepath: Path to save pickle file
        """
        if not self.is_fitted:
            logger.warning("Model not fitted. Nothing to save.")
            return
            
        save_path = self.cache_dir / filepath
        with open(save_path, 'wb') as f:
            pickle.dump({
                'vectorizer': self.vectorizer,
                'job_vectors': self.job_vectors,
                'job_ids': self.job_ids
            }, f)
        logger.info(f"Model saved to {save_path}")
    
    def load_model(self, filepath: str = "tfidf_model.pkl") -> bool:
        """
        Load fitted vectorizer and job vectors from disk.
        
        Args:
            filepath: Path to pickle file
            
        Returns:
            True if loaded successfully, False otherwise
        """
        load_path = self.cache_dir / filepath
        if not load_path.exists():
            logger.warning(f"Model file not found: {load_path}")
            return False
            
        try:
            with open(load_path, 'rb') as f:
                data = pickle.load(f)
            
            self.vectorizer = data['vectorizer']
            self.job_vectors = data['job_vectors']
            self.job_ids = data['job_ids']
            self.is_fitted = True
            
            logger.info(f"Model loaded from {load_path}")
            logger.info(f"Loaded {len(self.job_ids)} jobs")
            return True
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False
    
    def get_feature_importance(self, job_id: str, top_n: int = 10) -> List[Tuple[str, float]]:
        """
        Get most important features (terms) for a job.
        Useful for debugging and understanding matches.
        
        Args:
            job_id: Job ID
            top_n: Number of top features to return
            
        Returns:
            List of (term, score) tuples
        """
        if job_id not in self.job_ids:
            return []
            
        idx = self.job_ids.index(job_id)
        job_vector = self.job_vectors[idx].toarray()[0]
        
        # Get feature names
        feature_names = self.vectorizer.get_feature_names_out()
        
        # Get top N features
        top_indices = job_vector.argsort()[-top_n:][::-1]
        return [(feature_names[i], float(job_vector[i])) for i in top_indices]


# Singleton instance
_tfidf_service: Optional[TFIDFMatchingService] = None


def get_tfidf_service() -> TFIDFMatchingService:
    """
    Get or create singleton TF-IDF service instance.
    Used as dependency in FastAPI endpoints.
    """
    global _tfidf_service
    if _tfidf_service is None:
        _tfidf_service = TFIDFMatchingService()
    return _tfidf_service
