"""Job deduplication service using TF-IDF similarity."""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)


class DeduplicationService:
    """
    Service for detecting duplicate job postings.
    
    Uses multiple strategies:
    1. Exact text hash matching (fastest)
    2. TF-IDF cosine similarity (semantic matching)
    3. Key field matching (company + title + location)
    """
    
    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize deduplication service.
        
        Args:
            similarity_threshold: Minimum similarity score (0-1) to consider duplicate
        """
        self.similarity_threshold = similarity_threshold
        self.vectorizer = TfidfVectorizer(
            max_features=200,
            ngram_range=(1, 2),
            min_df=1,
            stop_words='english',
            lowercase=True
        )
    
    def compute_content_hash(self, text: str) -> str:
        """
        Compute MD5 hash of normalized text.
        
        Args:
            text: Job posting text
        
        Returns:
            MD5 hash string (32 chars)
        """
        # Normalize: lowercase, remove extra spaces
        normalized = ' '.join(text.lower().split())
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def compute_similarity(self, text1: str, text2: str) -> float:
        """
        Compute TF-IDF cosine similarity between two texts.
        
        Args:
            text1: First job posting text
            text2: Second job posting text
        
        Returns:
            Similarity score (0.0 to 1.0)
        """
        try:
            # Vectorize both texts
            vectors = self.vectorizer.fit_transform([text1, text2])
            
            # Compute cosine similarity
            similarity_matrix = cosine_similarity(vectors)
            
            # Return similarity between text1 and text2
            return float(similarity_matrix[0, 1])
            
        except Exception as e:
            logger.error(f"Error computing similarity: {e}")
            return 0.0
    
    def are_similar_by_fields(
        self,
        job1_data: dict,
        job2_data: dict
    ) -> Tuple[bool, float]:
        """
        Check if two jobs are similar based on key fields.
        
        Args:
            job1_data: First job dict with company, title, location
            job2_data: Second job dict
        
        Returns:
            Tuple of (is_similar: bool, score: float)
        """
        score = 0.0
        weights = {'company': 0.4, 'title': 0.4, 'location': 0.2}
        
        # Compare company (case-insensitive)
        company1 = str(job1_data.get('company', '')).lower().strip()
        company2 = str(job2_data.get('company', '')).lower().strip()
        if company1 and company2 and company1 == company2:
            score += weights['company']
        
        # Compare title (case-insensitive, partial match)
        title1 = str(job1_data.get('title', '')).lower().strip()
        title2 = str(job2_data.get('title', '')).lower().strip()
        if title1 and title2:
            # Exact match
            if title1 == title2:
                score += weights['title']
            # Partial match (one contains the other)
            elif title1 in title2 or title2 in title1:
                score += weights['title'] * 0.7
        
        # Compare location (case-insensitive)
        loc1 = str(job1_data.get('location', '')).lower().strip()
        loc2 = str(job2_data.get('location', '')).lower().strip()
        if loc1 and loc2:
            if loc1 == loc2:
                score += weights['location']
            elif loc1 in loc2 or loc2 in loc1:
                score += weights['location'] * 0.5
        
        # Consider similar if score > 0.7 (70% match)
        return (score >= 0.7, score)
    
    async def find_duplicate(
        self,
        job_text: str,
        job_data: dict,
        db_session,
        days_back: int = 7
    ) -> Optional[dict]:
        """
        Find if a duplicate job exists in the database.
        
        Args:
            job_text: Raw job posting text
            job_data: Extracted job data (company, title, location, etc.)
            db_session: Database session
            days_back: How many days to look back for duplicates
        
        Returns:
            Duplicate job dict or None
        """
        try:
            from app.models.job import Job
            from sqlalchemy import and_
            
            # Calculate cutoff date
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            
            # Strategy 1: Exact hash match (fastest)
            content_hash = self.compute_content_hash(job_text)
            exact_match = db_session.query(Job).filter(
                and_(
                    Job.content_hash == content_hash,
                    Job.created_at >= cutoff_date,
                    Job.is_active == True
                )
            ).first()
            
            if exact_match:
                logger.info(f"Found exact duplicate by hash: {exact_match.id}")
                return {
                    'id': str(exact_match.id),
                    'title': exact_match.title,
                    'company_id': str(exact_match.company_id),
                    'match_type': 'exact_hash',
                    'similarity': 1.0
                }
            
            # Strategy 2: Field-based matching
            # Get recent jobs from same company
            company_name = job_data.get('company', '').lower().strip()
            if company_name:
                # Get jobs from database (simplified - would need company table join)
                recent_jobs = db_session.query(Job).filter(
                    and_(
                        Job.created_at >= cutoff_date,
                        Job.is_active == True
                    )
                ).limit(100).all()  # Limit to last 100 jobs for performance
                
                for existing_job in recent_jobs:
                    existing_data = {
                        'company': existing_job.company_id,  # Would need to resolve company name
                        'title': existing_job.title,
                        'location': existing_job.location
                    }
                    
                    is_similar, field_score = self.are_similar_by_fields(job_data, existing_data)
                    
                    if is_similar:
                        logger.info(f"Found field-based duplicate: {existing_job.id} (score: {field_score:.2f})")
                        return {
                            'id': str(existing_job.id),
                            'title': existing_job.title,
                            'company_id': str(existing_job.company_id),
                            'match_type': 'field_match',
                            'similarity': field_score
                        }
            
            # Strategy 3: TF-IDF similarity (slowest, most accurate)
            # Only check top 50 most recent jobs for performance
            recent_jobs = db_session.query(Job).filter(
                and_(
                    Job.created_at >= cutoff_date,
                    Job.is_active == True
                )
            ).order_by(Job.created_at.desc()).limit(50).all()
            
            for existing_job in recent_jobs:
                if existing_job.raw_text:
                    similarity = self.compute_similarity(job_text, existing_job.raw_text)
                    
                    if similarity >= self.similarity_threshold:
                        logger.info(f"Found TF-IDF duplicate: {existing_job.id} (similarity: {similarity:.2f})")
                        return {
                            'id': str(existing_job.id),
                            'title': existing_job.title,
                            'company_id': str(existing_job.company_id),
                            'match_type': 'tfidf_similarity',
                            'similarity': similarity
                        }
            
            # No duplicate found
            logger.debug("No duplicate found")
            return None
            
        except Exception as e:
            logger.error(f"Error finding duplicate: {e}", exc_info=True)
            return None
    
    async def check_duplicate_simple(
        self,
        job_text: str,
        recent_jobs_texts: list,
        threshold: Optional[float] = None
    ) -> Tuple[bool, float]:
        """
        Simplified duplicate check against a list of recent job texts.
        
        Args:
            job_text: New job posting text
            recent_jobs_texts: List of recent job texts to compare against
            threshold: Custom threshold (uses default if None)
        
        Returns:
            Tuple of (is_duplicate: bool, max_similarity: float)
        """
        if not recent_jobs_texts:
            return False, 0.0
        
        threshold = threshold or self.similarity_threshold
        max_similarity = 0.0
        
        try:
            # Compute hash for exact match
            new_hash = self.compute_content_hash(job_text)
            
            for existing_text in recent_jobs_texts:
                # Check exact hash match
                existing_hash = self.compute_content_hash(existing_text)
                if new_hash == existing_hash:
                    return True, 1.0
                
                # Check TF-IDF similarity
                similarity = self.compute_similarity(job_text, existing_text)
                max_similarity = max(max_similarity, similarity)
                
                if similarity >= threshold:
                    return True, similarity
            
            return False, max_similarity
            
        except Exception as e:
            logger.error(f"Error in simple duplicate check: {e}")
            return False, 0.0


# Singleton instance
deduplication_service = DeduplicationService()
