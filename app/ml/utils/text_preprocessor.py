"""
Text preprocessing utilities for ML pipeline
Handles cleaning, normalization, and tokenization
"""

import re
from typing import List, Tuple
from urllib.parse import urlparse


class TextPreprocessor:
    """Clean and normalize text for ML processing"""
    
    # Common stopwords to remove (keep job-relevant ones)
    STOPWORDS = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
        'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
        'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they'
    }
    
    # Patterns to identify
    EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    PHONE_PATTERN = re.compile(r'\b(?:\+91|91)?[-.\s]?\d{10}\b|\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b')
    URL_PATTERN = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    SPECIAL_CHARS = re.compile(r'[^a-zA-Z0-9\s\-_.]')
    MULTIPLE_SPACES = re.compile(r'\s+')
    
    def __init__(self):
        pass
    
    def clean(self, text: str, preserve_urls: bool = True, preserve_emails: bool = True) -> str:
        """
        Clean text while preserving important information
        
        Args:
            text: Raw text to clean
            preserve_urls: Keep URLs intact
            preserve_emails: Keep emails intact
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Store URLs and emails if preserving
        urls = []
        emails = []
        
        if preserve_urls:
            urls = self.URL_PATTERN.findall(text)
            text = self.URL_PATTERN.sub(' URL_PLACEHOLDER ', text)
        
        if preserve_emails:
            emails = self.EMAIL_PATTERN.findall(text)
            text = self.EMAIL_PATTERN.sub(' EMAIL_PLACEHOLDER ', text)
        
        # Remove phone numbers (not needed for classification)
        text = self.PHONE_PATTERN.sub(' ', text)
        
        # Remove excessive special characters
        text = self.SPECIAL_CHARS.sub(' ', text)
        
        # Normalize spaces
        text = self.MULTIPLE_SPACES.sub(' ', text)
        
        # Restore URLs and emails
        if preserve_urls:
            for url in urls:
                text = text.replace('URL_PLACEHOLDER', url, 1)
        
        if preserve_emails:
            for email in emails:
                text = text.replace('EMAIL_PLACEHOLDER', email, 1)
        
        return text.strip()
    
    def normalize_case(self, text: str, mode: str = 'lower') -> str:
        """
        Normalize text case
        
        Args:
            text: Text to normalize
            mode: 'lower', 'upper', or 'title'
            
        Returns:
            Normalized text
        """
        if mode == 'lower':
            return text.lower()
        elif mode == 'upper':
            return text.upper()
        elif mode == 'title':
            return text.title()
        return text
    
    def tokenize(self, text: str, remove_stopwords: bool = False) -> List[str]:
        """
        Tokenize text into words
        
        Args:
            text: Text to tokenize
            remove_stopwords: Remove common stopwords
            
        Returns:
            List of tokens
        """
        # Split on whitespace
        tokens = text.lower().split()
        
        # Remove stopwords if requested
        if remove_stopwords:
            tokens = [t for t in tokens if t not in self.STOPWORDS]
        
        return tokens
    
    def extract_sentences(self, text: str) -> List[str]:
        """
        Split text into sentences
        
        Args:
            text: Text to split
            
        Returns:
            List of sentences
        """
        # Simple sentence splitting on common delimiters
        sentences = re.split(r'[.!?\n]+', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def extract_urls(self, text: str) -> List[str]:
        """Extract all URLs from text"""
        return self.URL_PATTERN.findall(text)
    
    def extract_emails(self, text: str) -> List[str]:
        """Extract all email addresses from text"""
        return self.EMAIL_PATTERN.findall(text)
    
    def extract_phone_numbers(self, text: str) -> List[str]:
        """Extract all phone numbers from text"""
        return self.PHONE_PATTERN.findall(text)
    
    def remove_duplicates(self, text: str) -> str:
        """
        Remove duplicate consecutive words
        (common in Telegram forwards)
        """
        words = text.split()
        result = []
        prev_word = None
        
        for word in words:
            if word != prev_word:
                result.append(word)
            prev_word = word
        
        return ' '.join(result)
    
    def extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from text"""
        return re.findall(r'#\w+', text)
    
    def extract_mentions(self, text: str) -> List[str]:
        """Extract @mentions from text"""
        return re.findall(r'@\w+', text)
    
    def preprocess_for_ml(self, text: str) -> str:
        """
        Full preprocessing pipeline for ML
        
        Args:
            text: Raw text
            
        Returns:
            Preprocessed text ready for ML
        """
        # Clean
        text = self.clean(text, preserve_urls=False, preserve_emails=False)
        
        # Remove duplicates
        text = self.remove_duplicates(text)
        
        # Normalize case
        text = self.normalize_case(text, mode='lower')
        
        # Remove extra spaces
        text = self.MULTIPLE_SPACES.sub(' ', text).strip()
        
        return text
    
    def preprocess_for_extraction(self, text: str) -> str:
        """
        Preprocessing for entity extraction (preserve more info)
        
        Args:
            text: Raw text
            
        Returns:
            Preprocessed text ready for extraction
        """
        # Clean but preserve URLs and emails
        text = self.clean(text, preserve_urls=True, preserve_emails=True)
        
        # Remove duplicates
        text = self.remove_duplicates(text)
        
        # Remove extra spaces
        text = self.MULTIPLE_SPACES.sub(' ', text).strip()
        
        return text
    
    def get_text_stats(self, text: str) -> dict:
        """
        Get statistics about text
        
        Returns:
            Dictionary with text statistics
        """
        words = text.split()
        sentences = self.extract_sentences(text)
        
        return {
            'char_count': len(text),
            'word_count': len(words),
            'sentence_count': len(sentences),
            'avg_word_length': sum(len(w) for w in words) / len(words) if words else 0,
            'url_count': len(self.extract_urls(text)),
            'email_count': len(self.extract_emails(text)),
            'phone_count': len(self.extract_phone_numbers(text)),
            'hashtag_count': len(self.extract_hashtags(text)),
            'mention_count': len(self.extract_mentions(text)),
        }


# Global instance
text_preprocessor = TextPreprocessor()
