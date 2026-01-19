"""Helper utilities."""

import hashlib
import re
from typing import Any, Dict, List


def normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    return " ".join(text.lower().split())


def generate_hash(text: str) -> str:
    """Generate SHA256 hash of text."""
    return hashlib.sha256(text.encode()).hexdigest()


def extract_email(text: str) -> str:
    """Extract email from text."""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, text)
    return match.group(0) if match else None


def extract_phone(text: str) -> str:
    """Extract phone number from text."""
    phone_pattern = r'\b\d{10,}\b'
    match = re.search(phone_pattern, text)
    return match.group(0) if match else None


def extract_urls(text: str) -> List[str]:
    """Extract URLs from text."""
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    return re.findall(url_pattern, text)


def calculate_similarity(text1: str, text2: str) -> float:
    """Calculate simple similarity between two texts."""
    words1 = set(normalize_text(text1).split())
    words2 = set(normalize_text(text2).split())
    
    if not words1 or not words2:
        return 0.0
    
    intersection = words1.intersection(words2)
    union = words1.union(words2)
    
    return len(intersection) / len(union) if union else 0.0


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for storage."""
    # Remove special characters
    sanitized = re.sub(r'[^\w\s.-]', '', filename)
    # Replace spaces with underscores
    sanitized = re.sub(r'\s+', '_', sanitized)
    return sanitized[:255]  # Limit length


def paginate_query(query: Any, page: int = 1, page_size: int = 20) -> Dict:
    """Helper for pagination."""
    offset = (page - 1) * page_size
    return {
        "offset": offset,
        "limit": page_size,
        "page": page,
        "page_size": page_size,
    }
