"""
Base AI Provider Interface
Abstract class for all AI providers (OpenAI, Gemini, OpenRouter)
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class AIProvider(ABC):
    """Base class for all AI providers"""
    
    @abstractmethod
    async def extract_job(self, message_text: str) -> Dict:
        """
        Extract job details from Telegram message
        
        Returns:
            {
                "is_job_posting": bool,
                "company": str | None,
                "title": str | None,
                "location": str | None,
                "job_type": "remote" | "office" | "hybrid" | None,
                "experience": str | None,
                "skills": List[str],
                "salary": str | None,
                "confidence": int (0-100)
            }
        """
        pass
    
    @abstractmethod
    async def generate_embedding(self, text: str) -> List[float]:
        """
        Generate text embedding for similarity matching
        
        Args:
            text: Text to generate embedding for
            
        Returns:
            List of floats (embedding vector, typically 1536 dimensions)
        """
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name"""
        pass
    
    @property
    @abstractmethod
    def cost_per_1k_tokens(self) -> float:
        """Cost per 1000 tokens in USD"""
        pass
    
    @property
    @abstractmethod
    def embedding_dimensions(self) -> int:
        """Embedding vector dimensions"""
        pass
    
    def build_job_extraction_prompt(self, message_text: str) -> str:
        """Build standardized prompt for job extraction"""
        return f"""You are a job extraction AI. Extract structured data from this Telegram message.

Message: {message_text}

Return ONLY valid JSON (no markdown, no code blocks):
{{
  "is_job_posting": true/false,
  "company": "string or null",
  "title": "string or null",
  "location": "string or null",
  "job_type": "remote/office/hybrid or null",
  "experience": "string or null",
  "skills": ["array", "of", "strings"],
  "salary": "string or null",
  "confidence": 0-100
}}

Rules:
- If not a job posting, return {{"is_job_posting": false}}
- Extract skills from context (Python, AWS, React, etc.)
- Normalize location names (Bengaluru → Bangalore, remote → remote)
- Keep salary as string with currency (preserve original format)
- Confidence 0-100 based on how clear the job posting is
- Only extract what's explicitly mentioned, don't infer
"""
