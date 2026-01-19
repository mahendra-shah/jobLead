"""
OpenAI Service Implementation
Uses GPT-4o-mini for job extraction and text-embedding-3-small for embeddings
"""
import json
import logging
from typing import Dict, List

from openai import AsyncOpenAI

from app.config import settings
from .base import AIProvider

logger = logging.getLogger(__name__)


class OpenAIService(AIProvider):
    """OpenAI API implementation"""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        self.chat_model = "gpt-4o-mini"  # Cheapest, fast
        self.embedding_model = "text-embedding-3-small"  # 1536 dimensions
    
    async def extract_job(self, message_text: str) -> Dict:
        """Extract job details using GPT-4o-mini"""
        try:
            prompt = self.build_job_extraction_prompt(message_text)
            
            response = await self.client.chat.completions.create(
                model=self.chat_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a job extraction AI. Extract structured job data from messages."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.1,  # Low temperature for consistency
                max_tokens=500
            )
            
            result = json.loads(response.choices[0].message.content)
            
            logger.info(f"OpenAI extracted job: {result.get('title', 'N/A')}")
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse OpenAI response: {e}")
            return {"is_job_posting": False, "error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"OpenAI extraction failed: {e}")
            return {"is_job_posting": False, "error": str(e)}
    
    async def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using text-embedding-3-small"""
        try:
            # Truncate text if too long (max 8191 tokens)
            if len(text) > 30000:  # Roughly 8000 tokens
                text = text[:30000]
            
            response = await self.client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            
            embedding = response.data[0].embedding
            logger.debug(f"Generated embedding with {len(embedding)} dimensions")
            return embedding
            
        except Exception as e:
            logger.error(f"OpenAI embedding failed: {e}")
            # Return zero vector on failure
            return [0.0] * 1536
    
    @property
    def name(self) -> str:
        return "openai"
    
    @property
    def cost_per_1k_tokens(self) -> float:
        # gpt-4o-mini: $0.15 per 1M tokens = $0.00015 per 1K tokens
        return 0.00015
    
    @property
    def embedding_dimensions(self) -> int:
        return 1536
