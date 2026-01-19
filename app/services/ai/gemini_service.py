"""
Google Gemini Service Implementation
Uses Gemini 1.5 Flash for job extraction (cheaper alternative to OpenAI)
"""
import json
import logging
import re
from typing import Dict, List

import google.generativeai as genai

from app.config import settings
from .base import AIProvider

logger = logging.getLogger(__name__)


class GeminiService(AIProvider):
    """Google Gemini API implementation"""
    
    def __init__(self):
        genai.configure(api_key=settings.GEMINI_API_KEY)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        self.embedding_model = "models/embedding-001"
    
    async def extract_job(self, message_text: str) -> Dict:
        """Extract job details using Gemini 1.5 Flash"""
        try:
            prompt = self.build_job_extraction_prompt(message_text)
            
            response = await self.model.generate_content_async(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=500,
                )
            )
            
            # Extract JSON from response (Gemini sometimes wraps in markdown)
            text = response.text
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = json.loads(text)
            
            logger.info(f"Gemini extracted job: {result.get('title', 'N/A')}")
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return {"is_job_posting": False, "error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"Gemini extraction failed: {e}")
            return {"is_job_posting": False, "error": str(e)}
    
    async def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using Gemini embedding-001"""
        try:
            # Truncate if too long
            if len(text) > 20000:
                text = text[:20000]
            
            result = genai.embed_content(
                model=self.embedding_model,
                content=text,
                task_type="retrieval_document"
            )
            
            embedding = result['embedding']
            logger.debug(f"Generated Gemini embedding with {len(embedding)} dimensions")
            return embedding
            
        except Exception as e:
            logger.error(f"Gemini embedding failed: {e}")
            # Return zero vector on failure (768 dimensions for Gemini)
            return [0.0] * 768
    
    @property
    def name(self) -> str:
        return "gemini"
    
    @property
    def cost_per_1k_tokens(self) -> float:
        # Gemini 1.5 Flash: $0.075 per 1M tokens = $0.000075 per 1K tokens
        # 50% cheaper than OpenAI!
        return 0.000075
    
    @property
    def embedding_dimensions(self) -> int:
        return 768  # Gemini embedding-001 uses 768 dimensions
