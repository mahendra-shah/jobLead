"""
OpenRouter Service Implementation
Uses various free/cheap models via OpenRouter API
"""
import json
import logging
import re
from typing import Dict, List

import httpx

from app.config import settings
from .base import AIProvider

logger = logging.getLogger(__name__)


class OpenRouterService(AIProvider):
    """OpenRouter API implementation (access to multiple models)"""
    
    def __init__(self):
        self.api_key = settings.OPENROUTER_API_KEY
        self.base_url = "https://openrouter.ai/api/v1"
        # Free model options:
        # - meta-llama/llama-3.1-8b-instruct:free
        # - google/gemini-flash-1.5-8b:free
        self.chat_model = "meta-llama/llama-3.1-8b-instruct:free"
        self.timeout = 60.0
    
    async def extract_job(self, message_text: str) -> Dict:
        """Extract job details using OpenRouter models"""
        try:
            prompt = self.build_job_extraction_prompt(message_text)
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "HTTP-Referer": settings.APP_URL or "https://placement-dashboard.com",
                        "X-Title": "Placement Dashboard",
                    },
                    json={
                        "model": self.chat_model,
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a job extraction AI. Extract structured job data."
                            },
                            {
                                "role": "user",
                                "content": prompt
                            }
                        ],
                        "temperature": 0.1,
                        "max_tokens": 500,
                    }
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Parse response
                content = data['choices'][0]['message']['content']
                
                # Extract JSON (models sometimes wrap in markdown)
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    result = json.loads(content)
                
                logger.info(f"OpenRouter extracted job: {result.get('title', 'N/A')}")
                return result
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse OpenRouter response: {e}")
            return {"is_job_posting": False, "error": "Invalid JSON response"}
        except httpx.HTTPError as e:
            logger.error(f"OpenRouter HTTP error: {e}")
            return {"is_job_posting": False, "error": str(e)}
        except Exception as e:
            logger.error(f"OpenRouter extraction failed: {e}")
            return {"is_job_posting": False, "error": str(e)}
    
    async def generate_embedding(self, text: str) -> List[float]:
        """
        OpenRouter doesn't provide embeddings, fallback to simple hash-based
        Note: For production, use OpenAI or Gemini for embeddings
        """
        logger.warning("OpenRouter doesn't support embeddings, returning zero vector")
        return [0.0] * 1536
    
    @property
    def name(self) -> str:
        return "openrouter"
    
    @property
    def cost_per_1k_tokens(self) -> float:
        # Free models available!
        return 0.0
    
    @property
    def embedding_dimensions(self) -> int:
        return 1536  # Match OpenAI for compatibility
