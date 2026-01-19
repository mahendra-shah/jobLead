"""
OpenAI Embeddings service for high-quality semantic matching.
Used as Stage 2 in hybrid matching approach.
"""

import openai
from typing import List, Optional, Dict
import json
import asyncio
import numpy as np
from redis import asyncio as aioredis
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


class OpenAIEmbeddingService:
    """
    OpenAI embeddings service with Redis caching.
    Used for accurate semantic matching in Stage 2 of hybrid approach.
    """
    
    # Model configuration
    MODEL = "text-embedding-3-small"  # 1536 dimensions, $0.02 per 1M tokens
    CACHE_TTL = 86400  # 24 hours cache
    MAX_TOKENS = 8191  # Model token limit
    
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        """
        Initialize OpenAI service.
        
        Args:
            redis_client: Redis client for caching (optional)
        """
        openai.api_key = settings.OPENAI_API_KEY
        self.redis = redis_client
        self.client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        
    async def get_embedding(
        self, 
        text: str, 
        cache_key: Optional[str] = None,
        use_cache: bool = True
    ) -> List[float]:
        """
        Get embedding vector for text with optional caching.
        
        Args:
            text: Text to embed
            cache_key: Redis cache key (e.g., "job:123" or "student:456")
            use_cache: Whether to use Redis cache
            
        Returns:
            Embedding vector (1536 dimensions)
            
        Raises:
            Exception: If OpenAI API call fails
        """
        # Try cache first
        if use_cache and cache_key and self.redis:
            try:
                cached = await self.redis.get(f"emb:{cache_key}")
                if cached:
                    logger.debug(f"Cache hit for {cache_key}")
                    return json.loads(cached)
            except Exception as e:
                logger.warning(f"Redis get error: {e}")
        
        # Truncate text if too long (conservative estimate: 1 token ≈ 4 chars)
        if len(text) > self.MAX_TOKENS * 3:
            text = text[:self.MAX_TOKENS * 3]
            logger.warning(f"Text truncated to {len(text)} characters")
        
        try:
            # Call OpenAI API
            logger.debug(f"Calling OpenAI API for embedding (cache_key: {cache_key})")
            response = await self.client.embeddings.create(
                input=text,
                model=self.MODEL
            )
            embedding = response.data[0].embedding
            
            # Cache the result
            if use_cache and cache_key and self.redis:
                try:
                    await self.redis.setex(
                        f"emb:{cache_key}",
                        self.CACHE_TTL,
                        json.dumps(embedding)
                    )
                    logger.debug(f"Cached embedding for {cache_key}")
                except Exception as e:
                    logger.warning(f"Redis set error: {e}")
            
            return embedding
            
        except openai.RateLimitError as e:
            logger.error(f"OpenAI rate limit: {e}")
            raise
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting embedding: {e}")
            raise
    
    async def get_embeddings_batch(
        self, 
        texts: List[str],
        cache_keys: Optional[List[str]] = None,
        use_cache: bool = True
    ) -> List[List[float]]:
        """
        Get embeddings for multiple texts efficiently.
        Checks cache first, then batches uncached requests.
        
        Args:
            texts: List of texts to embed
            cache_keys: List of cache keys (must match texts length)
            use_cache: Whether to use Redis cache
            
        Returns:
            List of embedding vectors
        """
        if cache_keys and len(cache_keys) != len(texts):
            raise ValueError("cache_keys length must match texts length")
        
        results = [None] * len(texts)
        uncached_indices = []
        uncached_texts = []
        
        # Check cache for each text
        if use_cache and cache_keys and self.redis:
            for i, cache_key in enumerate(cache_keys):
                try:
                    cached = await self.redis.get(f"emb:{cache_key}")
                    if cached:
                        results[i] = json.loads(cached)
                        logger.debug(f"Cache hit for {cache_key}")
                    else:
                        uncached_indices.append(i)
                        uncached_texts.append(texts[i])
                except Exception as e:
                    logger.warning(f"Redis error for {cache_key}: {e}")
                    uncached_indices.append(i)
                    uncached_texts.append(texts[i])
        else:
            uncached_indices = list(range(len(texts)))
            uncached_texts = texts
        
        # Fetch uncached embeddings from OpenAI (batch)
        if uncached_texts:
            logger.info(f"Fetching {len(uncached_texts)} embeddings from OpenAI")
            try:
                response = await self.client.embeddings.create(
                    input=uncached_texts,
                    model=self.MODEL
                )
                
                # Store results
                for i, embedding_obj in enumerate(response.data):
                    original_index = uncached_indices[i]
                    embedding = embedding_obj.embedding
                    results[original_index] = embedding
                    
                    # Cache the result
                    if use_cache and cache_keys and self.redis:
                        try:
                            cache_key = cache_keys[original_index]
                            await self.redis.setex(
                                f"emb:{cache_key}",
                                self.CACHE_TTL,
                                json.dumps(embedding)
                            )
                        except Exception as e:
                            logger.warning(f"Redis set error: {e}")
                            
            except Exception as e:
                logger.error(f"Batch embedding error: {e}")
                raise
        
        return results
    
    def cosine_similarity(
        self, 
        vec1: List[float], 
        vec2: List[float]
    ) -> float:
        """
        Calculate cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
            
        Returns:
            Similarity score between 0 and 1
        """
        vec1_np = np.array(vec1)
        vec2_np = np.array(vec2)
        
        dot_product = np.dot(vec1_np, vec2_np)
        norm1 = np.linalg.norm(vec1_np)
        norm2 = np.linalg.norm(vec2_np)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))
    
    def batch_cosine_similarity(
        self, 
        query_vector: List[float], 
        vectors: List[List[float]]
    ) -> List[float]:
        """
        Calculate cosine similarity between query and multiple vectors efficiently.
        
        Args:
            query_vector: Query embedding
            vectors: List of embeddings to compare against
            
        Returns:
            List of similarity scores
        """
        query_np = np.array(query_vector)
        vectors_np = np.array(vectors)
        
        # Normalize query
        query_norm = query_np / np.linalg.norm(query_np)
        
        # Normalize all vectors
        vectors_norm = vectors_np / np.linalg.norm(vectors_np, axis=1, keepdims=True)
        
        # Compute dot products (cosine similarity for normalized vectors)
        similarities = np.dot(vectors_norm, query_norm)
        
        return similarities.tolist()
    
    async def rank_matches(
        self,
        query_text: str,
        candidate_texts: List[str],
        query_cache_key: Optional[str] = None,
        candidate_cache_keys: Optional[List[str]] = None,
        top_k: Optional[int] = None
    ) -> List[tuple]:
        """
        Rank candidate texts by semantic similarity to query.
        
        Args:
            query_text: Query text (e.g., student profile)
            candidate_texts: Texts to rank (e.g., job descriptions)
            query_cache_key: Cache key for query embedding
            candidate_cache_keys: Cache keys for candidate embeddings
            top_k: Return only top K results (optional)
            
        Returns:
            List of (index, similarity_score) tuples, sorted by score descending
        """
        # Get query embedding
        query_embedding = await self.get_embedding(
            query_text,
            cache_key=query_cache_key
        )
        
        # Get candidate embeddings
        candidate_embeddings = await self.get_embeddings_batch(
            candidate_texts,
            cache_keys=candidate_cache_keys
        )
        
        # Calculate similarities
        similarities = self.batch_cosine_similarity(query_embedding, candidate_embeddings)
        
        # Create (index, score) pairs and sort
        ranked = sorted(
            enumerate(similarities),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Return top K if specified
        if top_k:
            ranked = ranked[:top_k]
        
        logger.info(
            f"Ranked {len(candidate_texts)} candidates. "
            f"Top score: {ranked[0][1]:.3f}, Lowest: {ranked[-1][1]:.3f}"
        )
        
        return ranked
    
    async def clear_cache(self, pattern: str = "emb:*") -> int:
        """
        Clear embeddings cache.
        
        Args:
            pattern: Redis key pattern to delete
            
        Returns:
            Number of keys deleted
        """
        if not self.redis:
            return 0
        
        try:
            keys = []
            async for key in self.redis.scan_iter(match=pattern):
                keys.append(key)
            
            if keys:
                deleted = await self.redis.delete(*keys)
                logger.info(f"Cleared {deleted} cached embeddings")
                return deleted
            return 0
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return 0
    
    async def get_cache_stats(self) -> Dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        if not self.redis:
            return {"enabled": False}
        
        try:
            count = 0
            async for _ in self.redis.scan_iter(match="emb:*"):
                count += 1
            
            return {
                "enabled": True,
                "cached_embeddings": count,
                "ttl_seconds": self.CACHE_TTL
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"enabled": True, "error": str(e)}


# Singleton instance
_openai_service: Optional[OpenAIEmbeddingService] = None


async def get_openai_service(redis_client: Optional[aioredis.Redis] = None) -> OpenAIEmbeddingService:
    """
    Get or create singleton OpenAI service instance.
    Used as dependency in FastAPI endpoints.
    
    Args:
        redis_client: Redis client for caching
        
    Returns:
        OpenAIEmbeddingService instance
    """
    global _openai_service
    if _openai_service is None:
        _openai_service = OpenAIEmbeddingService(redis_client)
    return _openai_service
