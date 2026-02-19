"""Redis cache manager with connection pooling and retry logic."""

import json
import logging
from typing import Any, Optional, Callable, Union, List
from functools import wraps
import redis
from redis.connection import ConnectionPool
from redis.exceptions import RedisError, ConnectionError, TimeoutError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.config import Settings

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Redis cache manager with advanced features:
    - Connection pooling for performance
    - Automatic retry logic with exponential backoff
    - Graceful degradation on failures
    - TTL management
    - Batch operations
    - Key pattern-based operations
    """

    def __init__(self, settings: Settings):
        """Initialize cache manager with settings."""
        self.settings = settings
        self.enabled = settings.CACHE_ENABLED
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._is_connected = False

        logger.info(f"CacheManager initialized. Enabled: {self.enabled}")

    def connect(self) -> None:
        """
        Establish Redis connection with connection pooling.
        Uses retry logic to handle transient connection issues.
        """
        if not self.enabled:
            logger.info("Cache is disabled. Skipping Redis connection.")
            return

        try:
            # Create connection pool for efficient connection reuse
            self._pool = ConnectionPool(
                host=self.settings.REDIS_HOST,
                port=self.settings.REDIS_PORT,
                db=self.settings.REDIS_DB,
                password=self.settings.REDIS_PASSWORD or None,
                max_connections=self.settings.REDIS_MAX_CONNECTIONS,
                socket_keepalive=self.settings.REDIS_SOCKET_KEEPALIVE,
                socket_timeout=self.settings.REDIS_SOCKET_TIMEOUT,
                retry_on_timeout=self.settings.REDIS_RETRY_ON_TIMEOUT,
                health_check_interval=self.settings.REDIS_HEALTH_CHECK_INTERVAL,
                decode_responses=True,  # Automatically decode responses to strings
            )

            # Create Redis client from pool
            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            self._client.ping()
            self._is_connected = True

            logger.info(
                f"Redis cache connected successfully to {self.settings.REDIS_HOST}:{self.settings.REDIS_PORT}"
            )

        except Exception as e:
            logger.error(f"Failed to connect to Redis cache: {e}")
            logger.warning("Cache will operate in degraded mode (no caching)")
            self._is_connected = False
            self.enabled = False

    def disconnect(self) -> None:
        """Close Redis connection and cleanup resources."""
        if self._client:
            try:
                self._client.close()
                logger.info("Redis cache connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

        if self._pool:
            try:
                self._pool.disconnect()
                logger.info("Redis connection pool disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Redis pool: {e}")

        self._is_connected = False

    def is_healthy(self) -> bool:
        """Check if Redis connection is healthy."""
        if not self.enabled or not self._client:
            return False

        try:
            self._client.ping()
            return True
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache with retry logic.

        Args:
            key: Cache key

        Returns:
            Cached value (deserialized from JSON) or None if not found/error
        """
        if not self.enabled or not self._client:
            logger.debug(f"Cache disabled, returning None for key: {key}")
            return None

        try:
            value = self._client.get(key)
            if value:
                logger.debug(f"Cache HIT: {key}")
                return json.loads(value)
            else:
                logger.debug(f"Cache MISS: {key}")
                return None

        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize cached value for key '{key}': {e}")
            # Delete corrupted cache entry
            try:
                self.delete(key)
            except Exception:
                pass  # Ignore delete errors
            return None

        except RedisError as e:
            logger.warning(f"Redis error getting key '{key}': {e}. Continuing without cache.")
            return None

        except Exception as e:
            logger.error(f"Unexpected error getting key '{key}': {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set value in cache with optional TTL.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time-to-live in seconds (None = use default TTL)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self._client:
            logger.debug(f"Cache disabled, skipping set for key: {key}")
            return False

        try:
            # Use default TTL if not specified
            ttl = ttl or self.settings.CACHE_DEFAULT_TTL

            # Serialize value to JSON
            serialized_value = json.dumps(value, default=str)

            # Set with expiration
            result = self._client.setex(key, ttl, serialized_value)
            logger.debug(f"Cache SET: {key} (TTL: {ttl}s)")

            return bool(result)

        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize value for key '{key}': {e}")
            return False

        except RedisError as e:
            logger.warning(f"Redis error setting key '{key}': {e}. Continuing without cache.")
            return False

        except Exception as e:
            logger.error(f"Unexpected error setting key '{key}': {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if deleted, False otherwise
        """
        if not self.enabled or not self._client:
            return False

        try:
            result = self._client.delete(key)
            logger.debug(f"Cache DELETE: {key}")
            return bool(result)

        except RedisError as e:
            logger.error(f"Redis error deleting key '{key}': {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error deleting key '{key}': {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.

        Args:
            pattern: Redis key pattern (e.g., "user:*", "rec:student_123:*")

        Returns:
            Number of keys deleted
        """
        if not self.enabled or not self._client:
            return 0

        try:
            deleted_count = 0
            # Use scan_iter for memory-efficient iteration
            for key in self._client.scan_iter(match=pattern, count=100):
                self._client.delete(key)
                deleted_count += 1

            logger.info(f"Cache DELETE PATTERN: {pattern} ({deleted_count} keys)")
            return deleted_count

        except RedisError as e:
            logger.error(f"Redis error deleting pattern '{pattern}': {e}")
            return 0

        except Exception as e:
            logger.error(f"Unexpected error deleting pattern '{pattern}': {e}")
            return 0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def get_many(self, keys: List[str]) -> dict[str, Any]:
        """
        Get multiple values from cache in a single operation.

        Args:
            keys: List of cache keys

        Returns:
            Dictionary mapping keys to their values (missing keys are omitted)
        """
        if not self.enabled or not self._client or not keys:
            return {}

        try:
            # Use pipeline for batch operation
            pipe = self._client.pipeline()
            for key in keys:
                pipe.get(key)

            values = pipe.execute()

            # Build result dictionary
            result = {}
            for key, value in zip(keys, values):
                if value:
                    try:
                        result[key] = json.loads(value)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to deserialize cached value for key '{key}'")
                        continue

            logger.debug(f"Cache GET_MANY: {len(result)}/{len(keys)} hits")
            return result

        except RedisError as e:
            logger.error(f"Redis error getting multiple keys: {e}")
            return {}

        except Exception as e:
            logger.error(f"Unexpected error getting multiple keys: {e}")
            return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def set_many(
        self,
        mapping: dict[str, Any],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set multiple key-value pairs in a single operation.

        Args:
            mapping: Dictionary of key-value pairs
            ttl: Time-to-live in seconds (applied to all keys)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self._client or not mapping:
            return False

        try:
            ttl = ttl or self.settings.CACHE_DEFAULT_TTL

            # Use pipeline for batch operation
            pipe = self._client.pipeline()
            for key, value in mapping.items():
                serialized_value = json.dumps(value, default=str)
                pipe.setex(key, ttl, serialized_value)

            pipe.execute()

            logger.debug(f"Cache SET_MANY: {len(mapping)} keys (TTL: {ttl}s)")
            return True

        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize values for batch set: {e}")
            return False

        except RedisError as e:
            logger.error(f"Redis error setting multiple keys: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error setting multiple keys: {e}")
            return False

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: Optional[int] = None,
    ) -> Optional[Any]:
        """
        Get value from cache or compute and cache it if not present.

        Args:
            key: Cache key
            factory: Callable that computes the value if cache miss
            ttl: Time-to-live in seconds

        Returns:
            Cached or computed value
        """
        # Try to get from cache
        cached_value = self.get(key)
        if cached_value is not None:
            return cached_value

        # Cache miss - compute value
        try:
            value = factory()
            if value is not None:
                self.set(key, value, ttl)
            return value

        except Exception as e:
            logger.error(f"Error computing value for key '{key}': {e}")
            return None

    def clear_all(self) -> bool:
        """
        Clear all keys in the current database.
        Use with caution in production!
        """
        if not self.enabled or not self._client:
            return False

        try:
            self._client.flushdb()
            logger.warning("Cache cleared (FLUSHDB executed)")
            return True

        except RedisError as e:
            logger.error(f"Redis error clearing cache: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error clearing cache: {e}")
            return False

    def get_stats(self) -> dict:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        if not self.enabled or not self._client:
            return {
                "enabled": False,
                "connected": False,
            }

        try:
            info = self._client.info("stats")
            keyspace = self._client.info("keyspace")

            db_info = keyspace.get(f"db{self.settings.REDIS_DB}", {})

            return {
                "enabled": True,
                "connected": self._is_connected,
                "keys": db_info.get("keys", 0),
                "expires": db_info.get("expires", 0),
                "total_connections_received": info.get("total_connections_received", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": (
                    info.get("keyspace_hits", 0)
                    / (info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0))
                    if (info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0)) > 0
                    else 0
                ),
            }

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {
                "enabled": True,
                "connected": False,
                "error": str(e),
            }


def cached(
    key_prefix: str,
    ttl: Optional[int] = None,
    key_builder: Optional[Callable[..., str]] = None,
):
    """
    Decorator to cache function results.

    Args:
        key_prefix: Prefix for cache key
        ttl: Time-to-live in seconds
        key_builder: Optional function to build cache key from function args

    Usage:
        @cached(key_prefix="user", ttl=3600)
        def get_user(user_id: int):
            return fetch_user_from_db(user_id)

        # Or with custom key builder:
        @cached(
            key_prefix="search",
            ttl=1800,
            key_builder=lambda query, filters: f"{query}:{json.dumps(filters)}"
        )
        def search(query: str, filters: dict):
            return perform_search(query, filters)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Build cache key
            if key_builder:
                key_suffix = key_builder(*args, **kwargs)
            else:
                # Default: use function args as key
                key_suffix = f"{args}:{kwargs}"

            cache_key = f"{key_prefix}:{key_suffix}"

            # Get cache manager from function's module or use global instance
            from app.main import cache_manager

            # Try to get from cache
            cached_value = cache_manager.get(cache_key)
            if cached_value is not None:
                return cached_value

            # Cache miss - execute function
            result = func(*args, **kwargs)

            # Cache result
            if result is not None:
                cache_manager.set(cache_key, result, ttl)

            return result

        return wrapper

    return decorator


# Singleton instance (initialized by main.py)
_cache_manager_instance: Optional[CacheManager] = None


def get_cache_manager() -> Optional[CacheManager]:
    """Get the global cache manager instance."""
    return _cache_manager_instance


def set_cache_manager(manager: CacheManager) -> None:
    """Set the global cache manager instance."""
    global _cache_manager_instance
    _cache_manager_instance = manager
