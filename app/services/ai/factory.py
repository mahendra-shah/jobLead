"""
AI Provider Factory
Centralized access to AI providers with fallback support
"""
import logging
from typing import Optional

from app.config import settings
from .base import AIProvider
from .openai_service import OpenAIService
from .gemini_service import GeminiService
from .openrouter_service import OpenRouterService

logger = logging.getLogger(__name__)


class AIFactory:
    """Factory to get AI provider based on configuration"""
    
    _providers = {
        'openai': OpenAIService,
        'gemini': GeminiService,
        'openrouter': OpenRouterService,
    }
    
    _instances = {}  # Singleton instances
    
    @classmethod
    def get_provider(cls, provider_name: Optional[str] = None) -> AIProvider:
        """
        Get AI provider instance
        
        Args:
            provider_name: Provider name ('openai', 'gemini', 'openrouter')
                          If None, uses settings.AI_PROVIDER
        
        Returns:
            AIProvider instance
        
        Raises:
            ValueError: If provider not found or API key missing
        """
        if provider_name is None:
            provider_name = getattr(settings, 'AI_PROVIDER', 'openai')
        
        # Check if already instantiated (singleton)
        if provider_name in cls._instances:
            return cls._instances[provider_name]
        
        # Get provider class
        provider_class = cls._providers.get(provider_name)
        if not provider_class:
            available = ', '.join(cls._providers.keys())
            raise ValueError(
                f"Unknown AI provider: {provider_name}. "
                f"Available providers: {available}"
            )
        
        # Validate API key exists
        cls._validate_api_key(provider_name)
        
        # Instantiate and cache
        try:
            instance = provider_class()
            cls._instances[provider_name] = instance
            logger.info(f"Initialized AI provider: {provider_name}")
            return instance
        except Exception as e:
            logger.error(f"Failed to initialize {provider_name}: {e}")
            raise
    
    @classmethod
    def get_provider_with_fallback(cls, primary: Optional[str] = None) -> AIProvider:
        """
        Get AI provider with automatic fallback
        
        Args:
            primary: Primary provider name
        
        Returns:
            AIProvider instance (primary or fallback)
        """
        if primary is None:
            primary = getattr(settings, 'AI_PROVIDER', 'openai')
        
        fallback = getattr(settings, 'AI_FALLBACK_PROVIDER', 'gemini')
        
        try:
            return cls.get_provider(primary)
        except Exception as e:
            logger.warning(f"Primary provider {primary} failed: {e}, using fallback {fallback}")
            return cls.get_provider(fallback)
    
    @classmethod
    def _validate_api_key(cls, provider_name: str):
        """Validate that API key is configured"""
        key_mapping = {
            'openai': 'OPENAI_API_KEY',
            'gemini': 'GEMINI_API_KEY',
            'openrouter': 'OPENROUTER_API_KEY',
        }
        
        key_name = key_mapping.get(provider_name)
        if not key_name:
            return  # No key required
        
        api_key = getattr(settings, key_name, None)
        if not api_key:
            raise ValueError(
                f"{provider_name} requires {key_name} to be set in environment variables"
            )
    
    @classmethod
    def list_providers(cls) -> list:
        """List all available providers"""
        return list(cls._providers.keys())


# Convenience function
def get_ai_provider(provider_name: Optional[str] = None) -> AIProvider:
    """Get AI provider instance (convenience function)"""
    return AIFactory.get_provider_with_fallback(provider_name)
