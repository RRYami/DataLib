import os

from config import settings
from logger.logger import get_logger

logger = get_logger(__name__)


def get_api_key(key_name: str = "API_KEY") -> str:
    """
    Retrieve API key from environment variables or the loaded settings.

    Args:
        key_name: Name of the environment variable containing the API key

    Returns:
        API key string

    Raises:
        ValueError: If API key is not found in environment or settings
    """
    api_key = os.getenv(key_name)
    if api_key is None:
        # Fallback to pydantic-settings (reads from .env)
        api_key = getattr(settings, key_name.lower(), None)
    if api_key is None:
        logger.warning(f"{key_name} not found in environment variables")
        raise ValueError(f"{key_name} not found in environment variables")
    return api_key
