import os

from dotenv import load_dotenv

from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def get_api_key(key_name: str = "API_KEY") -> str:
    """
    Retrieve API key from environment variables.

    Args:
        key_name: Name of the environment variable containing the API key

    Returns:
        API key string

    Raises:
        ValueError: If API key is not found in environment
    """
    try:
        load_dotenv(".env")
        api_key = os.getenv(key_name)
        if api_key is None:
            logger.warning(f"{key_name} not found in environment variables")
            raise ValueError(f"{key_name} not found in environment variables")
    except Exception as e:
        logger.error(f"Error retrieving API key: {e}")
        raise
    return api_key
