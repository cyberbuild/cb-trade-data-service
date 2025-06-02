import logging
import sys
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os

# Import the new storage config structure
from storage.storage_settings import StorageConfig

# Import the new exchange config structure
from exchange_source.config import CCXTConfig

# Basic logging configuration (can be refined later)
logging.basicConfig(
    level=logging.INFO,  # Or load from config settings.log_level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
        # Add FileHandler etc. based on config
    ],
)

logger = logging.getLogger(__name__)

# Add other config models as needed (e.g., AppConfig for log level)


class Settings(BaseSettings):
    # Top-level settings object to hold nested configs
    # Use a custom factory to determine which storage config to use
    storage: StorageConfig = Field(default_factory=lambda: _create_storage_config())
    # Use the imported CCXTConfig
    ccxt: CCXTConfig = Field(default_factory=CCXTConfig)
    # Exchange ID for CCXT client
    exchange_id: str = Field(
        description="Exchange ID to use with CCXT client",
        alias="CCXT_DEFAULT_EXCHANGE"
    )    # Add other nested configs
    # Pydantic-settings automatically loads .env files by default
    # Configure loading behavior
    model_config = SettingsConfigDict(
        extra="ignore",  # Ignore extra fields not defined in the models
        populate_by_name=True,  # Allow populating by field name or alias
    )


def _create_storage_config() -> StorageConfig:
    """Create the appropriate storage config based on available environment variables."""
    logger.info("Default factory for storage invoked.")
    
    # The new StorageConfig class handles both local and Azure automatically
    # based on available environment variables
    try:
        return StorageConfig()
    except Exception as e:
        logger.error(f"Failed to create storage config: {e}")
        raise


# Optional: Function to get settings easily
@lru_cache()
def get_settings(env_file: str = ".env") -> Settings:
    logger.info(f"Attempting to load Settings with env_file: {env_file}")
    try:
        # Create storage config with the specified env_file
        storage_config = StorageConfig(_env_file=env_file)
        
        # Create settings with the storage config
        settings = Settings(storage=storage_config, _env_file=env_file)
        logger.info(
            f"Successfully loaded Settings."
        )
       
        logger.info(f"Exchange ID: {settings.exchange_id}")

        return settings
    except Exception as e:
        logger.error(f"Error loading Settings with env_file {env_file}: {e}", exc_info=True)
        raise


def get_storage_backend():
    """
    Create and return the appropriate storage backend based on current settings.

    Returns:
        Configured storage backend instance
    """
    from storage.backend_factory import create_storage_backend

    settings = get_settings()
    return create_storage_backend(settings.storage)


# Example of accessing specific config based on type after loading
# try:
#     storage_settings = get_settings().storage
#     if storage_settings.is_local_storage():
#         logging.info(f"Using local storage at: {storage_settings.root_path}")
#     elif storage_settings.is_azure_storage():
#         logging.info(
#             f"Using Azure storage container: {storage_settings.container_name}"
#         )
# except Exception as e:
#     logging.error(f"Failed to load or validate settings: {e}")
#     sys.exit(1)
