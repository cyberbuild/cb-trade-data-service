import logging
import sys
from functools import lru_cache # Add this import
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field
from typing import Literal, Dict, Optional

# Import the new storage config structure
from storage.config import StorageConfig
# Import the new exchange config structure
from exchange_source.config import CCXTConfig

# Basic logging configuration (can be refined later)
logging.basicConfig(
    level=logging.INFO, # Or load from config settings.log_level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
        # Add FileHandler etc. based on config
    ]
)

# Add other config models as needed (e.g., AppConfig for log level)

class Settings(BaseSettings):
    # Top-level settings object to hold nested configs
    # Use the imported StorageConfig discriminated union
    # Pydantic-settings will look for STORAGE_TYPE and load accordingly
    storage: StorageConfig
    # Use the imported CCXTConfig
    ccxt: CCXTConfig = Field(default_factory=CCXTConfig)
    # Add other nested configs

    # Pydantic-settings automatically loads .env files by default
    # Configure loading behavior
    model_config = SettingsConfigDict(
        env_file='.env', # Load .env file
        extra='ignore',  # Ignore extra fields not defined in the models
        env_nested_delimiter='__' # Use double underscore for nested env vars e.g. STORAGE__AZURE_CONTAINER_NAME
    )

# Optional: Function to get settings easily
@lru_cache()
def get_settings() -> Settings:
    # Instantiate settings here, ensuring .env is loaded
    return Settings()

# Example of accessing specific config based on type after loading
# try:
#     storage_settings = get_settings().storage
#     if isinstance(storage_settings, LocalStorageSettings):
#         logging.info(f"Using local storage at: {storage_settings.root_path}")
#     elif isinstance(storage_settings, AzureStorageSettings):
#         logging.info(f"Using Azure storage container: {storage_settings.container_name}")
# except Exception as e:
#     logging.error(f"Failed to load or validate settings: {e}")
#     # Handle error appropriately, maybe exit
#     sys.exit(1)
