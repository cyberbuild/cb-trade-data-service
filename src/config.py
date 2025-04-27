import logging
import sys
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field
from typing import Literal, Dict, Optional

# Basic logging configuration (can be refined later)
logging.basicConfig(
    level=logging.INFO, # Or load from config settings.log_level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
        # Add FileHandler etc. based on config
    ]
)

class StorageConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='STORAGE_')

    type: Literal['local', 'azure'] = 'local'
    local_root_path: str = './data' # Default for local storage
    azure_connection_string: Optional[SecretStr] = None
    azure_container_name: str = 'rawdata'

class CCXTConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='CCXT_')

    default_exchange: str = 'crypto_com' # Example default
    # Add fields for API keys/secrets if needed by specific exchanges
    # crypto_com_api_key: Optional[SecretStr] = None
    # crypto_com_secret: Optional[SecretStr] = None

# Add other config models as needed (e.g., AppConfig for log level)

class Settings(BaseSettings):
    # Top-level settings object to hold nested configs
    storage: StorageConfig = Field(default_factory=StorageConfig)
    ccxt: CCXTConfig = Field(default_factory=CCXTConfig)
    # Add other nested configs

    # Pydantic-settings automatically loads .env files by default
    # model_config = SettingsConfigDict(env_file='.env', extra='ignore') # Explicitly load if needed

# Instantiate settings once
settings = Settings()

# Optional: Function to get settings easily
def get_settings() -> Settings:
    return settings
