# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\exchange_source\config.py
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import SettingsConfigDict # Import ConfigDict from pydantic_settings if using BaseSettings, or from pydantic otherwise
from typing import Optional

class CCXTConfig(BaseModel):
    """Configuration specific to the CCXT library and exchanges."""
    default_exchange: Optional[str] = Field(None, description="Default exchange ID to use if not specified")
    enable_rate_limit: bool = Field(True, description="Enable CCXT's built-in rate limiter")
    timeout: int = Field(30000, description="Request timeout in milliseconds") # Default 30 seconds

    # Example: Add specific API key fields if needed, though often handled dynamically
    # cryptocom_api_key: Optional[SecretStr] = Field(None, alias="CCXT_CRYPTOCOM_API_KEY")
    # cryptocom_secret: Optional[SecretStr] = Field(None, alias="CCXT_CRYPTOCOM_SECRET")

    # Use model_config with ConfigDict instead of nested class Config
    model_config = SettingsConfigDict(
        env_prefix = "CCXT_" # Prefix for environment variables
        # Consider case_sensitive = False if env vars might have different casing
    )
