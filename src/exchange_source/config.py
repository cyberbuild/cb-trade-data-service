# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\exchange_source\config.py
from pydantic import BaseModel, Field
from pydantic_settings import (
    SettingsConfigDict,
)


class CCXTConfig(BaseModel):
    """Configuration specific to the CCXT library and exchanges."""

    enable_rate_limit: bool = Field(
        True, description="Enable CCXT's built-in rate limiter"
    )
    timeout: int = Field(
        30000, description="Request timeout in milliseconds"
    )  # Default 30 seconds

    model_config = SettingsConfigDict(
        env_prefix="CCXT_",
    )
