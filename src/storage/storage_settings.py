# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\config.py
import os
import logging
from pydantic import Field, SecretStr, model_validator, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal, Union, Annotated, Any, Dict, List # Add required types

logger = logging.getLogger(__name__)


# Define settings for Local Storage
class LocalStorageSettings(BaseSettings):
    # Allow ignoring extra fields from environment for discriminated union
    model_config = SettingsConfigDict(extra='ignore', env_override_priority=2)
    # No prefix needed if loaded as part of a parent model with prefix
    # model_config = SettingsConfigDict(env_prefix='STORAGE_')
    type: Literal['local'] = 'local'
    # Use validation_alias for explicit env var mapping if needed outside nested loading
    root_path: str

    @model_validator(mode='before')
    @classmethod
    def resolve_local_root_path(cls, data: Any) -> Any:
        if isinstance(data, dict):
            path = data.get('root_path', './data')
            # Use the path as provided: absolute or relative
            data['root_path'] = path
            try:
                os.makedirs(path, exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create local storage directory {path}: {e}")
        return data

# Define settings for Azure Storage


class AzureStorageSettings(BaseSettings):
    # Allow ignoring extra fields from environment for discriminated union
    model_config = SettingsConfigDict(extra='ignore')
    type: Literal['azure'] = 'azure'
    # Use validation_alias for explicit env var mapping
    # The '...' indicates it's required
    connection_string: SecretStr = Field(..., validation_alias='STORAGE_AZURE_CONNECTION_STRING')
    container_name: str = Field(default='rawdata', validation_alias='STORAGE_AZURE_CONTAINER_NAME')


# Create a Discriminated Union using Annotated and Field
# This tells Pydantic to use the 'type' field to determine which model to use
StorageConfig = Annotated[
    Union[LocalStorageSettings, AzureStorageSettings],
    Field(discriminator="type")
]


# Storage settings for operations
class StorageSettings(BaseModel):
    """Settings for storage operations."""
    context: Dict[str, Any]
    partition_cols: List[str]
    format_hint: str = "delta"
    mode: str = "overwrite"
    timestamp_col: str = "timestamp"


# Helper function to get the specific storage config instance
# This might be useful if the main Settings object holds the Union directly
def get_storage_backend_config(settings: BaseSettings) -> Union[LocalStorageSettings, AzureStorageSettings]:
    """Extracts the specific storage config model from the main settings."""
    # Assuming the storage config is nested under a 'storage' field in the main Settings
    if hasattr(settings, 'storage') and isinstance(settings.storage, (LocalStorageSettings, AzureStorageSettings)):
        return settings.storage
    raise TypeError("Main settings object does not contain a valid StorageConfig instance.")
