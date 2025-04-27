# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\config.py
import os
import logging
from pydantic import Field, SecretStr, model_validator, ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal, Union, Optional, Annotated, Any # Add Any

logger = logging.getLogger(__name__)

# Define settings for Local Storage
class LocalStorageSettings(BaseSettings):
    # No prefix needed if loaded as part of a parent model with prefix
    # model_config = SettingsConfigDict(env_prefix='STORAGE_')
    type: Literal['local'] = 'local'
    # Use validation_alias for explicit env var mapping if needed outside nested loading
    root_path: str = Field(default='./data', validation_alias='STORAGE_LOCAL_ROOT_PATH')

    @model_validator(mode='before')
    @classmethod
    def resolve_local_root_path(cls, data: Any) -> Any:
        if isinstance(data, dict):
            path = data.get('root_path', './data')
            if not os.path.isabs(path):
                try:
                    # Try resolving relative to project root (assuming src is one level down)
                    project_root = Path(__file__).resolve().parents[2] # src -> storage -> config.py
                    abs_path = (project_root / path).resolve()
                    data['root_path'] = str(abs_path)
                    logger.debug(f"Resolved local storage root path to: {abs_path}")
                except Exception as e:
                    logger.warning(f"Could not resolve project root relative path for local storage: {path}. Using as is. Error: {e}")
                    # Fallback to resolving relative to current working directory
                    data['root_path'] = os.path.abspath(path)
            # Ensure directory exists
            try:
                os.makedirs(data['root_path'], exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create local storage directory {data['root_path']}: {e}")
                # Decide if this should raise an error or just log
                # raise
        return data

# Define settings for Azure Storage
class AzureStorageSettings(BaseSettings):
    # model_config = SettingsConfigDict(env_prefix='STORAGE_')
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

# Helper function to get the specific storage config instance
# This might be useful if the main Settings object holds the Union directly
def get_storage_backend_config(settings: BaseSettings) -> Union[LocalStorageSettings, AzureStorageSettings]:
    """Extracts the specific storage config model from the main settings."""
    # Assuming the storage config is nested under a 'storage' field in the main Settings
    if hasattr(settings, 'storage') and isinstance(settings.storage, (LocalStorageSettings, AzureStorageSettings)):
        return settings.storage
    raise TypeError("Main settings object does not contain a valid StorageConfig instance.")
