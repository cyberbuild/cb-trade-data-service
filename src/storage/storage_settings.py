# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\config.py
import logging
from pydantic import Field, SecretStr, model_validator, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import (
    Literal,
    Union,
    Annotated,
    Any,
    Dict,
    List,
    Optional,
)  # Add required types

logger = logging.getLogger(__name__)


# Define settings for Local Storage
class LocalStorageSettings(BaseSettings):
    # Allow ignoring extra fields from environment for discriminated union
    model_config = SettingsConfigDict(
        extra="ignore"
    )
    root_path: str = Field(validation_alias="STORAGE_ROOT_PATH")

    @model_validator(mode="before")
    @classmethod
    def resolve_local_root_path(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Check for both the field name and the environment variable name
            path = data.get("root_path") or data.get("STORAGE_ROOT_PATH")
            if path is None:
                raise ValueError("root_path is required for local storage")
            # Ensure the field name is set correctly for pydantic
            data["root_path"] = path
            # Don't create directories during settings validation - let the backend handle it
            # This avoids permission issues during test configuration
        return data


# Define settings for Azure Storage


class AzureStorageSettings(BaseSettings):
    # Allow ignoring extra fields from environment for discriminated union
    model_config = SettingsConfigDict(extra="ignore")
    
    # Authentication options - either connection string OR managed identity
    connection_string: Optional[SecretStr] = Field(
        default=None, validation_alias="STORAGE_AZURE_CONNECTION_STRING"
    )
    account_name: Optional[str] = Field(
        default=None, validation_alias="STORAGE_AZURE_ACCOUNT_NAME"
    )
    subscription_id: Optional[str] = Field(
        default=None, validation_alias="AZURE_SUBSCRIPTION_ID"
    )
    client_id: Optional[str] = Field(
        default=None, validation_alias="AZURE_CLIENT_ID"
    )
    client_secret: Optional[SecretStr] = Field(
        default=None, validation_alias="AZURE_CLIENT_SECRET"
    )
    tenant_id: Optional[str] = Field(
        default=None, validation_alias="AZURE_TENANT_ID"
    )
    use_managed_identity: bool = Field(
        default=False, validation_alias="STORAGE_AZURE_USE_MANAGED_IDENTITY"
    )

    container_name: str = Field(
        default="rawdata", validation_alias="STORAGE_AZURE_CONTAINER_NAME"
    )

    def model_post_init(self, __context) -> None:
        """Validate that either connection_string or (account_name + use_managed_identity) is provided."""
        if not self.connection_string and not (
            self.account_name and self.use_managed_identity
        ):
            raise ValueError(
                "Either STORAGE_AZURE_CONNECTION_STRING or "
                "(STORAGE_AZURE_ACCOUNT_NAME + STORAGE_AZURE_USE_MANAGED_IDENTITY=true) must be provided."
            )


# Create a configuration class that can handle both local and Azure storage
class StorageConfig(BaseSettings):
    """
    Unified storage configuration that supports both local and Azure storage
    based on available environment variables.
    """
    model_config = SettingsConfigDict(extra="ignore")
    
    # Local storage fields
    root_path: Optional[str] = Field(default=None, validation_alias="STORAGE_ROOT_PATH")
    
    # Azure storage fields
    connection_string: Optional[SecretStr] = Field(
        default=None, validation_alias="STORAGE_AZURE_CONNECTION_STRING"
    )
    account_name: Optional[str] = Field(
        default=None, validation_alias="STORAGE_AZURE_ACCOUNT_NAME"
    )
    subscription_id: Optional[str] = Field(
        default=None, validation_alias="AZURE_SUBSCRIPTION_ID"
    )
    client_id: Optional[str] = Field(
        default=None, validation_alias="AZURE_CLIENT_ID"
    )
    client_secret: Optional[SecretStr] = Field(
        default=None, validation_alias="AZURE_CLIENT_SECRET"
    )
    tenant_id: Optional[str] = Field(
        default=None, validation_alias="AZURE_TENANT_ID"    )
    use_managed_identity: bool = Field(
        default=False, validation_alias="STORAGE_AZURE_USE_MANAGED_IDENTITY"
    )
    container_name: Optional[str] = Field(
        default="rawdata", validation_alias="STORAGE_AZURE_CONTAINER_NAME"
    )

    def is_local_storage(self) -> bool:
        """Check if this configuration is for local storage."""
        return bool(self.root_path and not self.connection_string and not (self.account_name and self.use_managed_identity))
    
    def is_azure_storage(self) -> bool:
        """Check if this configuration is for Azure storage."""
        return bool(self.connection_string or (self.account_name and self.use_managed_identity))


# Storage settings for operations
class StorageSettings(BaseModel):
    """Settings for storage operations."""

    context: Dict[str, Any]
    partition_cols: List[str]
    format_hint: str = "delta"
    mode: str = "overwrite"
    timestamp_col: str = "timestamp"


# Helper function to get the specific storage config instance
def get_storage_backend_config(
    settings: BaseSettings,
) -> StorageConfig:
    """Extracts the specific storage config model from the main settings."""
    # Assuming the storage config is nested under a 'storage' field in the main Settings
    if hasattr(settings, "storage") and isinstance(settings.storage, StorageConfig):
        return settings.storage
    raise TypeError(
        "Main settings object does not contain a valid StorageConfig instance."
    )
