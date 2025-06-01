"""
Configuration factory for creating storage backends based on settings.
"""
import logging
from typing import Union
from storage.backends.istorage_backend import IStorageBackend
from storage.backends.local_file_backend import LocalFileBackend
from storage.backends.azure_blob_backend import AzureBlobBackend
from storage.storage_settings import LocalStorageSettings, AzureStorageSettings

logger = logging.getLogger(__name__)


def create_storage_backend(
    storage_config: Union[LocalStorageSettings, AzureStorageSettings]
) -> IStorageBackend:
    """
    Create the appropriate storage backend based on configuration.
    
    Args:
        storage_config: Configuration object for the storage backend
        
    Returns:
        Configured storage backend instance
        
    Raises:
        ValueError: If configuration is invalid or unsupported
    """
    if isinstance(storage_config, LocalStorageSettings):
        logger.info(f"Creating LocalFileBackend with root path: {storage_config.root_path}")
        return LocalFileBackend(root_path=storage_config.root_path)
    
    elif isinstance(storage_config, AzureStorageSettings):
        if storage_config.use_managed_identity and storage_config.account_name:
            logger.info(f"Creating AzureBlobBackend with managed identity for account: {storage_config.account_name}")
            return AzureBlobBackend(
                account_name=storage_config.account_name,
                container_name=storage_config.container_name,
                use_managed_identity=True
            )
        elif storage_config.connection_string:
            connection_str = storage_config.connection_string.get_secret_value()
            logger.info(f"Creating AzureBlobBackend with connection string for container: {storage_config.container_name}")
            return AzureBlobBackend(
                connection_string=connection_str,
                container_name=storage_config.container_name
            )
        else:
            raise ValueError(
                "Azure storage configuration requires either connection_string or "
                "(account_name + use_managed_identity=True)"
            )
    
    else:
        raise ValueError(f"Unsupported storage configuration type: {type(storage_config)}")
