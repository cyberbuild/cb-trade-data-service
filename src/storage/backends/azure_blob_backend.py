# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\backends\azure_blob_backend.py
import logging
from typing import List, Dict, Any, Optional
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, StorageStreamDownloader
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from pathlib import Path
from .istorage_backend import IStorageBackend

logger = logging.getLogger(__name__)

class AzureBlobBackend(IStorageBackend):
    """Implements IStorageBackend for Azure Blob Storage (compatible with ADLS Gen2)."""

    def __init__(self, connection_string: str, container_name: str):
        if not connection_string:
            raise ValueError("Azure connection string is required.")
        if not container_name:
            raise ValueError("Azure container name is required.")

        self.connection_string = connection_string
        self.container_name = container_name
        self._service_client: Optional[BlobServiceClient] = None
        self._container_client: Optional[ContainerClient] = None
        logger.info(f"Initialized AzureBlobBackend for container: {container_name}")

    async def _get_container_client(self) -> ContainerClient:
        """Initializes and returns the ContainerClient, creating container if needed."""
        if self._container_client is None:
            try:
                self._service_client = BlobServiceClient.from_connection_string(self.connection_string)
                self._container_client = self._service_client.get_container_client(self.container_name)
                # Check if container exists, create if not
                try:
                    await self._container_client.get_container_properties() # Check existence
                    logger.info(f"Connected to existing Azure container: {self.container_name}")
                except ResourceNotFoundError:
                    logger.warning(f"Azure container '{self.container_name}' not found, creating...")
                    await self._service_client.create_container(self.container_name)
                    self._container_client = self._service_client.get_container_client(self.container_name)
                    logger.info(f"Created and connected to Azure container: {self.container_name}")
            except Exception as e:
                logger.error(f"Failed to initialize Azure Blob Storage client for container {self.container_name}: {e}")
                # Reset clients to allow retry on next call
                self._container_client = None
                self._service_client = None
                raise
        return self._container_client

    def get_uri_for_identifier(self, identifier: str) -> str:
        """Returns an az:// URI for the identifier (suitable for Delta Lake on Azure Blob)."""
        # Construct the az:// URI format expected by deltalake-python
        # az://<container_name>/<path>
        # The storage options will handle authentication.
        return f"az://{self.container_name}/{identifier}"

    async def get_storage_options(self) -> Dict[str, str]:
        """Returns storage options for libraries like Delta Lake, PyArrow.
           Requires parsing the connection string, which can be complex.
           Alternatively, pass individual components (account_name, key/sas) during init.
        """
        # Basic parsing, might not cover all auth methods (SAS, Identity)
        parts = {p.split('=', 1)[0].lower(): p.split('=', 1)[1] for p in self.connection_string.split(';') if '=' in p}
        options = {}
        account_name = parts.get('accountname')
        account_key = parts.get('accountkey')
        sas_token = parts.get('sharedaccesssignature')

        if account_name:
            options['account_name'] = account_name # For fsspec/pyarrow
            options['storage_account'] = account_name # Common alternative name

        if account_key:
            options['account_key'] = account_key # For fsspec/pyarrow
            options['storage_access_key'] = account_key # For deltalake-python < 0.9
            options['azure_storage_access_key'] = account_key # For deltalake-python >= 0.9
        elif sas_token:
            options['sas_token'] = sas_token
            options['azure_storage_sas_token'] = sas_token # For deltalake-python
        # else: Assume managed identity or other ambient auth

        # Add endpoint if specified (e.g., for Azurite or regional endpoints)
        endpoint = parts.get('blobendpoint')
        if endpoint:
             options['endpoint_url'] = endpoint
             options['azure_storage_endpoint_url'] = endpoint # For deltalake-python

        # For Delta Lake specifically, it often uses 'AZURE_STORAGE_ACCOUNT_NAME', 'AZURE_STORAGE_ACCESS_KEY', etc.
        # or connection string directly via 'AZURE_STORAGE_CONNECTION_STRING' env var.
        # Providing options dict is mainly for pyarrow/fsspec integration.
        # Delta Lake Rust core might pick up env vars automatically.
        # Let's ensure keys used by deltalake-python are present if possible.
        if not options:
            logger.warning("Could not parse account details from connection string for storage_options. Relying on ambient auth or env vars.")
            return {"anon": True} # Indicate anonymous access might be attempted by pyarrow/fsspec

        logger.debug(f"Generated storage options: { {k: '***' if 'key' in k or 'token' in k else v for k, v in options.items()} }")
        return options

    async def save_bytes(self, identifier: str, data: bytes):
        """Uploads bytes to an Azure blob asynchronously."""
        container_client = await self._get_container_client()
        blob_client = container_client.get_blob_client(identifier)
        try:
            await blob_client.upload_blob(data, overwrite=True)
            logger.debug(f"Saved {len(data)} bytes to Azure blob: {self.container_name}/{identifier}")
        except Exception as e:
            logger.error(f"Error saving bytes to Azure blob {self.container_name}/{identifier}: {e}")
            raise
        finally:
            # Ensure clients are closed if they were created here (though typically managed by __aexit__)
            # await blob_client.close() # Closed automatically by context manager if used
            pass

    async def load_bytes(self, identifier: str) -> bytes:
        """Downloads bytes from an Azure blob asynchronously."""
        container_client = await self._get_container_client()
        blob_client = container_client.get_blob_client(identifier)
        try:
            downloader: StorageStreamDownloader = await blob_client.download_blob()
            data = await downloader.readall()
            logger.debug(f"Loaded {len(data)} bytes from Azure blob: {self.container_name}/{identifier}")
            return data
        except ResourceNotFoundError:
            logger.warning(f"Azure blob not found: {self.container_name}/{identifier}")
            raise FileNotFoundError(f"Blob not found: {identifier}")
        except Exception as e:
            logger.error(f"Error loading bytes from Azure blob {self.container_name}/{identifier}: {e}")
            raise
        finally:
            # await blob_client.close()
            pass

    async def list_items(self, prefix: str = "") -> List[str]:
        """Lists blobs matching a given prefix asynchronously."""
        container_client = await self._get_container_client()
        items = []
        try:
            async for blob in container_client.list_blobs(name_starts_with=prefix):
                items.append(blob.name)
            logger.debug(f"Listed {len(items)} blobs under prefix '{prefix}' in container {self.container_name}")
        except Exception as e:
            logger.error(f"Error listing blobs under prefix '{prefix}' in container {self.container_name}: {e}")
            raise
        return items

    async def exists(self, identifier: str) -> bool:
        """Checks if a blob exists asynchronously."""
        container_client = await self._get_container_client()
        blob_client = container_client.get_blob_client(identifier)
        try:
            exists = await blob_client.exists()
            logger.debug(f"Checked existence for blob {self.container_name}/{identifier}: {exists}")
            return exists
        except Exception as e:
            # Handle potential auth errors differently? For now, log and re-raise.
            logger.error(f"Error checking existence for blob {self.container_name}/{identifier}: {e}")
            raise
        finally:
            # await blob_client.close()
            pass

    async def delete(self, identifier: str):
        """Deletes a blob asynchronously."""
        container_client = await self._get_container_client()
        blob_client = container_client.get_blob_client(identifier)
        try:
            await blob_client.delete_blob(delete_snapshots="include")
            logger.info(f"Deleted blob: {self.container_name}/{identifier}")
        except ResourceNotFoundError:
            logger.warning(f"Attempted to delete non-existent blob: {self.container_name}/{identifier}")
            # Comply with interface expectation: don't raise error if not found
        except Exception as e:
            logger.error(f"Error deleting blob {self.container_name}/{identifier}: {e}")
            raise
        finally:
            # await blob_client.close()
            pass

    async def makedirs(self, identifier: str, exist_ok: bool = True):
        """Ensure that the directory structure for the identifier exists.
           In Blob storage, directories are virtual. Creating an empty blob
           can sometimes act as a placeholder if needed, especially for tools
           expecting directory markers.
           For Delta Lake, it manages its own structure.
           This implementation is often a no-op or creates a zero-byte blob
           at the 'directory' path if necessary.
        """
        # Azure Blob Storage doesn't have explicit directories like filesystems.
        # Paths are just part of the blob name.
        # However, some tools might rely on seeing a 'directory marker'.
        # We can create a zero-byte blob at the directory path if needed.
        # Let's assume for now it's not strictly required unless identifier ends with '/'.

        if identifier.endswith('/'):
            dir_identifier = identifier
        elif '.' in Path(identifier).name: # Heuristic: looks like a file path
             # We want the directory *containing* the identifier
             dir_identifier = str(Path(identifier).parent)
             if dir_identifier == '.': # Root level
                 return # No directory to create for root
             dir_identifier += '/' # Ensure trailing slash for directory marker
        else: # Assume it's already a directory path
            dir_identifier = identifier
            if not dir_identifier.endswith('/'):
                dir_identifier += '/'

        if not dir_identifier or dir_identifier == '/':
             return # Nothing to create for root

        # Optional: Create a zero-byte blob as a directory marker
        # This is often unnecessary but can help with some tools/listings.
        # container_client = await self._get_container_client()
        # blob_client = container_client.get_blob_client(dir_identifier)
        # try:
        #     await blob_client.upload_blob(b'', overwrite=exist_ok) # Create empty blob
        #     logger.debug(f"Ensured directory marker exists (by creating empty blob): {self.container_name}/{dir_identifier}")
        # except ResourceExistsError:
        #     if not exist_ok:
        #         logger.error(f"Directory marker blob already exists and exist_ok=False: {self.container_name}/{dir_identifier}")
        #         raise
        #     else:
        #         logger.debug(f"Directory marker blob already exists: {self.container_name}/{dir_identifier}")
        # except Exception as e:
        #     logger.error(f"Error creating directory marker blob {self.container_name}/{dir_identifier}: {e}")
        #     raise
        logger.debug(f"Azure makedirs called for {identifier}. Generally a no-op unless creating explicit markers.")
        pass # Often a no-op for blob storage

    async def close(self):
        """Closes the underlying BlobServiceClient."""
        if self._service_client:
            try:
                await self._service_client.close()
                logger.info("Closed Azure BlobServiceClient.")
            except Exception as e:
                logger.error(f"Error closing Azure BlobServiceClient: {e}")
            finally:
                self._service_client = None
                self._container_client = None

    async def __aenter__(self):
        await self._get_container_client() # Ensure client is ready
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

