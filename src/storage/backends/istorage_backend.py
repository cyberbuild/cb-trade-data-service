# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\backends\base.py
import abc
from typing import List, Dict, Any, Optional

class IStorageBackend(abc.ABC):
    """
    Interface for low-level, format-agnostic access to a physical storage medium.
    Handles byte-level operations, path/URI translation, and credentials.
    Identifiers are typically relative paths within the storage context (e.g., container/filesystem root).
    """

    @abc.abstractmethod
    def get_uri_for_identifier(self, identifier: str) -> str:
        """Returns the full, protocol-specific URI for a given logical identifier."""
# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\backends\istorage_backend.py
    @abc.abstractmethod
    def get_storage_options(self) -> Optional[Dict[str, Any]]:
        """Returns a dictionary of options needed by libraries like fsspec, pyarrow, deltalake
           to access the storage (e.g., credentials, endpoint URLs)."""
        pass

    @abc.abstractmethod
    async def save_bytes(self, identifier: str, data: bytes):
        """Saves raw bytes to the specified identifier."""
        pass

    @abc.abstractmethod
    async def load_bytes(self, identifier: str) -> bytes:
        """Loads raw bytes from the specified identifier."""
        pass

    @abc.abstractmethod
    async def list_items(self, prefix: str = "") -> List[str]:
        """Lists identifiers (files/directories) matching a given prefix."""
        pass
    
    @abc.abstractmethod
    async def list_directories(self, prefix: str = "") -> List[str]:
        """Lists only directories (not files) matching a given prefix."""
        pass
    
    @abc.abstractmethod
    async def exists(self, identifier: str) -> bool:
        """Checks if the specified identifier exists."""
        pass
    
    def get_base_path(self, context: Dict[str, Any]) -> str:
        """
        Returns the base path for this storage backend.
        For local storage, this is the root directory.
        For cloud storage, this might be empty or a container name.
        """
        return ""

    @abc.abstractmethod
    async def delete(self, identifier: str):
        """Deletes the item at the specified identifier."""
        pass

    @abc.abstractmethod
    async def makedirs(self, identifier: str, exist_ok: bool = True):
        """Ensure that the directory for the identifier exists."""
        pass
