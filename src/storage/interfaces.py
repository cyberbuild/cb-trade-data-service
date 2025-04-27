# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\interfaces.py
import abc
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import pyarrow as pa
from datetime import datetime

class IStorageBackend(abc.ABC):
    """
    Interface for low-level, format-agnostic access to a physical storage medium.
    Handles byte-level operations, path/URI translation, and credentials.
    Identifiers are typically relative paths within the storage context (e.g., container/filesystem root).
    """

    @abc.abstractmethod
    def get_uri_for_identifier(self, identifier: str) -> str:
        """Returns the full, protocol-specific URI for a given logical identifier."""
        pass

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
    async def exists(self, identifier: str) -> bool:
        """Checks if an identifier exists."""
        pass

    @abc.abstractmethod
    async def delete(self, identifier: str):
        """Deletes the item at the specified identifier."""
        pass

    @abc.abstractmethod
    async def makedirs(self, identifier: str, exist_ok: bool = True):
        """Ensure that the directory for the identifier exists."""
        pass


class IStorageManager(abc.ABC):
    """
    High-level interface for storing and retrieving structured data,
    abstracting format details and backend implementation.
    Context dictionary typically includes keys like 'exchange', 'coin', 'data_type', 'timestamp'.
    """

    @abc.abstractmethod
    async def save_entry(
        self,
        context: Dict[str, Any],
        data: Union[pd.DataFrame, pa.Table, List[Dict[str, Any]], Dict[str, Any]],
        format_hint: Optional[str] = None,
        mode: str = "append", # Relevant for Delta Lake: 'append', 'overwrite', 'error'
        partition_cols: Optional[List[str]] = None # Relevant for Delta/Parquet
    ):
        """
        Saves a data entry (single record or batch) to the appropriate location based on context.
        Handles serialization to the target format (inferred or hinted).
        Assumes data is already standardized if necessary.
        """
        pass

    @abc.abstractmethod
    async def get_range(
        self,
        context: Dict[str, Any], # Must include 'exchange', 'coin', 'data_type'
        start_time: datetime,
        end_time: datetime,
        filters: Optional[List[tuple]] = None, # Additional filters for Delta Lake/Parquet
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        output_format: str = "dataframe" # 'dataframe', 'arrow', 'dict'
    ) -> Union[pd.DataFrame, pa.Table, List[Dict[str, Any]]]:
        """
        Retrieves data within a specified time range for a given context.
        Applies filtering efficiently where possible (e.g., predicate pushdown).
        """
        pass

    @abc.abstractmethod
    async def list_coins(self, exchange_name: str, data_type: str) -> List[str]:
        """Lists available coins for a given exchange and data type."""
        pass

    @abc.abstractmethod
    async def check_coin_exists(self, exchange_name: str, coin_symbol: str, data_type: str) -> bool:
        """Checks if data exists for a specific coin, exchange, and data type."""
        pass

    # Potentially add methods like get_latest_entry, delete_range, etc. as needed
