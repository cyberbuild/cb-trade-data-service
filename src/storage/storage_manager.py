import logging
logger = logging.getLogger(__name__)

from typing import List, Dict, Any, Optional, Union, Type, TypeVar, Generic
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.json as pj
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from datetime import datetime
from pathlib import Path
import io
from abc import ABC, abstractmethod

from .backends.istorage_backend import IStorageBackend
from .partition_strategy import IPartitionStrategy, YearMonthDayPartitionStrategy
from .readerwriter.istorage_writer import IStorageWriter
from .path_strategy import IStoragePathStrategy

from exchange_source.models import Metadata, ExchangeData, IExchangeRecord, OHLCVRecord

logger = logging.getLogger(__name__)

TExchangeRecord = TypeVar('TRecord', bound=IExchangeRecord)

# --- Interface Definition ---
class IStorageManager(ABC, Generic[TExchangeRecord]):
    """
    High-level interface for storing and retrieving structured data,
    abstracting format details and backend implementation.
    Context dictionary typically includes keys like 'exchange', 'coin', 'data_type', 'timestamp'.
    """

    @abstractmethod
    async def save_entry(
        self,
        exchange_data: ExchangeData[TExchangeRecord], # Use ExchangeData directly
        **kwargs
    ):
        """
        Saves a data entry (single record or batch) to the appropriate location based on context.

        Args:
            exchange_data: An object containing the data and metadata.
            **kwargs: Additional arguments passed to the underlying writer (e.g., mode='overwrite').
        """
        pass

    @abstractmethod
    async def get_range(
        self,
        metadata: Metadata,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[ExchangeData[TExchangeRecord]]:
        """Retrieves a range of data based on metadata and date range.

        Args:
            metadata (Metadata): Metadata object identifying the data.
            start_date (Optional[datetime]): Start date for the data range.
            end_date (Optional[datetime]): End date for the data range.
            columns (Optional[List[str]]): Specific columns to retrieve.

        Returns:
            Optional[ExchangeData[TExchangeRecord]]: An ExchangeData object containing the requested data,
                                                    or None if not found.
        """
        pass

    @abstractmethod
    async def check_coin_exists(
        self,
        exchange_name: str,
        coin_symbol: str,
        data_type: str,
        interval: Optional[str] = None
    ) -> bool:
        """Checks if data exists for a specific coin, exchange, data type, and interval."""
        pass


# --- Implementation ---
class StorageManager(IStorageManager[TExchangeRecord]): # Inherit from moved interface
    """
    Implements the high-level storage logic, handling formats, paths, and partitioning.
    Uses injected strategies for backend, path generation, writing, formatting, and partitioning.
    """

    def __init__(
        self,
        backend: IStorageBackend,
        writer: IStorageWriter,
        path_strategy: IStoragePathStrategy,
        partition_strategy: Optional[IPartitionStrategy] = None # Make optional if DefaultPartitionStrategy exists
    ):
        """
        Initializes the StorageManager with necessary strategy components.
        All components (backend, writer, path_strategy) must be provided.
        """
        if not backend:
             raise ValueError("Storage backend instance must be provided.")
        self.backend = backend

        if not path_strategy:
            raise ValueError("Path strategy instance must be provided.")
        self.path_strategy = path_strategy

        if not writer:
             raise ValueError("Writer instance must be provided.")
        self.writer = writer

        # Use provided partition strategy or a default one if applicable
        self.partition_strategy = partition_strategy or YearMonthDayPartitionStrategy() # Assuming YearMonthDay is default

        logger.info(f"StorageManager initialized with: "
                    f"Backend={type(self.backend).__name__}, "
                    f"PathStrategy={type(self.path_strategy).__name__}, "
                    # f"Formatter={type(self.formatter).__name__}, " # Formatter removed?
                    f"Writer={type(self.writer).__name__}, "
                    f"PartitionStrategy={type(self.partition_strategy).__name__}")

    @property
    @abstractmethod
    def record_type(self) -> Type[TExchangeRecord]:
        """The concrete IExchangeRecord subclass this manager handles."""
        pass

    # --- Data Loading ---
    async def get_range(
        self,
        metadata: Metadata, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[ExchangeData[TExchangeRecord]]:
        base_path = self.path_strategy.get_path(metadata)
        logger.info(f"Getting range from {base_path} for {metadata} between {start_date} and {end_date}")

        try:
            table = await self.writer.load_range(
                self.backend,
                base_path,
                start_date,
                end_date,
                filters=None,
                columns=columns
            )
            logger.info(f"Successfully loaded data from {base_path}")
            if table is None or (hasattr(table, 'num_rows') and table.num_rows == 0):
                return None
            # If the result is already an ExchangeData, return it
            if isinstance(table, ExchangeData):
                return table
            # Otherwise, assume it's a pyarrow Table and convert
            records = [self.record_type(row) for row in table.to_pylist()]
            return ExchangeData(records, metadata)
        except TableNotFoundError:
            logger.warning(f"Table not found at path: {base_path}")
            return None
        except NotImplementedError:
            logger.error(f"Writer {type(self.writer).__name__} does not support load_range.")
            raise
        except Exception as e:
            logger.error(f"Failed to load data from {base_path}: {e}", exc_info=True)
            raise

    async def check_coin_exists(self, exchange_name: str, record_type: str, coin_symbol: str, interval: str) -> bool:
        """Checks if any data exists for a specific coin using the path strategy."""
        context = Metadata({
            'record_type': record_type, 
            'exchange': exchange_name, 
            'coin': coin_symbol,
            'interval': interval 
        })

        prefix = self.path_strategy.get_path(context) + '/' 
        logger.debug(f"Checking existence with prefix: {prefix}")
       
        items = await self.backend.list_items(prefix, limit=1) 
        exists = bool(items)
        logger.info(f"Existence check for {prefix}: {exists}")
        return exists

 
    async def save_entry(self, exchange_data: ExchangeData[TExchangeRecord], **kwargs):
        """
        Saves a single data entry using the configured formatter, writer, and strategies.
        Determines partitioning based on the PartitionStrategy.
        """
        metadata = exchange_data.metadata
        # Ensure data is not empty before proceeding
        if not exchange_data.data: # Check the original list in ExchangeData
            logger.warning(f"Attempted to save empty data for {metadata}. Skipping.")
            return

        logger.info(f"Saving entry for {metadata}")

        # 1. Format data using the formatter (e.g., to Arrow Table)
        # Assuming formatter takes the list of BaseExchangeData objects
        try:
            formatted_data = exchange_data.to_arrow()
        except Exception as e:
            logger.error(f"Failed to format data for {metadata}: {e}", exc_info=True)
            raise

        # 2. Determine path using the path strategy
        try:
            base_path = self.path_strategy.get_path(metadata)
        except Exception as e:
            logger.error(f"Failed to determine storage path for {metadata}: {e}", exc_info=True)
            raise

        # 3. Determine partition columns using the partition strategy
        try:
            partition_cols = self.partition_strategy.get_partition_cols(metadata)
        except Exception as e:
            logger.error(f"Failed to determine partition columns for {metadata}: {e}", exc_info=True)
            # Decide behavior: proceed without partitioning or raise?
            # Let's proceed without partitioning for now, but log warning.
            logger.warning(f"Proceeding without partitioning due to error in strategy.")
            partition_cols = None 

        # 4. Write data using the writer
        try:
            logger.info(f"Writing data to {base_path} with partitions: {partition_cols}, writer: {type(self.writer).__name__}")
            # Pass relevant kwargs, potentially filtering based on writer capabilities
            await self.writer.write(
                formatted_data,
                base_path,
                metadata,
                partition_cols=partition_cols, # Pass determined partition columns
                **kwargs # Pass through other options like mode='overwrite'
            )
            logger.info(f"Successfully saved data to {base_path}")
        except Exception as e:
            logger.error(f"Failed to write data to {base_path}: {e}", exc_info=True)
            raise # Re-raise the exception after logging

class OHLCVStorageManager(StorageManager[OHLCVRecord]): # Specify the concrete type here

    @property
    def record_type(self) -> Type[OHLCVRecord]:
        """Specifies that this manager handles OHLCVRecord."""
        return OHLCVRecord

    def __init__(
        self,
        backend: IStorageBackend, 
        writer: IStorageWriter, 
        path_strategy: IStoragePathStrategy,
        partition_strategy: IPartitionStrategy = None
    ):
        partition_strategy_instance = partition_strategy or YearMonthDayPartitionStrategy() 
        logger.info(f"OHLCVStorageManager using partition strategy: {type(partition_strategy_instance).__name__}.")

        super().__init__(
            backend=backend,
            writer=writer,
            path_strategy=path_strategy,
            partition_strategy=partition_strategy_instance
        )

