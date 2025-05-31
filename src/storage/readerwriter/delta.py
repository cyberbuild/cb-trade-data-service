import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import pyarrow as pa
import io
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
import pyarrow.compute as pc

from .istorage_writer import IStorageWriter
from storage.backends.istorage_backend import IStorageBackend # Import the backend interface

logger = logging.getLogger(__name__)

class DeltaReaderWriter(IStorageWriter):
    """
    Formatter for Delta Lake format.
    Implements loading and saving data from/to Delta Lake tables.
    """
    def __init__(self, backend: IStorageBackend): # Add backend to init
        self.backend = backend
        super().__init__() # Call parent init if necessary

    async def load_range(
        self,
        backend,  # Accept for interface compatibility, ignored (use self.backend)
        base_path: str,
        start_time: datetime,
        end_time: datetime,
        filters: Optional[List[tuple]] = None,
        columns: Optional[List[str]] = None,
        timestamp_col: str = None,
        storage_options: Optional[Dict[str, Any]] = None
    ) -> pa.Table:
        """
        Load Delta table data within the specified time range.
        Uses storage options from the backend instance.
        """
        table_uri = self.backend.get_uri_for_identifier(base_path)
        # Get storage options directly from the backend
        storage_options = await self.backend.get_storage_options() or {}
        logger.debug(f"Using storage options for DeltaTable: {storage_options}")

        try:
            # Pass the backend's storage options
            dt = DeltaTable(table_uri, storage_options=storage_options)
        except TableNotFoundError:
            logger.warning(f"Delta table not found at: {table_uri}")
            return pa.Table.from_pydict({})  # Return empty table
        except Exception as e:
            logger.error(f"Error initializing DeltaTable for {table_uri} with options {storage_options}: {e}", exc_info=True)
            raise # Re-raise the exception after logging

        # Use provided timestamp_col or default, and ensure it is in the schema
        if not timestamp_col or timestamp_col not in dt.schema().to_pyarrow().names:
            # Fallback to the first timestamp column found, or default to 'timestamp'
            pyarrow_schema = dt.schema().to_pyarrow()
            found = next((name for name in pyarrow_schema.names if pa.types.is_timestamp(pyarrow_schema.field(name).type)), None)
            timestamp_col = found if found else "timestamp"

        # Build filters: Combine time range and custom filters
        combined_filters = []

        # Convert DeltaSchema to PyArrow schema to access field names
        pyarrow_schema = dt.schema().to_pyarrow()
        if timestamp_col in pyarrow_schema.names:
            # Add time range filters
            # Ensure start_time and end_time are timezone-aware if comparing with timezone-aware Delta column
            # Assuming Delta table stores timestamps in UTC or without timezone
            # If Delta has timezone, ensure comparison values match
            time_filter_start = (timestamp_col, ">=", start_time)
            time_filter_end = (timestamp_col, "<=", end_time) # Inclusive end time
            combined_filters.extend([time_filter_start, time_filter_end])
        else:
            logger.warning(f"Timestamp column '{timestamp_col}' not found in Delta table schema. Cannot apply time filter.")

        # Add any custom filters
        if filters:
            combined_filters.extend(filters)

        logger.debug(f"Applying filters to Delta table: {combined_filters}")

        # Load data using filters and columns
        arrow_table = dt.to_pyarrow_table(filters=combined_filters, columns=columns)
        logger.info(f"Loaded {arrow_table.num_rows} rows from Delta table {table_uri} before limit/offset.")

        return arrow_table

    async def save_data(
        self,
        backend,  # Accept for interface compatibility, ignored (use self.backend)
        base_path: str,
        data: pa.Table,
        context: Optional[Dict[str, Any]] = None,
        mode: str = "append",
        partition_cols: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        timestamp_col: str = 'timestamp',
        timestamp_type: str = 'datetime64[ns, UTC]'
    ) -> None:
        """
        Save data to a Delta Lake table.
        Uses storage options from the backend instance.
        """
        table_uri = self.backend.get_uri_for_identifier(base_path)
        # Always use self.backend for storage options
        resolved_storage_options = await self.backend.get_storage_options() or {}
        logger.debug(f"Saving data to Delta table: {table_uri}, mode: {mode}, partitions: {partition_cols}, options: {resolved_storage_options}")

        # Ensure target directory exists conceptually for some backends
        await self.backend.makedirs(base_path, exist_ok=True)

        # Add year, month, day columns if partitioning by them, assuming a 'timestamp' column
        # This logic might be refined based on where timestamp processing occurs
        if partition_cols and all(col in ["year", "month", "day"] for col in partition_cols):
            if 'timestamp' in data.schema.names:
                try:
                    # Always cast to naive timestamp (no tz) to avoid ArrowInvalid: Cannot locate timezone 'UTC'
                    ts_type = data['timestamp'].type
                    if pa.types.is_timestamp(ts_type):
                        # Remove timezone if present
                        if ts_type.tz is not None:
                            timestamps_ns = data['timestamp'].cast(pa.timestamp('ns'))
                        else:
                            timestamps_ns = data['timestamp']
                    else:
                        timestamps_ns = data['timestamp'].cast(pa.timestamp('ns'))

                    years = pc.year(timestamps_ns).cast(pa.int32())
                    months = pc.month(timestamps_ns).cast(pa.int32())
                    days = pc.day(timestamps_ns).cast(pa.int32())

                    if "year" not in data.column_names:
                        data = data.append_column("year", years)
                    if "month" not in data.column_names:
                        data = data.append_column("month", months)
                    if "day" not in data.column_names:
                        data = data.append_column("day", days)
                except Exception as e:
                    logger.error(f"Error processing timestamp for partitioning columns: {e}", exc_info=True)
            else:
                logger.warning("Partitioning by year/month/day requested, but 'timestamp' column not found in data.")


        try:
            # Pass the backend's storage options to write_deltalake
            write_deltalake(
                table_or_uri=table_uri,
                data=data,
                mode=mode,
                partition_by=partition_cols,
                storage_options=resolved_storage_options,
                engine='rust',
                schema_mode="merge"
            )
            logger.info(f"Successfully wrote {data.num_rows} rows to Delta table: {table_uri}")
        except Exception as e:
            logger.error(f"Error writing Delta table {table_uri} with options {storage_options}: {e}", exc_info=True)
            raise # Re-raise the exception after logging

    # Add save_table if it's distinct from save_data or used elsewhere
    # Assuming save_table was just a placeholder name for save_data in the test
    async def save_table(self, data_table: pa.Table, path: str, mode: str, partition_cols: Optional[List[str]] = None):
        await self.save_data(
            self.backend,
            path,
            data_table,
            {},
            mode,
            partition_cols
        )
