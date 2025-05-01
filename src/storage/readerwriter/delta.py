import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import pyarrow as pa
import io
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
import pyarrow.compute as pc

from .istorage_writer import DataWriter

logger = logging.getLogger(__name__)

class DeltaReaderWriter(DataWriter):
    """
    Formatter for Delta Lake format.
    Implements loading and saving data from/to Delta Lake tables.
    """
    
    async def load_range(
        self, 
        backend,
        base_path: str, 
        start_time: datetime, 
        end_time: datetime,
        filters: Optional[List[tuple]] = None,
        columns: Optional[List[str]] = None,
        timestamp_col: str = 'timestamp',
        storage_options: Dict[str, Any] = None
    ) -> pa.Table:
        """
        Load Delta table data within the specified time range.
        """
        table_uri = backend.get_uri_for_identifier(base_path)
        storage_options = storage_options or {}
        
        try:
            dt = DeltaTable(table_uri, storage_options=storage_options)
        except TableNotFoundError:
            logger.warning(f"Delta table not found at: {table_uri}")
            return pa.Table.from_pydict({})  # Return empty table
        
        # Build filters: Combine time range and custom filters
        combined_filters = []
        
        # Convert DeltaSchema to PyArrow schema to access field names
        pyarrow_schema = dt.schema().to_pyarrow()
        if timestamp_col in pyarrow_schema.names:
            # Add time range filters
            time_filter_start = (timestamp_col, ">=", start_time)
            time_filter_end = (timestamp_col, "<=", end_time)
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
        backend,
        base_path: str,
        data: pa.Table,
        context: Dict[str, Any],
        mode: str = "append",
        partition_cols: Optional[List[str]] = None,
        storage_options: Dict[str, Any] = None
    ) -> None:
        """
        Save data to a Delta Lake table.
        """
        table_uri = backend.get_uri_for_identifier(base_path)
        storage_options = storage_options or {}
        
        logger.debug(f"Saving data to Delta table: {table_uri}, mode: {mode}, partitions: {partition_cols}")
        
        # Ensure target directory exists conceptually for some backends
        await backend.makedirs(base_path, exist_ok=True)
        
        # Add year, month, day columns if partitioning by them, assuming a 'timestamp' column
        # This logic might be refined based on where timestamp processing occurs
        if partition_cols and all(col in ["year", "month", "day"] for col in partition_cols):
            if 'timestamp' in data.schema.names:
                # Ensure timestamp is datetime64[ns]
                timestamps_ns = data['timestamp'].cast(pa.timestamp('ns'))
                years = pc.year(timestamps_ns).cast(pa.int32())
                months = pc.month(timestamps_ns).cast(pa.int32())
                days = pc.day(timestamps_ns).cast(pa.int32())

                # Add columns if they don't already exist (e.g., from a previous step)
                if "year" not in data.column_names:
                    data = data.add_column(data.num_columns, pa.field("year", pa.int32()), years)
                if "month" not in data.column_names:
                    data = data.add_column(data.num_columns, pa.field("month", pa.int32()), months)
                if "day" not in data.column_names:
                    data = data.add_column(data.num_columns, pa.field("day", pa.int32()), days)
            else:
                # Handle case where partitioning by date is requested but no timestamp column exists
                # Maybe raise an error or log a warning?
                logger.warning("Partitioning by year/month/day requested, but no 'timestamp' column found.")
                # Proceed without adding columns, partitioning might fail or be incorrect

        write_options = {
            "mode": mode,  # Default to append, can be overridden by kwargs
            "partition_by": partition_cols if partition_cols else None,
            "schema_mode": "merge" # Allow schema evolution
        }
        # Filter out None values from options passed to write_deltalake
        filtered_options = {k: v for k, v in write_options.items() if v is not None}

        write_deltalake(table_uri, data, **filtered_options)
        
        logger.info(f"Successfully saved {data.num_rows} rows to Delta table: {table_uri}")
