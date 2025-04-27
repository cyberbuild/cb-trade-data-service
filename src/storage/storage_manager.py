# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\storage_manager.py
import logging
from typing import List, Dict, Any, Optional, Union
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

from .interfaces import IStorageManager, IStorageBackend

logger = logging.getLogger(__name__)

# Default partitioning scheme for Delta tables if not specified
DEFAULT_PARTITION_COLS = ["year", "month", "day"]

class StorageManagerImpl(IStorageManager):
    """Implements the high-level storage logic, handling formats and paths."""

    def __init__(self, backend: IStorageBackend):
        if not backend:
            raise ValueError("Storage backend cannot be None.")
        self.backend = backend
        logger.info(f"StorageManagerImpl initialized with backend: {type(backend).__name__}")

    def _generate_base_path(self, context: Dict[str, Any]) -> str:
        """Generates the base directory path for a given context.
           Example: data_type/exchange/coin/
        """
        required_keys = ['data_type', 'exchange', 'coin']
        if not all(key in context for key in required_keys):
            raise ValueError(f"Context must contain keys: {required_keys}")

        # Normalize inputs (e.g., lowercase, replace spaces)
        data_type = str(context['data_type']).lower().strip()
        exchange = str(context['exchange']).lower().replace(' ', '_').strip()
        coin = str(context['coin']).upper().replace('/', '_').strip() # e.g., BTC_USDT

        if not all([data_type, exchange, coin]):
             raise ValueError("Context values (data_type, exchange, coin) cannot be empty.")

        # Use POSIX-style paths internally
        path = f"{data_type}/{exchange}/{coin}"
        return path

    def _add_time_partitions_to_df(self, df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """Adds year, month, day columns based on a timestamp column for partitioning."""
        if timestamp_col not in df.columns:
             logger.warning(f"Timestamp column '{timestamp_col}' not found for partitioning. Skipping.")
             return df

        # Ensure timestamp is in datetime format
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            try:
                df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            except Exception as e:
                logger.error(f"Failed to convert timestamp column '{timestamp_col}' to datetime: {e}. Skipping partitioning.")
                return df

        df['year'] = df[timestamp_col].dt.year
        df['month'] = df[timestamp_col].dt.month
        df['day'] = df[timestamp_col].dt.day
        return df

    def _convert_to_arrow(self, data: Union[pd.DataFrame, pa.Table, List[Dict[str, Any]], Dict[str, Any]]) -> pa.Table:
        """Converts input data to a PyArrow Table."""
        if isinstance(data, pa.Table):
            return data
        elif isinstance(data, pd.DataFrame):
            # Add partitioning columns if timestamp exists before converting
            # data = self._add_time_partitions_to_df(data)
            return pa.Table.from_pandas(data, preserve_index=False)
        elif isinstance(data, list) and data:
            # Try creating DataFrame first for robust type inference
            df = pd.DataFrame(data)
            # data = self._add_time_partitions_to_df(df)
            return pa.Table.from_pandas(df, preserve_index=False)
        elif isinstance(data, dict):
            # Treat dict as a single row
            df = pd.DataFrame([data])
            # data = self._add_time_partitions_to_df(df)
            return pa.Table.from_pandas(df, preserve_index=False)
        else:
            raise ValueError(f"Unsupported data type for conversion to Arrow Table: {type(data)}")

    async def save_entry(
        self,
        context: Dict[str, Any],
        data: Union[pd.DataFrame, pa.Table, List[Dict[str, Any]], Dict[str, Any]],
        format_hint: Optional[str] = "delta", # Default to Delta Lake
        mode: str = "append",
        partition_cols: Optional[List[str]] = None,
        timestamp_col: str = 'timestamp' # Column used for time-based partitioning
    ):
        """Saves data, handling format conversion and backend interaction."""
        if data is None or (isinstance(data, (list, pd.DataFrame)) and len(data) == 0):
            logger.warning(f"Attempted to save empty data for context: {context}. Skipping.")
            return

        base_path = self._generate_base_path(context)
        arrow_table = self._convert_to_arrow(data)

        # Add partitioning columns if using default time partitioning
        if partition_cols is None and format_hint in ["delta", "parquet_partitioned"]:
            partition_cols = DEFAULT_PARTITION_COLS
            # Ensure timestamp column exists and add partition columns
            if timestamp_col in arrow_table.column_names:
                 # Convert timestamp to datetime objects for extraction
                 timestamps = arrow_table.column(timestamp_col).to_pandas()
                 if not pd.api.types.is_datetime64_any_dtype(timestamps):
                     timestamps = pd.to_datetime(timestamps)

                 years = pa.array(timestamps.dt.year, type=pa.int32())
                 months = pa.array(timestamps.dt.month, type=pa.int32())
                 days = pa.array(timestamps.dt.day, type=pa.int32())

                 # Add columns if they don't already exist
                 if 'year' not in arrow_table.column_names:
                     arrow_table = arrow_table.append_column('year', years)
                 if 'month' not in arrow_table.column_names:
                     arrow_table = arrow_table.append_column('month', months)
                 if 'day' not in arrow_table.column_names:
                     arrow_table = arrow_table.append_column('day', days)
            else:
                 logger.warning(f"Timestamp column '{timestamp_col}' not found. Cannot apply default time partitioning {DEFAULT_PARTITION_COLS}.")
                 partition_cols = [] # Reset partition_cols if timestamp is missing

        fmt = format_hint.lower() if format_hint else "delta"
        # Await the async call to get storage options
        storage_options = await self.backend.get_storage_options()
        identifier = base_path # For directory-based formats like Delta

        try:
            if fmt == "delta":
                table_uri = self.backend.get_uri_for_identifier(identifier)
                logger.debug(f"Saving data to Delta table: {table_uri}, mode: {mode}, partitions: {partition_cols}")
                # Ensure target directory exists conceptually for some backends
                await self.backend.makedirs(identifier, exist_ok=True)
                write_deltalake(
                    table_uri,
                    arrow_table,
                    mode=mode,
                    partition_by=partition_cols,
                    storage_options=storage_options
                    # engine='pyarrow' # Removed deprecated engine argument
                )
                logger.info(f"Successfully saved {arrow_table.num_rows} rows to Delta table: {table_uri}")

            elif fmt == "parquet":
                # Requires a specific file name. Generate one based on timestamp or UUID?
                # For simplicity, let's assume context includes a timestamp for filename
                ts = context.get('timestamp', datetime.utcnow())
                if isinstance(ts, (int, float)): ts = datetime.fromtimestamp(ts / 1000)
                if isinstance(ts, datetime):
                    filename = f"{ts.strftime('%Y%m%d_%H%M%S%f')}.parquet"
                else:
                    import uuid
                    filename = f"{uuid.uuid4()}.parquet"

                file_identifier = f"{base_path}/{filename}"
                logger.debug(f"Saving data to Parquet file: {file_identifier}")
                buf = io.BytesIO()
                pq.write_table(arrow_table, buf)
                await self.backend.save_bytes(file_identifier, buf.getvalue())
                logger.info(f"Successfully saved {arrow_table.num_rows} rows to Parquet file: {file_identifier}")

            elif fmt == "json": # JSON Lines format
                ts = context.get('timestamp', datetime.utcnow())
                if isinstance(ts, (int, float)): ts = datetime.fromtimestamp(ts / 1000)
                if isinstance(ts, datetime):
                    filename = f"{ts.strftime('%Y%m%d_%H%M%S%f')}.jsonl"
                else:
                    import uuid
                    filename = f"{uuid.uuid4()}.jsonl"

                file_identifier = f"{base_path}/{filename}"
                logger.debug(f"Saving data to JSON Lines file: {file_identifier}")
                # Convert arrow table to bytes using pandas to_json (lines=True)
                df = arrow_table.to_pandas()
                json_bytes = df.to_json(orient='records', lines=True).encode('utf-8')
                await self.backend.save_bytes(file_identifier, json_bytes)
                logger.info(f"Successfully saved {arrow_table.num_rows} rows to JSON Lines file: {file_identifier}")

            else:
                raise ValueError(f"Unsupported format_hint: {fmt}")

        except Exception as e:
            logger.error(f"Failed to save entry with context {context} and format {fmt}: {e}")
            raise

    async def get_range(
        self,
        context: Dict[str, Any],
        start_time: datetime,
        end_time: datetime,
        filters: Optional[List[tuple]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        output_format: str = "dataframe",
        format_hint: Optional[str] = "delta", # Assume Delta if not specified
        timestamp_col: str = 'timestamp'
    ) -> Union[pd.DataFrame, pa.Table, List[Dict[str, Any]]]:
        """Retrieves data, applying filters and handling formats."""

        base_path = self._generate_base_path(context)
        fmt = format_hint.lower() if format_hint else "delta"
        # Await the async call to get storage options
        storage_options = await self.backend.get_storage_options()

        logger.debug(f"Getting range for {base_path}, format: {fmt}, time: {start_time} -> {end_time}")

        try:
            if fmt == "delta":
                table_uri = self.backend.get_uri_for_identifier(base_path)
                try:
                    # Await storage_options if it's a coroutine (it should be awaited before this)
                    dt = DeltaTable(table_uri, storage_options=storage_options)
                except TableNotFoundError:
                    logger.warning(f"Delta table not found at: {table_uri}")
                    # Return empty result based on output_format
                    if output_format == "dataframe": return pd.DataFrame()
                    if output_format == "arrow": return pa.Table.from_pydict({})
                    return []

                # Build filters: Combine time range and custom filters
                combined_filters = []
                # Convert DeltaSchema to PyArrow schema to access field names
                pyarrow_schema = dt.schema().to_pyarrow()
                if timestamp_col in pyarrow_schema.names:
                    # Ensure start/end times are compatible with column type (e.g., timestamp or date)
                    # This might need adjustment based on actual schema inspection
                    time_filter_start = (timestamp_col, ">=", start_time)
                    time_filter_end = (timestamp_col, "<=", end_time)
                    combined_filters.extend([time_filter_start, time_filter_end])
                else:
                    logger.warning(f"Timestamp column '{timestamp_col}' not found in Delta table schema. Cannot apply time filter.")

                if filters:
                    combined_filters.extend(filters)

                logger.debug(f"Applying filters to Delta table: {combined_filters}")

                # Load data using filters and columns
                arrow_table = dt.to_pyarrow_table(filters=combined_filters, columns=columns)
                logger.info(f"Loaded {arrow_table.num_rows} rows from Delta table {table_uri} before limit/offset.")

            elif fmt == "parquet" or fmt == "json": # Assumes files are organized by date
                # This requires listing files and filtering based on filename/path patterns
                # which match the time range. More complex than reading a single Delta table.
                # For simplicity, this example will load ALL files under the base_path
                # and filter in memory. THIS IS INEFFICIENT for large datasets.
                # A better implementation would list files matching the date range.
                logger.warning(f"Loading range for {fmt} by listing all files under {base_path}. This can be inefficient.")
                all_files = await self.backend.list_items(prefix=base_path)
                relevant_files = [f for f in all_files if f.endswith(f".{fmt}") or f.endswith(f".{fmt}l")]

                if not relevant_files:
                    logger.warning(f"No {fmt} files found under: {base_path}")
                    if output_format == "dataframe": return pd.DataFrame()
                    if output_format == "arrow": return pa.Table.from_pydict({})
                    return []

                tables = []
                for file_identifier in relevant_files:
                    try:
                        file_bytes = await self.backend.load_bytes(file_identifier)
                        buf = pa.BufferReader(file_bytes)
                        if fmt == "parquet":
                            tables.append(pq.read_table(buf, columns=columns))
                        elif fmt == "json":
                            # PyArrow JSON reader might be more robust
                            tables.append(pj.read_json(buf)) # read_json doesn't support columns directly?
                    except FileNotFoundError:
                        logger.warning(f"File listed but not found during load: {file_identifier}")
                    except Exception as e:
                        logger.error(f"Error reading {fmt} file {file_identifier}: {e}")

                if not tables:
                    if output_format == "dataframe": return pd.DataFrame()
                    if output_format == "arrow": return pa.Table.from_pydict({})
                    return []

                arrow_table = pa.concat_tables(tables)

                # Apply time filtering in memory
                if timestamp_col in arrow_table.column_names:
                    ts_series = arrow_table.column(timestamp_col).to_pandas()
                    if not pd.api.types.is_datetime64_any_dtype(ts_series):
                        ts_series = pd.to_datetime(ts_series)
                    time_filter_mask = (ts_series >= start_time) & (ts_series <= end_time)
                    arrow_table = arrow_table.filter(pa.array(time_filter_mask))
                else:
                    logger.warning(f"Timestamp column '{timestamp_col}' not found. Cannot apply time filter in-memory.")

                # Apply other filters in memory (less efficient)
                if filters:
                    logger.warning("Applying additional filters in-memory for Parquet/JSON. Consider Delta Lake for predicate pushdown.")
                    # This requires converting filters to PyArrow expressions - complex
                    # For simplicity, convert to pandas and filter there if output is dataframe
                    if output_format == "dataframe":
                         df_temp = arrow_table.to_pandas()
                         for col, op, val in filters:
                              if col in df_temp.columns:
                                   if op == '==': df_temp = df_temp[df_temp[col] == val]
                                   elif op == '!=': df_temp = df_temp[df_temp[col] != val]
                                   elif op == '>': df_temp = df_temp[df_temp[col] > val]
                                   elif op == '>=': df_temp = df_temp[df_temp[col] >= val]
                                   elif op == '<': df_temp = df_temp[df_temp[col] < val]
                                   elif op == '<=': df_temp = df_temp[df_temp[col] <= val]
                                   # Add more ops as needed (in, not in, etc.)
                         arrow_table = pa.Table.from_pandas(df_temp, preserve_index=False)
                    else:
                         logger.warning("Cannot apply complex filters directly to Arrow table easily. Filters ignored.")

                logger.info(f"Loaded and filtered {arrow_table.num_rows} rows from {len(relevant_files)} {fmt} files under {base_path}.")

            else:
                raise ValueError(f"Unsupported format_hint for get_range: {fmt}")

            # Apply limit and offset (after all filtering)
            if offset > 0:
                arrow_table = arrow_table.slice(offset)
            if limit is not None and limit >= 0:
                arrow_table = arrow_table.slice(0, limit)

            # Convert to desired output format
            if output_format == "dataframe":
                return arrow_table.to_pandas()
            elif output_format == "arrow":
                return arrow_table
            elif output_format == "dict":
                return arrow_table.to_pylist()
            else:
                raise ValueError(f"Unsupported output_format: {output_format}")

        except Exception as e:
            logger.error(f"Failed to get range for context {context} and format {fmt}: {e}")
            raise

    async def list_coins(self, exchange_name: str, data_type: str) -> List[str]:
        """Lists available coins by listing subdirectories at the exchange level."""
        exchange_name_norm = exchange_name.lower().replace(' ', '_').strip()
        data_type_norm = data_type.lower().strip()
        prefix = f"{data_type_norm}/{exchange_name_norm}/"
        logger.debug(f"Listing coins with prefix: {prefix}")

        items = await self.backend.list_items(prefix)
        coins = set()
        for item_path in items:
            # Expecting paths like: data_type/exchange/COIN/... or data_type/exchange/COIN
            relative_path = Path(item_path).relative_to(Path(prefix))
            if relative_path.parts:
                coin_part = relative_path.parts[0]
                # Check if it looks like a coin directory (no extension) or a direct coin file
                # This logic might need refinement based on actual storage structure
                if '/' not in coin_part and '.' not in coin_part:
                     coins.add(coin_part)
                # Consider cases where coin itself might be stored as a file if not directory based
                # elif len(relative_path.parts) == 1 and '.' not in coin_part: # Is a file, but maybe the coin name?
                #    coins.add(coin_part)

        logger.info(f"Found {len(coins)} coins for {exchange_name}/{data_type}: {list(coins)}")
        return sorted(list(coins))

    async def check_coin_exists(self, exchange_name: str, coin_symbol: str, data_type: str) -> bool:
        """Checks if any data exists for a specific coin by listing its prefix."""
        context = {'data_type': data_type, 'exchange': exchange_name, 'coin': coin_symbol}
        prefix = self._generate_base_path(context) + '/' # Add trailing slash for directory check
        logger.debug(f"Checking existence with prefix: {prefix}")
        items = await self.backend.list_items(prefix)
        exists = bool(items)
        logger.info(f"Existence check for {prefix}: {exists}")
        return exists
