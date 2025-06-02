# tests/integration/storage/readerwriter/test_delta_integration.py
import logging
import uuid
import pytest
import pandas as pd
import pyarrow as pa
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Generator

# Use PEP 420 compliant imports
from storage.backends.istorage_backend import IStorageBackend
from storage.readerwriter.delta import DeltaReaderWriter
from storage.storage_settings import StorageSettings
from storage.path_strategy import (
    OHLCVPathStrategy,
    IStoragePathStrategy,
)  # Added IStoragePathStrategy
from tests.helpers.storage.data_utils import generate_test_data  # Reusing helper
from tests.helpers.path_test_strategy import OHLCVTestPathStrategy  # Added import
from storage.backends.azure_blob_backend import AzureBlobBackend  # Added import

logger = logging.getLogger(__name__)

# --- Fixtures ---


# Reuse the backend fixtures from the root conftest.py
# We request 'local_backend' and 'azure_backend' which are defined there.
@pytest.fixture(params=["local_backend", "azure_backend"], scope="function")
def storage_backend(request) -> Generator[IStorageBackend, None, None]:
    """Provides parameterized storage backends (Local and Azure)."""
    backend_fixture_name = request.param
    backend: IStorageBackend = request.getfixturevalue(backend_fixture_name)
    logger.info(f"--- Testing with Backend: {type(backend).__name__} ---")
    yield backend
    # Cleanup is handled by the original backend fixtures in root conftest


@pytest.fixture(scope="function")
def path_strategy(storage_backend: IStorageBackend) -> IStoragePathStrategy:
    """
    Provides a path strategy. Uses OHLCVTestPathStrategy for Azure backend
    to ensure test isolation with prefixed paths.
    """
    if isinstance(storage_backend, AzureBlobBackend):
        return OHLCVTestPathStrategy()
    else:  # For LocalFileBackend or any other
        return OHLCVPathStrategy()


@pytest.fixture(scope="function")
def delta_reader_writer(storage_backend: IStorageBackend) -> DeltaReaderWriter:
    """Provides a DeltaReaderWriter instance configured with the active backend."""
    return DeltaReaderWriter(backend=storage_backend)


@pytest.fixture(scope="function")
def sample_data() -> pa.Table:
    """Provides a sample PyArrow Table for testing."""
    start_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(
        2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc
    )  # 10 hours of data (0-9)
    df = generate_test_data(start_dt, end_dt, freq_minutes=60)
    # Ensure timestamp is compatible
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    # Add partition columns
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    return pa.Table.from_pandas(df)


@pytest.fixture(scope="function")
def test_context() -> Dict[str, Any]:
    """Provides a sample context dictionary, simulating StorageManager context."""
    from config import get_settings

    settings = get_settings()
    real_exchange = settings.exchange_id

    # Use unique run ID to avoid collisions between parallel tests if ever run
    run_id = uuid.uuid4()
    return {
        "exchange": real_exchange,
        "coin": "TEST/USD",
        "interval": "1h",  # Required by OHLCVPathStrategy
        "data_type": f"test_ohlcv_{run_id}",  # Unique data type for isolation
        "year": 2024,
        "month": 1,
        "day": 1,
    }


@pytest.fixture
def storage_settings(test_context: Dict[str, Any]) -> StorageSettings:
    """Creates StorageSettings based on the test context."""
    # Define partition columns used in sample_data
    partition_cols = ["year", "month", "day"]
    return StorageSettings(
        context=test_context,
        partition_cols=partition_cols,
        format_hint="delta",
        mode="overwrite",  # Default mode for initial save
        timestamp_col="timestamp",
    )


# --- Test Cases ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delta_write_read(
    delta_reader_writer: DeltaReaderWriter,
    sample_data: pa.Table,
    storage_settings: StorageSettings,
    test_context: Dict[str, Any],
    path_strategy: IStoragePathStrategy,
):
    """
    Tests writing data using save_table and reading it back using load_range.
    This test runs for both local and Azure backends via fixture parameterization.
    """
    # Get backend type from the reader/writer's backend
    backend_type = type(delta_reader_writer.backend).__name__
    logger.info(f"[{backend_type}] Running test_delta_write_read...")

    # --- Write ---
    logger.info(f"[{backend_type}] Writing sample data...")

    # Generate proper path using strategy
    relative_path = path_strategy.generate_base_path(test_context)

    # Get the full path for writing
    # For our test we'll just use the relative path directly
    write_base_path = relative_path
    logger.info(
        f"[{backend_type}] Target write base path: {write_base_path}"
    )  # Call save_table (which delegates to save_data)
    await delta_reader_writer.save_table(
        data_table=sample_data,
        path=write_base_path,
        mode=storage_settings.mode,
        partition_cols=storage_settings.partition_cols,
    )
    logger.info(f"[{backend_type}] Data written successfully.")

    # Add delay for Azure eventual consistency
    if backend_type == "AzureBlobBackend":
        import asyncio

        await asyncio.sleep(2)  # Give Azure time to propagate the write
        logger.info(f"[{backend_type}] Waited for Azure consistency.")

    # --- Read ---
    logger.info(f"[{backend_type}] Reading data back...")
    start_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_date = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)

    # Use the same path for reading
    read_base_path = write_base_path
    logger.info(f"[{backend_type}] Target read base path: {read_base_path}")

    timestamp_col = (
        storage_settings.timestamp_col
        if hasattr(storage_settings, "timestamp_col")
        else "timestamp"
    )

    # Add retry logic for Azure eventual consistency
    read_table = None
    max_retries = 5 if backend_type == "AzureBlobBackend" else 1

    for attempt in range(max_retries):
        try:
            # Call load_range with backend as first argument (interface compliance)
            read_table = await delta_reader_writer.load_range(
                delta_reader_writer.backend,
                read_base_path,
                start_date,
                end_date,
                None,
                None,
                timestamp_col,
            )
            if read_table is not None and read_table.num_rows > 0:
                break  # Success
            elif attempt < max_retries - 1:
                logger.info(
                    f"[{backend_type}] Read attempt {attempt + 1} returned {read_table.num_rows if read_table else 0} rows, retrying..."
                )
                await asyncio.sleep(1)  # Wait before retry

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"[{backend_type}] Read attempt {attempt + 1} failed: {e}, retrying..."
                )
                await asyncio.sleep(1)
            else:
                logger.error(f"[{backend_type}] Error during read: {e}", exc_info=True)
                pytest.fail(f"[{backend_type}] Failed to read data: {e}")

    logger.info(f"[{backend_type}] Data read successfully. Rows: {read_table.num_rows}")

    # --- Verification ---
    logger.info(f"[{backend_type}] Verifying data...")
    assert read_table is not None, f"[{backend_type}] Read operation returned None"
    expected_count = sample_data.num_rows
    actual_count = read_table.num_rows
    logger.info(
        f"[{backend_type}] Expected record count: {expected_count}, Actual record count: {actual_count}"
    )
    assert (
        actual_count == expected_count
    ), f"[{backend_type}] Row count mismatch after read: expected {expected_count}, got {actual_count}"
    # Comparing schemas directly might fail if pandas->arrow conversion slightly changes types (e.g., int32 vs int64)
    # Let's compare column names and row count for now. Deeper comparison below.
    assert set(read_table.column_names) == set(
        sample_data.column_names
    ), f"[{backend_type}] Column names mismatch after read"

    # Optional: Deeper comparison if needed (can be slow for large data)
    # Convert both to pandas for easier comparison, ensure sorting
    read_df = (
        read_table.to_pandas().sort_values(by=timestamp_col).reset_index(drop=True)
    )
    sample_df = (
        sample_data.to_pandas().sort_values(by=timestamp_col).reset_index(drop=True)
    )

    # Convert timestamp back to the same type if necessary for comparison
    if pd.api.types.is_datetime64_ns_dtype(read_df[timestamp_col]):
        # Ensure sample_df timestamp also has timezone info if read_df does
        if (
            read_df[timestamp_col].dt.tz is not None
            and sample_df[timestamp_col].dt.tz is None
        ):
            sample_df[timestamp_col] = sample_df[timestamp_col].dt.tz_localize("UTC")
        elif (
            read_df[timestamp_col].dt.tz is None
            and sample_df[timestamp_col].dt.tz is not None
        ):
            read_df[timestamp_col] = read_df[timestamp_col].dt.tz_localize("UTC")

    # Compare dataframes
    pd.testing.assert_frame_equal(
        read_df[[c for c in read_df.columns if c != timestamp_col]],
        sample_df[[c for c in sample_df.columns if c != timestamp_col]],
        check_dtype=False,
    )

    # Log success
    logger.info(f"[{backend_type}] Test completed successfully!")
