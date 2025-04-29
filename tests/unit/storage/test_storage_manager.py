import pytest
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, ANY
import io

# Use PEP 420 compliant imports
from storage.storage_manager import StorageManagerImpl, DEFAULT_PARTITION_COLS
from storage.interfaces import IStorageBackend

# --- Fixtures ---

@pytest.fixture
def mock_backend() -> AsyncMock:
    """Provides a mock IStorageBackend."""
    mock = AsyncMock(spec=IStorageBackend)
    mock.get_storage_options = AsyncMock(return_value={})
    # get_uri_for_identifier should be a regular Mock, not AsyncMock, since it is not async
    from unittest.mock import Mock
    mock.get_uri_for_identifier = Mock(side_effect=lambda identifier: f"mock://{identifier}")
    mock.list_items = AsyncMock(return_value=[])
    mock.load_bytes = AsyncMock(return_value=b"")
    mock.makedirs = AsyncMock()
    mock.save_bytes = AsyncMock()
    return mock

@pytest.fixture
def storage_manager(mock_backend: AsyncMock) -> StorageManagerImpl:
    """Provides a StorageManagerImpl instance with a mock backend."""
    return StorageManagerImpl(backend=mock_backend)

@pytest.fixture
def sample_context() -> dict:
    return {'data_type': 'ohlcv', 'exchange': 'binance', 'coin': 'BTC/USDT'}

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provides a sample pandas DataFrame."""
    now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return pd.DataFrame({
        'timestamp': [now, now + pd.Timedelta(hours=1)],
        'open': [100.0, 101.0],
        'high': [105.0, 106.0],
        'low': [99.0, 100.0],
        'close': [101.0, 105.0],
        'volume': [1000, 1100]
    })

@pytest.fixture
def sample_arrow_table(sample_dataframe: pd.DataFrame) -> pa.Table:
    """Provides a sample PyArrow Table."""
    return pa.Table.from_pandas(sample_dataframe, preserve_index=False)

# --- Test Cases ---

def test_init_success(mock_backend: AsyncMock):
    """Test successful initialization."""
    manager = StorageManagerImpl(backend=mock_backend)
    assert manager.backend is mock_backend

def test_init_no_backend():
    """Test initialization raises ValueError if backend is None."""
    with pytest.raises(ValueError, match="Storage backend cannot be None"):
        StorageManagerImpl(backend=None)

# --- _generate_base_path ---

def test_generate_base_path_success(storage_manager: StorageManagerImpl, sample_context: dict):
    """Test generating a valid base path."""
    expected_path = "ohlcv/binance/BTC_USDT"
    assert storage_manager._generate_base_path(sample_context) == expected_path

def test_generate_base_path_missing_key(storage_manager: StorageManagerImpl):
    """Test error handling for missing context keys."""
    invalid_context = {'data_type': 'ohlcv', 'exchange': 'binance'} # Missing 'coin'
    with pytest.raises(ValueError, match="Context must contain keys"):
        storage_manager._generate_base_path(invalid_context)

def test_generate_base_path_empty_value(storage_manager: StorageManagerImpl):
    """Test error handling for empty context values."""
    invalid_context = {'data_type': '', 'exchange': 'binance', 'coin': 'BTC/USDT'}
    with pytest.raises(ValueError, match="Context values .* cannot be empty"):
        storage_manager._generate_base_path(invalid_context)

def test_generate_base_path_normalization(storage_manager: StorageManagerImpl):
    """Test normalization of context values."""
    context = {'data_type': 'OHLCV ', 'exchange': 'Binance Global', 'coin': ' ETH/BTC '}
    expected_path = "ohlcv/binance_global/ETH_BTC"
    assert storage_manager._generate_base_path(context) == expected_path

# --- _convert_to_arrow ---

def test_convert_to_arrow_from_dataframe(storage_manager: StorageManagerImpl, sample_dataframe: pd.DataFrame):
    """Test conversion from pandas DataFrame."""
    arrow_table = storage_manager._convert_to_arrow(sample_dataframe)
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == len(sample_dataframe)
    assert set(arrow_table.column_names) == set(sample_dataframe.columns)

def test_convert_to_arrow_from_arrow_table(storage_manager: StorageManagerImpl, sample_arrow_table: pa.Table):
    """Test conversion from PyArrow Table (should return identity)."""
    arrow_table = storage_manager._convert_to_arrow(sample_arrow_table)
    assert arrow_table is sample_arrow_table # Should be the same object

def test_convert_to_arrow_from_list_dict(storage_manager: StorageManagerImpl):
    """Test conversion from list of dictionaries."""
    data = [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}]
    arrow_table = storage_manager._convert_to_arrow(data)
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 2
    assert set(arrow_table.column_names) == {'a', 'b'}

def test_convert_to_arrow_from_dict(storage_manager: StorageManagerImpl):
    """Test conversion from a single dictionary."""
    data = {'a': 1, 'b': 'x'}
    arrow_table = storage_manager._convert_to_arrow(data)
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 1
    assert set(arrow_table.column_names) == {'a', 'b'}

def test_convert_to_arrow_unsupported(storage_manager: StorageManagerImpl):
    """Test conversion raises error for unsupported types."""
    with pytest.raises(ValueError, match="Unsupported data type"):
        storage_manager._convert_to_arrow(123)
    with pytest.raises(ValueError, match="Unsupported data type"):
        storage_manager._convert_to_arrow("string")

# --- save_entry ---

@pytest.mark.asyncio
@patch('storage.storage_manager.write_deltalake') # Mock the actual write function
async def test_save_entry_delta_default_partition(
    mock_write_deltalake: MagicMock,
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict,
    sample_dataframe: pd.DataFrame
):
    """Test saving delta with default time partitioning."""
    await storage_manager.save_entry(sample_context, sample_dataframe, format_hint="delta", mode="append")

    expected_base_path = "ohlcv/binance/BTC_USDT"
    expected_uri = f"mock://{expected_base_path}"
    mock_backend.get_storage_options.assert_called_once()
    mock_backend.get_uri_for_identifier.assert_called_once_with(expected_base_path)
    mock_backend.makedirs.assert_awaited_once_with(expected_base_path, exist_ok=True)

    # Check that write_deltalake was called with correct args
    call_args, call_kwargs = mock_write_deltalake.call_args
    assert call_args[0] == expected_uri
    assert isinstance(call_args[1], pa.Table) # Check data type
    assert 'year' in call_args[1].column_names # Check partition cols added
    assert 'month' in call_args[1].column_names
    assert 'day' in call_args[1].column_names
    assert call_kwargs['mode'] == "append"
    assert call_kwargs['partition_by'] == DEFAULT_PARTITION_COLS
    assert call_kwargs['storage_options'] == {}

@pytest.mark.asyncio
@patch('storage.storage_manager.write_deltalake')
async def test_save_entry_delta_custom_partition(
    mock_write_deltalake: MagicMock,
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict,
    sample_dataframe: pd.DataFrame
):
    """Test saving delta with custom partitioning."""
    custom_partitions = ['exchange', 'data_type']
    # Add partition columns to dataframe for the test
    df_with_partitions = sample_dataframe.copy()
    df_with_partitions['exchange'] = sample_context['exchange']
    df_with_partitions['data_type'] = sample_context['data_type']

    await storage_manager.save_entry(
        sample_context,
        df_with_partitions,
        format_hint="delta",
        mode="overwrite",
        partition_cols=custom_partitions
    )

    expected_base_path = "ohlcv/binance/BTC_USDT"
    expected_uri = f"mock://{expected_base_path}"
    mock_backend.makedirs.assert_awaited_once_with(expected_base_path, exist_ok=True)

    call_args, call_kwargs = mock_write_deltalake.call_args
    assert call_args[0] == expected_uri
    assert isinstance(call_args[1], pa.Table)
    assert 'year' not in call_args[1].column_names # Default partitions should NOT be added
    assert call_kwargs['mode'] == "overwrite"
    assert call_kwargs['partition_by'] == custom_partitions

@pytest.mark.asyncio
@patch('storage.storage_manager.pq.write_table') # Mock parquet write
async def test_save_entry_parquet(
    mock_write_parquet: MagicMock,
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict,
    sample_dataframe: pd.DataFrame
):
    """Test saving in Parquet format."""
    await storage_manager.save_entry(sample_context, sample_dataframe, format_hint="parquet")

    expected_base_path = "ohlcv/binance/BTC_USDT"
    mock_backend.get_storage_options.assert_called_once()
    # Check that save_bytes was called (filename might vary, so check prefix)
    mock_backend.save_bytes.assert_awaited_once()
    call_args, _ = mock_backend.save_bytes.call_args
    assert call_args[0].startswith(expected_base_path + "/")
    assert call_args[0].endswith(".parquet")
    assert isinstance(call_args[1], bytes) # Check data type

    # Check that pq.write_table was called
    mock_write_parquet.assert_called_once()
    assert isinstance(mock_write_parquet.call_args[0][0], pa.Table) # Arg 0 is table
    assert isinstance(mock_write_parquet.call_args[0][1], io.BytesIO) # Arg 1 is buffer

@pytest.mark.asyncio
async def test_save_entry_json(
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict,
    sample_dataframe: pd.DataFrame
):
    """Test saving in JSON Lines format."""
    await storage_manager.save_entry(sample_context, sample_dataframe, format_hint="json")

    expected_base_path = "ohlcv/binance/BTC_USDT"
    mock_backend.get_storage_options.assert_called_once()
    # Check that save_bytes was called (filename might vary, so check prefix)
    mock_backend.save_bytes.assert_awaited_once()
    call_args, _ = mock_backend.save_bytes.call_args
    assert call_args[0].startswith(expected_base_path + "/")
    assert call_args[0].endswith(".jsonl")
    assert isinstance(call_args[1], bytes)

@pytest.mark.asyncio
async def test_save_entry_empty_data(
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict
):
    """Test that saving empty data is skipped."""
    await storage_manager.save_entry(sample_context, pd.DataFrame(), format_hint="delta")
    await storage_manager.save_entry(sample_context, [], format_hint="delta")
    await storage_manager.save_entry(sample_context, None, format_hint="delta")

    mock_backend.save_bytes.assert_not_called()
    # write_deltalake is mocked globally, need to check if it was called via patch object if used
    # For this test, checking backend calls is sufficient as conversion happens first.

@pytest.mark.asyncio
async def test_save_entry_unsupported_format(
    storage_manager: StorageManagerImpl,
    sample_context: dict,
    sample_dataframe: pd.DataFrame
):
    """Test saving with an unsupported format raises error."""
    with pytest.raises(ValueError, match="Unsupported format_hint: csv"):
        await storage_manager.save_entry(sample_context, sample_dataframe, format_hint="csv")

# --- get_range ---

@pytest.mark.asyncio
@patch('storage.storage_manager.DeltaTable') # Mock DeltaTable class
async def test_get_range_delta_success(
    MockDeltaTable: MagicMock,
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict,
    sample_arrow_table: pa.Table
):
    """Test getting a range from a Delta table."""
    start_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 15, 23, 59, 59, tzinfo=timezone.utc)

    # Configure the mock DeltaTable instance
    mock_dt_instance = MockDeltaTable.return_value
    # Mock dt.schema().to_pyarrow().names to include 'timestamp'
    mock_dt_instance.schema.return_value.to_pyarrow.return_value.names = ['timestamp']
    mock_dt_instance.to_pyarrow_table.return_value = sample_arrow_table

    result_df = await storage_manager.get_range(
        sample_context,
        start_time,
        end_time,
        output_format="dataframe",
        format_hint="delta",
        timestamp_col='timestamp'
    )

    expected_base_path = "ohlcv/binance/BTC_USDT"
    expected_uri = f"mock://{expected_base_path}"
    mock_backend.get_storage_options.assert_called_once()
    mock_backend.get_uri_for_identifier.assert_called_once_with(expected_base_path)
    MockDeltaTable.assert_called_once_with(expected_uri, storage_options={})

    # Check filters passed to DeltaTable
    expected_filters = [
        ('timestamp', '>=', start_time),
        ('timestamp', '<=', end_time)
    ]
    mock_dt_instance.to_pyarrow_table.assert_called_once_with(filters=expected_filters, columns=None)

    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert len(result_df) == sample_arrow_table.num_rows

@pytest.mark.asyncio
@patch('storage.storage_manager.DeltaTable')
async def test_get_range_delta_not_found(
    MockDeltaTable: MagicMock,
    storage_manager: StorageManagerImpl,
    mock_backend: AsyncMock,
    sample_context: dict
):
    """Test getting range when Delta table doesn't exist."""
    from deltalake.exceptions import TableNotFoundError
    MockDeltaTable.side_effect = TableNotFoundError("Table not found")

    start_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 15, 23, 59, 59, tzinfo=timezone.utc)

    result_df = await storage_manager.get_range(sample_context, start_time, end_time, output_format="dataframe")

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty

# --- list_coins ---

@pytest.mark.asyncio
async def test_list_coins_success(storage_manager: StorageManagerImpl, mock_backend: AsyncMock):
    """Test listing coins successfully."""
    exchange = "TestEx"
    data_type = "candles"
    prefix = f"{data_type}/{exchange.lower()}/"
    mock_backend.list_items.return_value = [
        f"{prefix}BTC_USDT/data.parquet",
        f"{prefix}ETH_BTC/_delta_log/000.json",
        f"{prefix}ETH_BTC/part-0.parquet",
        f"{prefix}ADA_USDT/", # Directory marker style
        f"{prefix}XRP_USD/some_other_file.txt",
        f"{prefix}XRP_USD/data.jsonl",
    ]

    coins = await storage_manager.list_coins(exchange_name=exchange, data_type=data_type)

    mock_backend.list_items.assert_awaited_once_with(prefix)
    assert sorted(coins) == ["ADA_USDT", "BTC_USDT", "ETH_BTC", "XRP_USD"]

@pytest.mark.asyncio
async def test_list_coins_empty(storage_manager: StorageManagerImpl, mock_backend: AsyncMock):
    """Test listing coins when none exist."""
    mock_backend.list_items.return_value = []
    coins = await storage_manager.list_coins(exchange_name="empty", data_type="data")
    assert coins == []

# --- check_coin_exists ---

@pytest.mark.asyncio
async def test_check_coin_exists_true(storage_manager: StorageManagerImpl, mock_backend: AsyncMock):
    """Test checking existence when coin data exists."""
    context = {'data_type': 'ticks', 'exchange': 'kraken', 'coin': 'DOT_EUR'}
    prefix = storage_manager._generate_base_path(context) + '/'
    mock_backend.list_items.return_value = [f"{prefix}file.parquet"] # Simulate finding items

    exists = await storage_manager.check_coin_exists(
        exchange_name=context['exchange'],
        coin_symbol=context['coin'],
        data_type=context['data_type']
    )

    mock_backend.list_items.assert_awaited_once_with(prefix)
    assert exists is True

@pytest.mark.asyncio
async def test_check_coin_exists_false(storage_manager: StorageManagerImpl, mock_backend: AsyncMock):
    """Test checking existence when coin data does not exist."""
    context = {'data_type': 'ticks', 'exchange': 'kraken', 'coin': 'NONEXISTENT'}
    prefix = storage_manager._generate_base_path(context) + '/'
    mock_backend.list_items.return_value = [] # Simulate not finding items

    exists = await storage_manager.check_coin_exists(
        exchange_name=context['exchange'],
        coin_symbol=context['coin'],
        data_type=context['data_type']
    )

    mock_backend.list_items.assert_awaited_once_with(prefix)
    assert exists is False

