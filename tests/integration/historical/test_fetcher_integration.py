import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from historical.fetcher import HistoricalFetcher
from storage.storage_manager import IStorageManager
from exchange_source.models import Metadata, ExchangeData, Format
import pyarrow as pa

logger = logging.getLogger(__name__)

@pytest_asyncio.fixture
def fetcher(storage_manager: IStorageManager):
    return HistoricalFetcher(storage_manager)

# Test fetching when data exists (using setup fixture)
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_integration_fetch_data_with_stored_data(fetcher, setup_historical_data):
    """Test fetching data within the range populated by the setup fixture."""
    context = setup_historical_data
    metadata = Metadata(context)
    
    # Fetch within the last hour (which setup_historical_data populated)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    result_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=start_time,
        end_time=end_time
    )

    assert isinstance(result_data, ExchangeData)
    assert len(result_data.data) > 0 # Should fetch the data saved by the fixture
    
    # Check if timestamps are within the expected range (approx last hour)
    min_timestamp = min(item.timestamp for item in result_data.data) / 1000  # Convert ms to seconds
    max_timestamp = max(item.timestamp for item in result_data.data) / 1000  # Convert ms to seconds
    min_datetime = datetime.fromtimestamp(min_timestamp, tz=timezone.utc)
    max_datetime = datetime.fromtimestamp(max_timestamp, tz=timezone.utc)
    
    assert min_datetime >= start_time - timedelta(minutes=5) # Allow buffer
    assert max_datetime <= end_time + timedelta(minutes=5)   # Allow buffer
    
    # Expecting around 60 records for 1 hour of 1m data
    assert 50 <= len(result_data.data) <= 70 # Allow for slight variations/timing

# Test fetching when no data exists for the context
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("storage_manager", ["local", "azure"], indirect=True)
async def test_integration_fetch_data_empty_storage(fetcher):
    """Test fetching data returns empty when no data exists for the context."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    context_nodata = {'exchange': 'nonexistent', 'coin': 'NONE/FAKE', 'data_type': 'ohlcv', 'interval': '1m'}
    metadata = Metadata(context_nodata)
    
    result_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=start_time,
        end_time=end_time
    )
        
    assert isinstance(result_data, ExchangeData)
    assert len(result_data.data) == 0

# Test fetching with limit and offset using the setup data
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_integration_fetch_data_with_limit_offset(fetcher, setup_historical_data):
    """Test fetching data with limit and offset parameters using setup data."""
    context = setup_historical_data
    metadata = Metadata(context)
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1) # Range covered by setup
    
    # First, fetch all data to know what to expect
    all_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=start_time,
        end_time=end_time
    )
        
    # Sort data by timestamp for consistent ordering
    sorted_data = sorted(all_data.data, key=lambda x: x.timestamp)
    
    if len(sorted_data) < 3:
        pytest.skip("Not enough data fetched by setup fixture to test limit/offset effectively.")
    
    limit = 2
    offset = 1
    
    result_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset
    )
    
    assert isinstance(result_data, ExchangeData)
    assert len(result_data.data) == limit
    
    # Sort result data for consistent ordering
    sorted_result = sorted(result_data.data, key=lambda x: x.timestamp)
    
    # Verify offset: the first timestamp in result should match the second timestamp in all_data
    expected_ts_1 = sorted_data[offset].timestamp
    expected_ts_2 = sorted_data[offset + 1].timestamp
    result_ts_1 = sorted_result[0].timestamp
    result_ts_2 = sorted_result[1].timestamp
    
    # Compare timestamps (should be exact since we're fetching the same objects)
    assert expected_ts_1 == result_ts_1
    assert expected_ts_2 == result_ts_2

# Test fetching a specific time range within the setup data
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_integration_fetch_data_time_range(fetcher, setup_historical_data, env_logger_func): # Add env_logger_func fixture
    env_logger_func(logger, context_message=f"Inside test_integration_fetch_data_time_range ({setup_historical_data.get('exchange')}) before any operations") # Call via fixture
    """Test fetching data within a specific sub-range of the setup data."""
    context = setup_historical_data
    metadata = Metadata(context)
    now = datetime.now(timezone.utc)

    # Define a smaller range within the last hour (e.g., 15 minutes in the middle)
    fetch_end_time = now - timedelta(minutes=20)
    fetch_start_time = fetch_end_time - timedelta(minutes=15)

    result_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=fetch_start_time,
        end_time=fetch_end_time
    )

    assert isinstance(result_data, ExchangeData)
    # Expecting around 15 records for 15 mins of 1m data
    assert 10 <= len(result_data.data) <= 20 # Allow some buffer

    # Verify timestamps are within the requested sub-range
    if len(result_data.data) > 0:
        min_timestamp = min(item.timestamp for item in result_data.data) / 1000  # Convert ms to seconds
        max_timestamp = max(item.timestamp for item in result_data.data) / 1000  # Convert ms to seconds
        min_datetime = datetime.fromtimestamp(min_timestamp, tz=timezone.utc)
        max_datetime = datetime.fromtimestamp(max_timestamp, tz=timezone.utc)
        
        assert min_datetime >= fetch_start_time - timedelta(seconds=60) # Allow buffer
        assert max_datetime <= fetch_end_time + timedelta(seconds=60)   # Allow buffer

# Test ExchangeData conversion methods with fetcher results
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_integration_exchange_data_conversion(fetcher, setup_historical_data):
    """Test ExchangeData conversion methods with fetcher results."""
    context = setup_historical_data
    metadata = Metadata(context)
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1) # Range covered by setup
    
    result_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=start_time,
        end_time=end_time
    )

    assert isinstance(result_data, ExchangeData)
      # Test to_dataframe method
    df = result_data.to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(result_data.data)
    
    # Test to_arrow method
    table = result_data.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.num_rows == len(result_data.data)

# Test format conversion of fetched data
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_integration_fetch_data_with_format_conversion(fetcher, setup_historical_data):
    """Test converting fetched ExchangeData to different formats."""
    context = setup_historical_data
    metadata = Metadata(context)
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=30)  # Get some data
    
    # Fetch data
    result_data = await fetcher.fetch_data(
        metadata=metadata,
        start_time=start_time,
        end_time=end_time,
        limit=10  # Limit to 10 records for test simplicity
    )
    
    assert isinstance(result_data, ExchangeData)
    
    if len(result_data.data) == 0:
        pytest.skip("No data available for conversion test")
    
    # Test converting to DataFrame
    df = result_data.convert(Format.DATAFRAME)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(result_data.data)
    assert 'timestamp' in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df['timestamp'])
    
    # Test the to_dataframe convenience method
    df2 = result_data.to_dataframe()
    assert isinstance(df2, pd.DataFrame)
    assert len(df2) == len(result_data.data)
    
    # Test converting to PyArrow Table
    table = result_data.convert(Format.ARROW)
    assert isinstance(table, pa.Table)
    assert table.num_rows == len(result_data.data)
    assert 'timestamp' in table.column_names
    
    # Test the to_arrow convenience method
    table2 = result_data.to_arrow()
    assert isinstance(table2, pa.Table)
    assert table2.num_rows == len(result_data.data)
