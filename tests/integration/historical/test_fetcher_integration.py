import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from historical.fetcher import HistoricalFetcher
from storage.interfaces import IStorageManager

logger = logging.getLogger(__name__)

@pytest_asyncio.fixture
def fetcher(storage_manager: IStorageManager):
    return HistoricalFetcher(storage_manager)

# Test fetching when data exists (using setup fixture)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_with_stored_data(fetcher, setup_historical_data):
    """Test fetching data within the range populated by the setup fixture."""
    context = setup_historical_data
    # Fetch within the last hour (which setup_historical_data populated)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    result_df = await fetcher.fetch_data(
        context=context,
        start_time=start_time,
        end_time=end_time,
        output_format="dataframe" # Explicitly request DataFrame
    )

    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty # Should fetch the data saved by the fixture
    # Check if timestamps are within the expected range (approx last hour)
    assert result_df['timestamp'].min() >= start_time - timedelta(minutes=5) # Allow buffer
    assert result_df['timestamp'].max() <= end_time + timedelta(minutes=5)   # Allow buffer
    # Expecting around 60 records for 1 hour of 1m data
    assert 50 <= len(result_df) <= 70 # Allow for slight variations/timing

# Test fetching when no data exists for the context
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_empty_storage(fetcher):
    """Test fetching data returns empty when no data exists for the context."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    context_nodata = {'exchange': 'nonexistent', 'coin': 'NONE/FAKE', 'data_type': 'ohlcv_1m'}

    result_df = await fetcher.fetch_data(
        context=context_nodata,
        start_time=start_time,
        end_time=end_time,
        output_format="dataframe"
    )

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty

# Test fetching with limit and offset using the setup data
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_with_limit_offset(fetcher, setup_historical_data):
    """Test fetching data with limit and offset parameters using setup data."""
    context = setup_historical_data
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1) # Range covered by setup

    # First, fetch all data to know what to expect
    all_data_df = await fetcher.fetch_data(
        context=context,
        start_time=start_time,
        end_time=end_time,
        output_format="dataframe"
    )
    all_data_df = all_data_df.sort_values('timestamp').reset_index(drop=True)

    if len(all_data_df) < 3:
        pytest.skip("Not enough data fetched by setup fixture to test limit/offset effectively.")

    limit = 2
    offset = 1

    result_df = await fetcher.fetch_data(
        context=context,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
        output_format="dataframe"
    )
    result_df = result_df.sort_values('timestamp').reset_index(drop=True)

    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == limit

    # Verify offset: the first timestamp in result should match the second timestamp in all_data
    expected_ts_1 = all_data_df['timestamp'].iloc[offset]
    expected_ts_2 = all_data_df['timestamp'].iloc[offset + 1]
    result_ts_1 = result_df['timestamp'].iloc[0]
    result_ts_2 = result_df['timestamp'].iloc[1]

    # Compare timestamps (allow small tolerance if needed, though should be exact)
    assert abs((result_ts_1 - expected_ts_1).total_seconds()) < 1
    assert abs((result_ts_2 - expected_ts_2).total_seconds()) < 1

# Test fetching a specific time range within the setup data
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_time_range(fetcher, setup_historical_data):
    """Test fetching data within a specific sub-range of the setup data."""
    context = setup_historical_data
    now = datetime.now(timezone.utc)

    # Define a smaller range within the last hour (e.g., 15 minutes in the middle)
    fetch_end_time = now - timedelta(minutes=20)
    fetch_start_time = fetch_end_time - timedelta(minutes=15)

    result_df = await fetcher.fetch_data(
        context=context,
        start_time=fetch_start_time,
        end_time=fetch_end_time,
        output_format="dataframe"
    )

    assert isinstance(result_df, pd.DataFrame)
    # Expecting around 15 records for 15 mins of 1m data
    assert 10 <= len(result_df) <= 20 # Allow some buffer

    # Verify timestamps are within the requested sub-range
    if not result_df.empty:
        assert result_df['timestamp'].min() >= fetch_start_time - timedelta(seconds=60) # Allow buffer
        assert result_df['timestamp'].max() <= fetch_end_time + timedelta(seconds=60)   # Allow buffer
