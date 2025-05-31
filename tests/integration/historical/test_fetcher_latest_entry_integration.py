import pytest
import pytest_asyncio
import datetime
import pandas as pd
import logging
from historical.fetcher import HistoricalFetcher
from storage.storage_manager import IStorageManager
from exchange_source.models import Metadata

logger = logging.getLogger(__name__)

@pytest_asyncio.fixture
def fetcher(storage_manager: IStorageManager):
    return HistoricalFetcher(storage_manager)

# Test fetching latest entry when data exists (using the setup fixture)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_with_data(fetcher, setup_historical_data):
    """Test get_latest_entry returns the latest entry when data exists."""
    context = setup_historical_data # Use context from the setup fixture
    metadata = Metadata(context)
    result = await fetcher.get_latest_entry(metadata)

    assert result is not None
    assert isinstance(result, dict)
    assert 'timestamp' in result
    # Add more specific checks if needed, e.g., timestamp is recent
    now = datetime.datetime.now(datetime.timezone.utc)
    # Convert timestamp from milliseconds to datetime
    result_time = pd.to_datetime(result['timestamp'], unit='ms', utc=True)
    # Check if the timestamp is within the last ~1 hour (plus buffer)
    assert now - datetime.timedelta(hours=1, minutes=10) <= result_time <= now + datetime.timedelta(minutes=10)

# Test fetching latest entry when NO data exists for the specific context
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_none(fetcher, storage_manager): # Don't use setup_historical_data here
    """Test get_latest_entry returns None when no data exists for the context."""
    # Use a context that is unlikely to have data from the setup fixture
    context_nodata = {'exchange': 'some_other_exchange', 'coin': 'XYZ/ABC', 'data_type': 'ohlcv_1m'}
    metadata = Metadata(context_nodata)
    result = await fetcher.get_latest_entry(metadata)
    assert result is None

# Test fetching for a different coin after setup (should return None if setup only added BTC/USD)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_other_coin(fetcher, setup_historical_data):
    """Test get_latest_entry returns None for a coin not added by the setup fixture."""
    # setup_historical_data added 'cryptocom', 'BTC/USD', 'ohlcv_1m'
    context_other_coin = {'exchange': 'cryptocom', 'coin': 'ETH/USD', 'data_type': 'ohlcv_1m'}
    metadata = Metadata(context_other_coin)
    result = await fetcher.get_latest_entry(metadata)
    # This assumes the setup fixture ONLY added BTC/USD for cryptocom
    assert result is None

# Test with invalid context (missing keys)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_invalid_context(fetcher):
    """Test get_latest_entry handles invalid context gracefully."""
    invalid_context = {'exchange': 'binance'} # Missing coin and data_type
    metadata = Metadata(invalid_context)
    # Expecting a ValueError or similar, or None depending on implementation
    # Current implementation catches Exception and returns None
    result = await fetcher.get_latest_entry(metadata)
    assert result is None
    # If specific exception handling is desired, adjust the test
    # with pytest.raises(ValueError):
    #     await fetcher.get_latest_entry(invalid_context)
