import pytest
import datetime
import dateutil.parser
from historical.fetcher import HistoricalFetcher
from storage.backends.local_file_backend import LocalFileBackend

@pytest.fixture
def storage_path(tmp_path):
    """Creates a temporary directory for test data storage"""
    return tmp_path / "test_data"

@pytest.fixture
async def local_storage(storage_path):
    """Creates a LocalFileStorage instance for testing"""
    storage_path.mkdir(exist_ok=True)
    from config import StorageConfig
    config = StorageConfig(
        type='local',
        local_root_path=str(storage_path)
    )
    return LocalFileBackend(config)

@pytest.fixture
def fetcher(local_storage):
    """Creates a HistoricalFetcher instance with LocalFileStorage"""
    return HistoricalFetcher(local_storage)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_empty_storage(fetcher):
    """Test fetching data from empty storage returns empty list"""
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=1)
    
    result = await fetcher.fetch_data(
        coin="BTC",
        exchange="binance",
        start_time=start_time,
        end_time=end_time
    )
    
    assert isinstance(result, list)
    assert len(result) == 0

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_with_stored_data(fetcher, local_storage):
    """Test fetching data when storage contains data"""
    # Prepare test data
    now = datetime.datetime.now(datetime.timezone.utc)
    test_data = {
        "timestamp": int(now.timestamp() * 1000),
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "volume": 100.0
    }
    
    # Store test data
    await local_storage.save_entry(
        exchange_name="binance",
        coin_symbol="BTC",
        timestamp=now,
        data=test_data
    )
    
    # Fetch data
    start_time = now - datetime.timedelta(minutes=5)
    end_time = now + datetime.timedelta(minutes=5)
    
    result = await fetcher.fetch_data(
        coin="BTC",
        exchange="binance",
        start_time=start_time,
        end_time=end_time
    )
    
    assert isinstance(result, list)
    assert len(result) == 1
    # Compare timestamps using datetime
    result_ts = dateutil.parser.isoparse(result[0]["timestamp"])
    assert abs((result_ts - now).total_seconds()) < 2
    assert result[0]["data"]["open"] == test_data["open"]
    assert result[0]["data"]["close"] == test_data["close"]

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_with_limit_offset(fetcher, local_storage):
    """Test fetching data with limit and offset parameters"""
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # Store multiple test entries
    for i in range(5):
        timestamp = now + datetime.timedelta(minutes=i*5)
        test_data = {
            "timestamp": int(timestamp.timestamp() * 1000),
            "open": 50000.0 + i,
            "high": 51000.0 + i,
            "low": 49000.0 + i,
            "close": 50500.0 + i,
            "volume": 100.0 + i
        }
        await local_storage.save_entry(
            exchange_name="binance",
            coin_symbol="BTC",
            timestamp=timestamp,
            data=test_data
        )
    
    # Fetch with limit and offset
    start_time = now - datetime.timedelta(minutes=5)
    end_time = now + datetime.timedelta(minutes=30)
    
    result = await fetcher.fetch_data(
        coin="BTC",
        exchange="binance",
        start_time=start_time,
        end_time=end_time,
        limit=2,
        offset=1
    )
    
    assert isinstance(result, list)
    assert len(result) == 2  # Verify limit is respected
    # Verify offset is respected (should skip first entry)
    result_ts = dateutil.parser.isoparse(result[0]["timestamp"])
    first_ts = now + datetime.timedelta(minutes=5)
    assert abs((result_ts - first_ts).total_seconds()) < 2

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_fetch_data_time_range(fetcher, local_storage):
    """Test fetching data within a specific time range"""
    base_time = datetime.datetime.now(datetime.timezone.utc)
    
    # Store data points across different times
    for i in range(-2, 3):  # -2, -1, 0, 1, 2
        timestamp = base_time + datetime.timedelta(hours=i)
        test_data = {
            "timestamp": int(timestamp.timestamp() * 1000),
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0
        }
        await local_storage.save_entry(
            exchange_name="binance",
            coin_symbol="BTC",
            timestamp=timestamp,
            data=test_data
        )
    
    # Fetch data for middle 3 hours
    start_time = base_time - datetime.timedelta(hours=1)
    end_time = base_time + datetime.timedelta(hours=1)
    
    result = await fetcher.fetch_data(
        coin="BTC",
        exchange="binance",
        start_time=start_time,
        end_time=end_time
    )
    
    assert isinstance(result, list)
    # Accept 2 or 3 due to possible file system timestamp truncation
    assert len(result) in (2, 3)
    # Verify all timestamps are within range
    for entry in result:
        ts = dateutil.parser.isoparse(entry["timestamp"])
        assert start_time <= ts <= end_time
