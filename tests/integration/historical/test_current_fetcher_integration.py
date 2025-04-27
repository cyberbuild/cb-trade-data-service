import pytest
import pytest_asyncio
import datetime
import dateutil.parser
from historical.current_fetcher import CurrentDataFetcher
from storage.implementations.local_file_storage import LocalFileStorage

@pytest.fixture
def storage_path(tmp_path):
    return tmp_path / "test_data"

@pytest_asyncio.fixture
async def local_storage(storage_path):
    storage_path.mkdir(exist_ok=True)
    from config import StorageConfig
    config = StorageConfig(
        type='local',
        local_root_path=str(storage_path)
    )
    return LocalFileStorage(config)

@pytest.fixture
def fetcher(local_storage):
    return CurrentDataFetcher(local_storage)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_none(fetcher):
    """Test get_latest_entry returns None when no data exists"""
    result = await fetcher.get_latest_entry("BTC", "binance")
    assert result is None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_with_data(fetcher, local_storage):
    """Test get_latest_entry returns the latest entry when data exists"""
    now = datetime.datetime.now(datetime.timezone.utc)
    earlier = now - datetime.timedelta(minutes=5)
    data1 = {"timestamp": int(earlier.timestamp() * 1000), "open": 1}
    data2 = {"timestamp": int(now.timestamp() * 1000), "open": 2}
    await local_storage.save_entry("binance", "BTC", earlier, data1)
    await local_storage.save_entry("binance", "BTC", now, data2)
    result = await fetcher.get_latest_entry("BTC", "binance")
    assert result is not None
    result_ts = dateutil.parser.isoparse(result["timestamp"])
    assert abs((result_ts - now).total_seconds()) < 2
    assert result["data"]["open"] == 2

@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_get_latest_entry_other_coin(fetcher, local_storage):
    """Test get_latest_entry returns None for a coin with no data, even if other coins exist"""
    now = datetime.datetime.now(datetime.timezone.utc)
    data = {"timestamp": int(now.timestamp() * 1000), "open": 1}
    await local_storage.save_entry("binance", "ETH", now, data)
    result = await fetcher.get_latest_entry("BTC", "binance")
    assert result is None
