import pytest
import datetime
from historical.manager import HistoricalDataManagerImpl
from storage.implementations.local_file_storage import LocalFileStorage
from storage.path_utility import StoragePathUtility

class DummyWS:
    def __init__(self, fail_on=None):
        self.sent = []
        self.fail_on = fail_on or set()
        self.send_count = 0
    def send_json(self, msg):
        self.send_count += 1
        if self.send_count in self.fail_on:
            raise Exception("WebSocket send error")
        self.sent.append(msg)

@pytest.fixture
def storage_path(tmp_path):
    return tmp_path / "test_data"

@pytest.fixture
async def local_storage(storage_path):
    storage_path.mkdir(exist_ok=True)
    from config import StorageConfig
    config = StorageConfig(
        type='local',
        local_root_path=str(storage_path)
    )
    return LocalFileStorage(config)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_historical_data_integration(local_storage):
    # Store 3 entries
    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(3):
        ts = now + datetime.timedelta(minutes=i)
        data = {"timestamp": int(ts.timestamp() * 1000), "open": 100 + i}
        await local_storage.save_entry("binance", "BTC", ts, data)
    ws = DummyWS()
    mgr = HistoricalDataManagerImpl(local_storage)
    await mgr.stream_historical_data("BTC", "binance", now, now + datetime.timedelta(minutes=2), ws, chunk_size=2)
    # Should send 2 chunks (2+1) and a complete
    chunk_types = [m["type"] for m in ws.sent]
    assert chunk_types.count("historical_data_chunk") == 3
    assert chunk_types[-1] == "historical_data_complete"

@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_historical_data_websocket_error_integration(local_storage):
    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(2):
        ts = now + datetime.timedelta(minutes=i)
        data = {"timestamp": int(ts.timestamp() * 1000), "open": 100 + i}
        await local_storage.save_entry("binance", "BTC", ts, data)
    ws = DummyWS(fail_on={2})
    mgr = HistoricalDataManagerImpl(local_storage)
    await mgr.stream_historical_data("BTC", "binance", now, now + datetime.timedelta(minutes=1), ws, chunk_size=1)
    types = [m["type"] for m in ws.sent]
    assert "historical_data_chunk" in types
    assert "historical_data_error" in types
    assert types[-1] == "historical_data_error"
