import pytest
import asyncio
from unittest.mock import MagicMock
from historical.manager import HistoricalDataManagerImpl

class DummyWS:
    def __init__(self, fail_on=None):
        self.sent = []
        self.fail_on = fail_on or set()
        self.send_count = 0
    async def send_json(self, msg):
        self.send_count += 1
        if self.send_count in self.fail_on:
            raise Exception("WebSocket send error")
        self.sent.append(msg)

class DummyStorageManager:
    def __init__(self, data=None, latest=None):
        self.data = data or []
        self.latest = latest or []
        self.calls = []
    async def get_range(self, context, start_time, end_time, limit=None, offset=None, output_format=None, **kwargs):
        import pandas as pd
        self.calls.append((context, start_time, end_time, limit, offset, output_format))
        # If latest is set and output_format is 'dataframe', return as DataFrame for latest test
        if self.latest and output_format == 'dataframe':
            return pd.DataFrame(self.latest)
        return self.data[offset:offset+limit] if self.data else []
    async def get_latest_entry(self, context):
        return self.latest

@pytest.mark.asyncio
async def test_stream_historical_data_sends_chunks_and_complete():
    ws = DummyWS()
    storage = DummyStorageManager(data=[{"ts": 1}, {"ts": 2}, {"ts": 3}])
    mgr = HistoricalDataManagerImpl(storage)
    context = {'coin': 'BTC', 'exchange': 'binance'}
    await mgr.stream_historical_data(context, 1, 2, ws, chunk_size=1)
    chunk_types = [m["type"] for m in ws.sent]
    assert chunk_types.count("historical_data_chunk") == 3
    assert chunk_types[-1] == "historical_data_complete"
    assert ws.sent[-1]["context"] == context

@pytest.mark.asyncio
async def test_stream_historical_data_websocket_error():
    ws = DummyWS(fail_on={2})  # Fail on second send
    storage = DummyStorageManager(data=[{"ts": 1}, {"ts": 2}])
    mgr = HistoricalDataManagerImpl(storage)
    context = {'coin': 'BTC', 'exchange': 'binance'}
    await mgr.stream_historical_data(context, 1, 2, ws, chunk_size=1)
    types = [m["type"] for m in ws.sent]
    assert "historical_data_chunk" in types
    assert "historical_data_error" in types
    assert types[-1] == "historical_data_error"

@pytest.mark.asyncio
async def test_get_most_current_data_returns_latest():
    storage = DummyStorageManager(latest=[{"timestamp": 1234567890, "ts": 123, "price": 100}])
    mgr = HistoricalDataManagerImpl(storage)
    context = {'coin': 'BTC', 'exchange': 'binance', 'data_type': 'ohlcv_1m'}
    result = await mgr.get_most_current_data(context)
    assert result['timestamp'] == 1234567890
    assert result['ts'] == 123
    assert result['price'] == 100

@pytest.mark.asyncio
async def test_get_most_current_data_handles_exception():
    storage = DummyStorageManager()
    storage.raise_on_latest = True
    mgr = HistoricalDataManagerImpl(storage)
    context = {'coin': 'BTC', 'exchange': 'binance', 'data_type': 'ohlcv_1m'}
    result = await mgr.get_most_current_data(context)
    assert result is None

@pytest.mark.asyncio
async def test_stream_historical_data_empty():
    ws = DummyWS()
    storage = DummyStorageManager(data=[])
    mgr = HistoricalDataManagerImpl(storage)
    context = {'coin': 'ETH', 'exchange': 'binance'}
    await mgr.stream_historical_data(context, 1, 2, ws)
    # No data, so no messages should be sent
    assert len(ws.sent) == 0
