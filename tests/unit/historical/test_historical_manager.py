import pytest
from unittest.mock import MagicMock
from historical.manager import HistoricalDataManagerImpl

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

class DummyStorageManager:
    def __init__(self, data=None, latest=None):
        self.data = data or []
        self.latest = latest
        self.calls = []
    def get_range(self, exchange_name, coin_symbol, start_time, end_time, limit=None, offset=None):
        self.calls.append((exchange_name, coin_symbol, start_time, end_time, limit, offset))
        return self.data[offset:offset+limit] if self.data else []
    def get_latest_entry(self, exchange, coin):
        if hasattr(self, 'raise_on_latest') and self.raise_on_latest:
            raise Exception('fail')
        return self.latest

def test_stream_historical_data_sends_chunks_and_complete():
    ws = DummyWS()
    storage = DummyStorageManager(data=[{"ts": 1}, {"ts": 2}])
    mgr = HistoricalDataManagerImpl(storage)
    mgr.stream_historical_data("BTC", "binance", 1, 2, ws, chunk_size=1)
    # Should send two chunks and one complete
    chunk_types = [m["type"] for m in ws.sent]
    assert chunk_types.count("historical_data_chunk") == 2
    assert chunk_types[-1] == "historical_data_complete"
    assert ws.sent[-1]["coin"] == "BTC"
    assert ws.sent[-1]["exchange"] == "binance"

def test_stream_historical_data_websocket_error():
    ws = DummyWS(fail_on={2})  # Fail on second send
    storage = DummyStorageManager(data=[{"ts": 1}, {"ts": 2}])
    mgr = HistoricalDataManagerImpl(storage)
    mgr.stream_historical_data("BTC", "binance", 1, 2, ws, chunk_size=1)
    # Should send one chunk, then error, then stop
    types = [m["type"] for m in ws.sent]
    assert "historical_data_chunk" in types
    assert "historical_data_error" in types
    # Should not send complete after error
    assert types[-1] == "historical_data_error"

def test_get_most_current_data_returns_latest():
    storage = DummyStorageManager(latest={"ts": 123, "price": 100})
    mgr = HistoricalDataManagerImpl(storage)
    result = mgr.get_most_current_data("BTC", "binance")
    assert result == {"ts": 123, "price": 100}

def test_get_most_current_data_handles_exception():
    storage = DummyStorageManager()
    storage.raise_on_latest = True
    mgr = HistoricalDataManagerImpl(storage)
    result = mgr.get_most_current_data("BTC", "binance")
    assert result is None

def test_stream_historical_data_empty():
    ws = DummyWS()
    storage = DummyStorageManager(data=[])
    mgr = HistoricalDataManagerImpl(storage)
    mgr.stream_historical_data("ETH", "binance", 1, 2, ws)
    assert ws.sent[-1]["type"] == "historical_data_complete"
    assert len(ws.sent) == 1
