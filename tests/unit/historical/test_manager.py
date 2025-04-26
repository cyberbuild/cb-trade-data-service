import pytest
from unittest.mock import MagicMock
from historical.manager import HistoricalDataManagerImpl

class DummyWS:
    def __init__(self):
        self.sent = []
    def send_json(self, msg):
        self.sent.append(msg)

class DummyStorageManager:
    def __init__(self, data=None, latest=None):
        self.data = data or []
        self.latest = latest
    def get_raw_data_range(self, exchange_name, coin_symbol, start_time, end_time, limit=None, offset=None):
        return self.data
    def get_latest_entry(self, exchange, coin):
        return self.latest

def test_stream_historical_data_sends_chunks_and_complete():
    ws = DummyWS()
    storage = DummyStorageManager(data=[{"ts": 1}, {"ts": 2}])
    mgr = HistoricalDataManagerImpl(storage)
    mgr.stream_historical_data("BTC", "binance", 1, 2, ws)
    assert ws.sent[0]["type"] == "historical_data_chunk"
    assert ws.sent[1]["type"] == "historical_data_chunk"
    assert ws.sent[-1]["type"] == "historical_data_complete"
    assert ws.sent[-1]["coin"] == "BTC"
    assert ws.sent[-1]["exchange"] == "binance"

def test_get_most_current_data_returns_latest():
    storage = DummyStorageManager(latest={"ts": 123, "price": 100})
    mgr = HistoricalDataManagerImpl(storage)
    result = mgr.get_most_current_data("BTC", "binance")
    assert result == {"ts": 123, "price": 100}

def test_stream_historical_data_empty():
    ws = DummyWS()
    storage = DummyStorageManager(data=[])
    mgr = HistoricalDataManagerImpl(storage)
    mgr.stream_historical_data("ETH", "binance", 1, 2, ws)
    assert ws.sent[-1]["type"] == "historical_data_complete"
    assert len(ws.sent) == 1
