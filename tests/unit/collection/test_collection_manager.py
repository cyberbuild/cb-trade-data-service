import pytest
from unittest.mock import MagicMock
from collection.manager import DataCollectionManagerImpl
from collection.interfaces import IDataCollectionManager

class DummyClient:
    def __init__(self, available=True):
        self.available = available
        self.started = []
    def check_coin_availability(self, coin):
        return self.available
    def start_realtime_stream(self, coin, callback):
        self.started.append((coin, callback))

class DummyConnector:
    def __init__(self, available=True):
        self.client = DummyClient(available)
    def get_client(self, exchange):
        return self.client

class DummyStorageManager:
    def __init__(self):
        self.stored = []
    def store_raw_data(self, exchange, coin, timestamp, data):
        self.stored.append((exchange, coin, timestamp, data))

@pytest.fixture
def manager():
    connector = DummyConnector()
    storage = DummyStorageManager()
    return DataCollectionManagerImpl(connector, storage), connector, storage

def test_add_coin_success(manager):
    mgr, connector, storage = manager
    result = mgr.add_coin('binance', 'BTC')
    assert result is True
    assert connector.client.started[0][0] == 'BTC'

def test_add_coin_unavailable(manager):
    connector = DummyConnector(available=False)
    storage = DummyStorageManager()
    mgr = DataCollectionManagerImpl(connector, storage)
    result = mgr.add_coin('binance', 'DOGE')
    assert result is False
    assert connector.client.started == []

def test_add_coin_already_added(manager):
    mgr, connector, storage = manager
    mgr.add_coin('binance', 'BTC')
    result = mgr.add_coin('binance', 'BTC')
    assert result is True
    assert len(connector.client.started) == 1

def test_check_availability(manager):
    mgr, connector, storage = manager
    assert mgr.check_availability('binance', 'BTC') is True
    connector.client.available = False
    assert mgr.check_availability('binance', 'DOGE') is False
