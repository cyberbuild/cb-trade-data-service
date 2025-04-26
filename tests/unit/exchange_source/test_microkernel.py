import pytest
import asyncio
from exchange_source.interfaces import IExchangeAPIClient
from exchange_source.microkernel import PluginRegistry, DataSourceConnectorImpl

class DummyClient(IExchangeAPIClient):
    def __init__(self, name, available=True, data=None):
        self._name = name
        self._available = available
        self._data = data or ["mockdata"]
    def get_exchange_name(self):
        return self._name
    async def fetch_historical_data(self, coin_symbol, start_time, end_time):
        return self._data
    async def start_realtime_stream(self, coin_symbol, callback):
        await callback({"coin": coin_symbol, "exchange": self._name})
    async def check_coin_availability(self, coin_symbol):
        return self._available

def test_add_and_get_plugin():
    registry = PluginRegistry()
    client = DummyClient("foo")
    registry.add_plugin(client)
    assert registry.get_plugin("foo") is client

def test_add_duplicate_plugin():
    registry = PluginRegistry()
    client1 = DummyClient("foo")
    client2 = DummyClient("foo")
    registry.add_plugin(client1)
    with pytest.raises(ValueError):
        registry.add_plugin(client2)

def test_get_nonexistent_plugin():
    registry = PluginRegistry()
    with pytest.raises(KeyError):
        registry.get_plugin("bar")

def test_add_multiple_plugins():
    registry = PluginRegistry()
    client1 = DummyClient("foo")
    client2 = DummyClient("bar")
    registry.add_plugin(client1)
    registry.add_plugin(client2)
    assert set(registry.list_plugins()) == {"foo", "bar"}

@pytest.mark.asyncio
async def test_connector_get_client():
    registry = PluginRegistry()
    client = DummyClient("foo")
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry)
    assert connector.get_client("foo") is client

@pytest.mark.asyncio
async def test_connector_check_coin_availability():
    registry = PluginRegistry()
    client = DummyClient("foo", available=True)
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry)
    result = await connector.check_coin_availability("foo", "BTC")
    assert result is True

@pytest.mark.asyncio
async def test_connector_fetch_historical_data():
    registry = PluginRegistry()
    client = DummyClient("foo", data=[{"price": 1}])
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry)
    data = await connector.fetch_historical_data("foo", "BTC", 0, 1)
    assert data == [{"price": 1}]

@pytest.mark.asyncio
async def test_connector_start_realtime_stream():
    registry = PluginRegistry()
    client = DummyClient("foo")
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry)
    result = {}
    async def cb(entry):
        result["entry"] = entry
    await connector.start_realtime_stream("foo", "BTC", cb)
    assert result["entry"]["coin"] == "BTC"
    assert result["entry"]["exchange"] == "foo"

@pytest.mark.asyncio
async def test_connector_get_client_not_found():
    registry = PluginRegistry()
    connector = DataSourceConnectorImpl(registry)
    with pytest.raises(KeyError):
        connector.get_client("notfound")
