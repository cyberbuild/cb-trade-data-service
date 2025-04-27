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
    async def fetch_historical_data(self, *args, **kwargs):
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

def test_add_duplicate_plugin(caplog):
    registry = PluginRegistry()
    client1 = DummyClient("foo")
    client2 = DummyClient("foo")
    registry.add_plugin(client1)
    with caplog.at_level('WARNING'):
        registry.add_plugin(client2)
        assert any("already exists. Overwriting." in m for m in caplog.messages)

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
    from config import get_settings
    registry = PluginRegistry()
    client = DummyClient("foo")
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry, get_settings())
    result = await connector.get_client("foo")
    assert result is client

@pytest.mark.asyncio
async def test_connector_check_coin_availability():
    from config import get_settings
    registry = PluginRegistry()
    client = DummyClient("foo", available=True)
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry, get_settings())
    result = await connector.check_coin_availability("foo", "BTC")
    assert result is True

@pytest.mark.asyncio
async def test_connector_fetch_historical_data():
    from config import get_settings
    registry = PluginRegistry()
    client = DummyClient("foo", data=[{"price": 1}])
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry, get_settings())
    result = await connector.fetch_historical_data("foo", "BTC", None, None)
    assert result == [{"price": 1}]

@pytest.mark.asyncio
async def test_connector_start_realtime_stream():
    from config import get_settings
    registry = PluginRegistry()
    client = DummyClient("foo")
    registry.add_plugin(client)
    connector = DataSourceConnectorImpl(registry, get_settings())
    called = {}
    async def cb(data):
        called["data"] = data
    await connector.start_realtime_stream("foo", "BTC", cb)
    assert called["data"]["coin"] == "BTC"
    assert called["data"]["exchange"] == "foo"

@pytest.mark.asyncio
async def test_connector_get_client_not_found():
    from config import get_settings
    registry = PluginRegistry()
    connector = DataSourceConnectorImpl(registry, get_settings())
    with pytest.raises(KeyError):
        await connector.get_client("bar")
