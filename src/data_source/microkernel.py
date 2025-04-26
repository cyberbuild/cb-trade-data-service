from typing import Dict
from src.data_source.interfaces import IExchangeAPIClient, IDataSourceConnector

class PluginRegistry:
    def __init__(self):
        self._plugins: Dict[str, IExchangeAPIClient] = {}

    def add_plugin(self, plugin: IExchangeAPIClient):
        name = plugin.get_exchange_name()
        if name in self._plugins:
            raise ValueError(f"Plugin with name '{name}' already exists.")
        self._plugins[name] = plugin

    def get_plugin(self, name: str) -> IExchangeAPIClient:
        if name not in self._plugins:
            raise KeyError(f"Plugin with name '{name}' not found.")
        return self._plugins[name]

    def list_plugins(self):
        return list(self._plugins.keys())

class DataSourceConnectorImpl(IDataSourceConnector):
    def __init__(self, plugin_registry: PluginRegistry):
        self._registry = plugin_registry

    def get_client(self, exchange_name: str):
        return self._registry.get_plugin(exchange_name)

    async def check_coin_availability(self, exchange_name: str, coin_symbol: str) -> bool:
        client = self.get_client(exchange_name)
        return await client.check_coin_availability(coin_symbol)

    async def fetch_historical_data(self, exchange_name: str, coin_symbol: str, start_time, end_time) -> list:
        client = self.get_client(exchange_name)
        return await client.fetch_historical_data(coin_symbol, start_time, end_time)

    async def start_realtime_stream(self, exchange_name: str, coin_symbol: str, callback):
        client = self.get_client(exchange_name)
        await client.start_realtime_stream(coin_symbol, callback)
