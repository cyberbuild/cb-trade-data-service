from typing import Dict, List # Add List
import logging # Add logging
import asyncio # Add asyncio

from .interfaces import IExchangeAPIClient, IDataSourceConnector
# Use absolute import from package root
from config import Settings

logger = logging.getLogger(__name__)

class PluginRegistry:
    def __init__(self):
        self._plugins: Dict[str, IExchangeAPIClient] = {}

    def add_plugin(self, plugin: IExchangeAPIClient):
        name = plugin.get_exchange_name()
        if name in self._plugins:
            # Log warning instead of raising error? Or ensure cleanup on error?
            logger.warning(f"Plugin with name '{name}' already exists. Overwriting.")
            # Consider closing the existing plugin before overwriting
            # existing_plugin = self._plugins[name]
            # asyncio.create_task(existing_plugin.close()) # Close in background
        self._plugins[name] = plugin
        logger.info(f"Registered plugin: {name}")

    def get_plugin(self, name: str) -> IExchangeAPIClient:
        if name not in self._plugins:
            logger.error(f"Plugin with name '{name}' not found.")
            raise KeyError(f"Plugin with name '{name}' not found.")
        return self._plugins[name]

    def list_plugins(self) -> List[str]: # Add return type hint
        return list(self._plugins.keys())

    async def close_all(self): # Add async close method
        """Closes all registered plugins."""
        logger.info("Closing all registered exchange plugins...")
        tasks = [plugin.close() for plugin in self._plugins.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for name, result in zip(self._plugins.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Error closing plugin {name}: {result}")
        self._plugins.clear()
        logger.info("All exchange plugins closed.")


class DataSourceConnectorImpl(IDataSourceConnector):
    # Accept Settings object to pass relevant config to plugins
    def __init__(self, plugin_registry: PluginRegistry, settings: Settings):
        self._registry = plugin_registry
        self._settings = settings # Store settings
        # Plugin instantiation might happen here or externally
        # If here, iterate through desired exchanges and create/add plugins
        # Example (needs refinement based on actual plugin classes and config):
        # self._initialize_plugins()

    # Example initialization (adapt as needed)
    # def _initialize_plugins(self):
    #     from .plugins.ccxt_exchange import CCXTExchangeClient
    #     # Example: Instantiate CCXT client based on default exchange in config
    #     if self._settings.ccxt.default_exchange:
    #         try:
    #             ccxt_client = CCXTExchangeClient(
    #                 config=self._settings.ccxt,
    #                 exchange_id=self._settings.ccxt.default_exchange
    #                 # Pass API keys from config if needed
    #             )
    #             self._registry.add_plugin(ccxt_client)
    #         except Exception as e:
    #             logger.error(f"Failed to initialize CCXT plugin for {self._settings.ccxt.default_exchange}: {e}")
        # Add logic for other plugin types or discovery

    # Make methods async
    async def get_client(self, exchange_name: str) -> IExchangeAPIClient:
        # This method itself doesn't need to be async unless plugin retrieval is async
        try:
            return self._registry.get_plugin(exchange_name)
        except KeyError:
            logger.error(f"Attempted to get non-existent client: {exchange_name}")
            raise # Re-raise the KeyError

    async def check_coin_availability(self, exchange_name: str, coin_symbol: str) -> bool:
        try:
            client = await self.get_client(exchange_name) # get_client might become async later
            return await client.check_coin_availability(coin_symbol)
        except KeyError:
            return False # Client not found
        except Exception as e:
            logger.error(f"Error checking availability for {coin_symbol} on {exchange_name}: {e}")
            return False

    async def fetch_historical_data(
        self,
        exchange_name: str,
        coin_symbol: str,
        start_time,
        end_time,
        interval: str = '5m' # Add interval
    ) -> list:
        try:
            client = await self.get_client(exchange_name)
            # Pass interval to the client method
            return await client.fetch_historical_data(coin_symbol, start_time, end_time, interval)
        except KeyError:
            return [] # Client not found
        except Exception as e:
            logger.error(f"Error fetching historical data for {coin_symbol} on {exchange_name}: {e}")
            return []

    async def start_realtime_stream(self, exchange_name: str, coin_symbol: str, callback):
        # This needs a proper async implementation in the client first
        try:
            client = await self.get_client(exchange_name)
            # Assuming client.start_realtime_stream is now async
            await client.start_realtime_stream(coin_symbol, callback)
            logger.info(f"Real-time stream started for {coin_symbol} on {exchange_name}")
        except NotImplementedError:
             logger.error(f"Real-time streaming not implemented for client {exchange_name}")
             raise # Re-raise if caller needs to know
        except KeyError:
            logger.error(f"Cannot start stream: Client not found for {exchange_name}")
            # Decide whether to raise or just log
        except Exception as e:
            logger.error(f"Error starting real-time stream for {coin_symbol} on {exchange_name}: {e}")
            # Decide whether to raise or just log

    async def close_all_plugins(self): # Add method to close plugins via registry
        """Closes connections for all registered exchange plugins."""
        await self._registry.close_all()
