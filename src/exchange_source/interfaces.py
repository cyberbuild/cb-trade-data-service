from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator

class IExchangeAPIClient(ABC):
    @abstractmethod
    def get_exchange_name(self) -> str:
        """Return the name of the exchange."""
        pass

    @abstractmethod
    async def fetch_historical_data(self, coin_symbol: str, start_time: Any, end_time: Any) -> list:
        """Fetch historical data for a coin between start_time and end_time."""
        pass

    @abstractmethod
    async def start_realtime_stream(self, coin_symbol: str, callback: Any) -> None:
        """Start a real-time data stream for a coin, calling callback for each new entry."""
        pass

    @abstractmethod
    async def check_coin_availability(self, coin_symbol: str) -> bool:
        """Check if a coin is available on the exchange."""
        pass

class IDataSourceConnector(ABC):
    @abstractmethod
    def get_client(self, exchange_name: str) -> IExchangeAPIClient:
        """Retrieve the registered exchange client by name."""
        pass

    @abstractmethod
    async def check_coin_availability(self, exchange_name: str, coin_symbol: str) -> bool:
        """Delegate to the exchange client to check coin availability."""
        pass

    @abstractmethod
    async def fetch_historical_data(self, exchange_name: str, coin_symbol: str, start_time: Any, end_time: Any) -> list:
        """Delegate to the exchange client to fetch historical data."""
        pass

    @abstractmethod
    async def start_realtime_stream(self, exchange_name: str, coin_symbol: str, callback: Any) -> None:
        """Delegate to the exchange client to start a real-time stream."""
        pass
