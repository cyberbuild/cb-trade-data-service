# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\exchange_source\clients\base.py
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Union, List, Dict
from exchange_source.models import ExchangeData, IExchangeRecord

class IExchangeAPIClient(ABC):
    @abstractmethod
    def get_exchange_name(self) -> str:
        """Return the name of the exchange."""
        pass

    @abstractmethod
    async def fetch_ohlcv_data(self, coin_symbol: str, start_time: Any, end_time: Any,
                              interval: str = '5m') -> 'ExchangeData[IExchangeRecord]': # Use forward reference
        """Fetch OHLCV (Open-High-Low-Close-Volume) data for a coin between start_time and end_time."""
        pass

    @abstractmethod
    async def start_realtime_stream(self, coin_symbol: str, callback: Any) -> None:
        """Start a real-time data stream for a coin, calling callback for each new entry."""
        pass

    @abstractmethod
    async def check_coin_availability(self, coin_symbol: str) -> bool:
        """Check if a coin is available on the exchange."""
        pass

    @abstractmethod
    async def close(self):
        """Close any underlying connections."""
        pass
