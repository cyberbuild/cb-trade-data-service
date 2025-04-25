from abc import ABC, abstractmethod
from typing import Any, List

class IRawDataStorage(ABC):
    @abstractmethod
    def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: Any, data: dict) -> None:
        """
        Save a single raw data entry for the given exchange, coin, and timestamp.
        """
        pass

    @abstractmethod
    def get_range(self, exchange_name: str, coin_symbol: str, start_time: Any, end_time: Any, limit: int = 1000, offset: int = 0) -> List[dict]:
        """
        Retrieve entries within a time range for a given exchange and coin.
        """
        pass

    @abstractmethod
    def get_latest_entry(self, exchange_name: str, coin_symbol: str) -> dict:
        """
        Retrieve the single latest entry for a given exchange and coin.
        """
        pass

    @abstractmethod
    def list_coins(self, exchange_name: str) -> List[str]:
        """
        List all available coin symbols for a given exchange.
        """
        pass

    @abstractmethod
    def check_coin_exists(self, exchange_name: str, coin_symbol: str) -> bool:
        """
        Check if a folder for a coin exists for a given exchange.
        """
        pass
