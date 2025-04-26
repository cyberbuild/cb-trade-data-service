"""
IDataCollectionManager interface for managing real-time data collection.
"""
from abc import ABC, abstractmethod
from typing import Any

class IDataCollectionManager(ABC):
    @abstractmethod
    def add_coin(self, exchange_name: str, coin_symbol: str) -> bool:
        """
        Add a coin for real-time data collection from a specific exchange.
        Returns True if successfully added, False otherwise.
        """
        pass

    @abstractmethod
    def stream_realtime_data(self, exchange_name: str, coin_symbol: str, ws: Any):
        """
        Stream real-time data for a coin to a websocket or similar sink.
        """
        pass

    @abstractmethod
    def check_availability(self, exchange_name: str, coin_symbol: str) -> bool:
        """
        Check if a coin is available for collection on a given exchange.
        """
        pass
