"""
RealTimeProcessor: Handles processing of real-time data entries, storing them via IStorageManager and preparing for distribution.
"""
from typing import Any
from storage.interfaces import IStorageManager

class RealTimeProcessor:
    def __init__(self, storage_manager: IStorageManager):
        self.storage_manager = storage_manager

    def process_realtime_entry(self, exchange_name: str, coin_symbol: str, timestamp, data: Any):
        """
        Process a real-time data entry: store it and prepare for distribution.
        Args:
            exchange_name (str): Name of the exchange.
            coin_symbol (str): Symbol of the coin.
            timestamp: Timestamp of the data entry (datetime or compatible).
            data (Any): The raw order book data.
        """
        # Store the data using the storage manager
        self.storage_manager.store_raw_data(exchange_name, coin_symbol, timestamp, data)
        # Additional logic for distribution to subscribers would go here (not implemented yet)
