from typing import List, Any, Optional
from storage.storage_manager import IStorageManager

class HistoricalFetcher:
    def __init__(self, storage_manager: IStorageManager):
        self._storage_manager = storage_manager

    def fetch_data(self, coin: str, exchange: str, start_time, end_time, limit=None, offset=None):
        """
        Fetch historical data for a coin and exchange from storage within a time range.
        Returns a list of data entries, or an empty list if none found.
        """
        # Ensure limit and offset are integers if not provided
        if limit is None:
            limit = 1000
        if offset is None:
            offset = 0
        return self._storage_manager.get_range(
            exchange_name=exchange,
            coin_symbol=coin,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset
        )
