from historical.fetcher import HistoricalFetcher
from historical.interfaces import IHistoricalDataManager
from exchange_source.models import ExchangeData, Metadata
from storage.paging import Paging
from typing import Dict, Any, Optional
from datetime import datetime

class HistoricalDataManagerImpl(IHistoricalDataManager):
    def __init__(self, storage_manager):
        self._storage_manager = storage_manager
        self._fetcher = HistoricalFetcher(storage_manager)

    async def get_historical_data(self, metadata: Metadata, start: Optional[datetime] = None, end: Optional[datetime] = None, paging: Optional[Paging] = None) -> ExchangeData:
        """
        Get historical data with optional pagination support.
        """
        try:
            # Default to all records if no paging specified
            if paging is None:
                paging = Paging.all_records()
            
            exchange_data = await self._fetcher.fetch_data(
                metadata=metadata,
                start_time=start,
                end_time=end,
                limit=paging.limit,
                offset=paging.offset,
                output_format="dict"
            )
            return exchange_data
        except Exception as e:
            print(f"Error fetching historical data: {e}")
            # Return empty ExchangeData on error
            return ExchangeData(data=[], metadata=metadata)

    async def get_most_current_data(self, metadata: Metadata):
        """
        Get the most current data entry using metadata.
        """
        try:
            result = await self._fetcher.get_latest_entry(metadata)
            return result
        except Exception as e:
            print(f"Error getting most current data: {e}")
            return None

    async def save_data(self, context: dict, exchange_data: ExchangeData):
        """
        Save data to storage.
        """
        try:
            await self._storage_manager.save_data(context, exchange_data)
        except Exception as e:
            print(f"Error saving data: {e}")
            raise
