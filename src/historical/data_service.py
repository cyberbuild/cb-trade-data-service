from abc import ABC, abstractmethod
from typing import Dict, Any, List, Union, Optional
import pandas as pd
import pyarrow as pa
from datetime import datetime

from storage.interfaces import IStorageManager
from storage.data_container import ExchangeData
from storage.strategy_factory import PathStrategyFactory

class IHistoricalDataService(ABC):
    @abstractmethod
    async def fetch_historical_data(self, context: Dict[str, Any], start_time: datetime, 
                                   end_time: datetime, **kwargs) -> ExchangeData:
        """Fetch historical data for a given context and time range, returning an ExchangeData container"""
        pass

class HistoricalDataService(IHistoricalDataService):
    def __init__(self, storage_manager: IStorageManager):
        self._storage_manager = storage_manager
        
    async def fetch_historical_data(self, context: Dict[str, Any], start_time: datetime, 
                                  end_time: datetime, **kwargs) -> ExchangeData:
        """
        Fetch historical data as an ExchangeData container to maintain metadata separation
        """
        # Get the data using storage manager
        result_data = await self._storage_manager.get_range(
            context=context,
            start_time=start_time,
            end_time=end_time,
            **kwargs
        )
        
        # Create and return ExchangeData with metadata
        return ExchangeData(data=result_data, metadata=context.copy())
