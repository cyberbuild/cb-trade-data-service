from abc import ABC, abstractmethod
from exchange_source.models import Metadata, ExchangeData
from storage.paging import Paging
from typing import Optional
from datetime import datetime

class IHistoricalDataManager(ABC):
    @abstractmethod
    async def get_historical_data(self, metadata: Metadata, start: Optional[datetime] = None, end: Optional[datetime] = None, paging: Optional[Paging] = None) -> ExchangeData:
        """Get historical data with optional pagination support"""
        pass

    @abstractmethod
    async def get_most_current_data(self, metadata: Metadata):
        pass

    @abstractmethod
    async def save_data(self, context: dict, exchange_data: ExchangeData):
        pass
