from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from exchange_source.models import IExchangeRecord, ExchangeData
from enum import Enum, auto

class Interval(Enum):
    MINUTE = '1m'
    FIVEMINUTES = '5m'
    HOUR = '1h'
    DAY = '1d' 
    MONTH = '1mo'
    YEAR = '1y'
    
    @classmethod
    def from_string(cls, interval_str: str) -> 'Interval':
        """Convert a string to an Interval enum value"""
        for interval in cls:
            if interval.value == interval_str:
                return interval
        raise ValueError(f"Invalid interval: {interval_str}")
    
    def to_timedelta(self) -> timedelta:
        """Convert the interval to a timedelta object"""
        intervals_map = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '1h': timedelta(hours=1),
            '1d': timedelta(days=1),
            '1mo': timedelta(days=30),
            '1y': timedelta(days=365),
        }
        return intervals_map[self.value]
    
class IExchangeDataService(ABC):
    @abstractmethod
    async def get_ohlcv_data(self, symbol: str, interval: Interval, start: Optional[datetime] = None, end: Optional[datetime] = None) -> ExchangeData:
        pass
    
    @abstractmethod
    async def sync_with_exchange(self, symbol: str, interval: Interval) -> 'IExchangeDataService':
        pass