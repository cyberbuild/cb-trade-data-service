from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime, timedelta

from exchange_source.models import ExchangeData
from storage.paging import Paging
from enum import Enum


class Interval(Enum):
    MINUTE = "1m"
    FIVEMINUTES = "5m"
    HOUR = "1h"
    DAY = "1d"
    MONTH = "1mo"
    YEAR = "1y"

    @classmethod
    def from_string(cls, interval_str: str) -> "Interval":
        """Convert a string to an Interval enum value"""
        for interval in cls:
            if interval.value == interval_str:
                return interval
        raise ValueError(f"Invalid interval: {interval_str}")

    def to_timedelta(self) -> timedelta:
        """Convert the interval to a timedelta object"""
        intervals_map = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
            "1mo": timedelta(days=30),
            "1y": timedelta(days=365),
        }
        return intervals_map[self.value]


class IExchangeDataService(ABC):
    @abstractmethod
    async def get_ohlcv_data(
        self,
        symbol: str,
        interval: Interval,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        paging: Optional[Paging] = None,
    ) -> ExchangeData:
        """Get OHLCV data with optional pagination support"""
        pass

    @abstractmethod
    async def sync_with_exchange(
        self, symbol: str, interval: Interval
    ) -> "IExchangeDataService":
        pass
