import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Dict, get_args, Literal
from datetime import datetime, timedelta
from enum import Enum, auto
from exchange_source.models import IExchangeRecord, ExchangeData
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from .interface import IExchangeDataService, Interval

from config import Settings

from historical.manager import IHistoricalDataManager
from exchange_source.clients.iexchange_api_client import IExchangeAPIClient


class ExchangeDataService(IExchangeDataService):
    def __init__(self, exchange_client: IExchangeAPIClient, historical_manager: IHistoricalDataManager):
        self.historical_manager = historical_manager
        self.exchange_client = exchange_client

    async def get_ohlcv_data(self, symbol: str, interval: Interval, start: Optional[datetime] = None, end: Optional[datetime] = None) -> ExchangeData:
        """
        Internal implementation with strong typing for interval parameter
        """
        await self.sync_with_exchange(symbol, interval)

        # Create metadata using dynamic values from components
        from exchange_source.models import Metadata
        metadata = Metadata({
            'data_type': 'ohlcv',
            'exchange': self.exchange_client.get_exchange_name(),
            'coin': symbol,
            'interval': interval.value
        })
        
        # Use the fetcher from the manager to get data
        return await self.historical_manager._fetcher.fetch_data(
            metadata=metadata,
            start_time=start,
            end_time=end
        )
        
        
    async def sync_with_exchange(self, symbol: str, interval: Interval) -> 'IExchangeDataService':
      
        try:
            # Get the latest entry from historical storage - use string value for API compatibility
            latest_entry = await self.historical_manager.get_most_current_data(symbol, interval.value)
            
            now = datetime.now()
            # Get the timedelta directly from the interval enum
            interval_delta = interval.to_timedelta()
            
            # Determine if we need to fetch new data
            if latest_entry is None:
                # No data exists, fetch from a reasonable start time
                start_time = now - timedelta(days=30)  # Default to last 30 days
                need_sync = True
            else:
                # Check if the latest entry is recent enough based on interval
                latest_timestamp = latest_entry.get('timestamp')
                if isinstance(latest_timestamp, int):
                    # Convert from milliseconds to datetime
                    latest_datetime = datetime.fromtimestamp(latest_timestamp / 1000)
                else:
                    # Assume it's already a datetime
                    latest_datetime = latest_timestamp
                
                # Calculate time since last update
                time_since_last = now - latest_datetime
                
                # Only sync if we're behind by more than one interval
                need_sync = time_since_last > interval_delta
                start_time = latest_datetime
            
            # If sync is needed, fetch data from exchange
            if need_sync:
                # Create metadata context
                context = {
                    'exchange': self.exchange_client.get_exchange_name(),
                    'symbol': symbol,
                    'interval': interval.value  # Pass the string value to the context
                }
                
                # Fetch data from the exchange
                exchange_data = await self.exchange_client.fetch_ohlcv_data(
                    coin_symbol=symbol,
                    start_time=start_time,
                    end_time=now,
                    interval=interval.value  # Pass the string value to the client
                )
                
                # Save to historical data service
                if exchange_data and exchange_data.data:
                    await self.historical_manager.save_data(context, exchange_data)
        except Exception as e:
            # Log the error but don't crash
            print(f"Error syncing data for {symbol} {interval.value}: {e}")
        
        return self
