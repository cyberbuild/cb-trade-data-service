from historical.fetcher import HistoricalFetcher
from historical.current_fetcher import CurrentDataFetcher
from historical.interfaces import IHistoricalDataManager
from storage.data_container import ExchangeData
import asyncio
import inspect
from typing import Dict, Any, Optional

class HistoricalDataManagerImpl(IHistoricalDataManager):
    def __init__(self, storage_manager):
        self._fetcher = HistoricalFetcher(storage_manager)
        self._current_fetcher = CurrentDataFetcher(storage_manager)

    async def _stream_historical_data_async(self, context: Dict[str, Any], start, end, ws, chunk_size=1000):
        """
        Async method to stream historical data in chunks over the websocket.
        """
        offset = 0
        sent_any = False
        # Extract coin/exchange for logging/messaging if needed, but pass context to fetcher
        coin = context.get('coin', 'UNKNOWN')
        exchange = context.get('exchange', 'UNKNOWN')
        
        while True:
            try:
                # Fetch data as ExchangeData
                exchange_data = await self._fetcher.fetch_data(
                    context=context,
                    start_time=start,
                    end_time=end,
                    limit=chunk_size,
                    offset=offset,
                    output_format="dict"  # Stream expects dicts
                )
                
                # Extract data from ExchangeData container
                data_chunk = exchange_data.data
                
                # Add metadata to outgoing message
                metadata = exchange_data.metadata
                
            except Exception as e:
                await ws.send_json({"type": "historical_data_error", "error": str(e), "context": context})
                break
                
            if not data_chunk:
                break

            try:
                # Send the whole chunk list at once if ws supports it, or iterate
                await ws.send_json({
                    "type": "historical_data_chunk", 
                    "data": data_chunk, 
                    "metadata": metadata
                })
                sent_any = True
            except Exception as e:
                await ws.send_json({"type": "historical_data_error", "error": str(e), "context": context})
                break

            # If returned data is less than chunk size, we're done
            if len(data_chunk) < chunk_size:
                break
            offset += chunk_size  # Move to next chunk

        if not sent_any:
            await ws.send_json({"type": "historical_data_empty", "context": context})

    def stream_historical_data(self, context: Dict[str, Any], start, end, ws, chunk_size=1000):
        """
        Streams historical data. Runs async version.
        """
        # If called from sync context, run in event loop
        if not inspect.iscoroutinefunction(self.stream_historical_data):
            asyncio.run(self._stream_historical_data_async(context, start, end, ws, chunk_size))
        else:
            # If already in async context, just await the coroutine
            return self._stream_historical_data_async(context, start, end, ws, chunk_size)

    async def _get_most_current_data_async(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Async method to get the most current data entry using context.
        """
        try:
            # Get data as ExchangeData
            exchange_data = await self._current_fetcher.get_latest_entry(context)
            if not exchange_data:
                return None
                
            # Extract and return actual data
            return exchange_data.data
        except Exception as e:
            print(f"Error getting most current data: {e}")
            return None

    def get_most_current_data(self, context: Dict[str, Any]):
        """
        Gets the most current data entry using context. Runs async version.
        """
        # If called from sync context, run in event loop
        if not inspect.iscoroutinefunction(self.get_most_current_data):
            return asyncio.run(self._get_most_current_data_async(context))
        else:
            # If already in async context, just await the coroutine
            return self._get_most_current_data_async(context)
