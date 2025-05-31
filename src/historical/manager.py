from historical.fetcher import HistoricalFetcher
from historical.interfaces import IHistoricalDataManager
from exchange_source.models import ExchangeData, Metadata
import asyncio
import inspect
from typing import Dict, Any, Optional

class HistoricalDataManagerImpl(IHistoricalDataManager):
    def __init__(self, storage_manager):
        self._fetcher = HistoricalFetcher(storage_manager)

    async def _stream_historical_data_async(self, metadata: Metadata, start, end, ws, chunk_size=1000):
        """
        Async method to stream historical data in chunks over the websocket.
        """
        offset = 0
        sent_any = False
        
        while True:
            try:
                # Fetch data as ExchangeData - pass metadata directly
                exchange_data = await self._fetcher.fetch_data(
                    metadata=metadata,
                    start_time=start,
                    end_time=end,
                    limit=chunk_size,
                    offset=offset,
                    output_format="dict"  # Stream expects dicts
                )
                
                # Extract data from ExchangeData container
                data_chunk = exchange_data.data
                # Use metadata from response
                response_metadata = exchange_data.metadata
                
            except Exception as e:
                try:
                    await ws.send_json({"type": "historical_data_error", "error": str(e), "metadata": dict(metadata)})
                except Exception:
                    pass
                break
                
            if not data_chunk:
                break

            try:
                # Send the whole chunk list at once
                await ws.send_json({
                    "type": "historical_data_chunk", 
                    "data": data_chunk, 
                    "metadata": dict(response_metadata)
                })
                sent_any = True
            except Exception as e:
                try:
                    await ws.send_json({"type": "historical_data_error", "error": str(e), "metadata": dict(metadata)})
                except Exception:
                    pass
                break

            # If returned data is less than chunk size, we're done
            if len(data_chunk) < chunk_size:
                break
            offset += chunk_size  # Move to next chunk
        
        if not sent_any:
            try:
                await ws.send_json({"type": "historical_data_empty", "metadata": dict(metadata)})
            except Exception:
                pass

    def stream_historical_data(self, metadata: Metadata, start, end, ws, chunk_size=1000):
        """
        Streams historical data. Runs async version.
        """
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, just return the coroutine to be awaited
            return self._stream_historical_data_async(metadata, start, end, ws, chunk_size)
        except RuntimeError:
            # No running loop, safe to use asyncio.run()
            return asyncio.run(self._stream_historical_data_async(metadata, start, end, ws, chunk_size))

    async def _get_most_current_data_async(self, metadata: Metadata):
        """
        Async method to get the most current data entry using metadata.
        """
        try:
            # Get latest entry from the historical fetcher
            result = await self._fetcher.get_latest_entry(metadata)
            return result
        except Exception as e:
            print(f"Error getting most current data: {e}")
            return None

    def get_most_current_data(self, metadata: Metadata):
        """
        Gets the most current data entry using metadata. Runs async version.
        """
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, create a task
            return asyncio.create_task(self._get_most_current_data_async(metadata))
        except RuntimeError:
            # No running loop, safe to use asyncio.run()
            return asyncio.run(self._get_most_current_data_async(metadata))
