from historical.fetcher import HistoricalFetcher
from historical.current_fetcher import CurrentDataFetcher
from historical.interfaces import IHistoricalDataManager
import asyncio
import inspect
from typing import Dict, Any

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
                # Pass context directly to fetcher
                data_chunk = await self._fetcher.fetch_data(
                    context=context,
                    start_time=start,
                    end_time=end,
                    limit=chunk_size,
                    offset=offset,
                    output_format="dict" # Stream expects dicts
                )
            except Exception as e:
                await ws.send_json({"type": "historical_data_error", "error": str(e), "context": context})
                break
            if not data_chunk:
                break

            try:
                # Send the whole chunk list at once if ws supports it, or iterate
                await ws.send_json({"type": "historical_data_chunk", "data": data_chunk, "context": context})
                sent_any = True
            except Exception as ws_exc:
                try:
                    await ws.send_json({"type": "historical_data_error", "error": f"WebSocket send error: {ws_exc}", "context": context})
                except Exception: pass # Avoid error loops if ws is broken
                return # Stop streaming on WS error

            offset += len(data_chunk)
            if len(data_chunk) < chunk_size:
                break # Fetched last chunk

        # Send completion message if anything was sent
        if sent_any:
            try:
                await ws.send_json({"type": "historical_data_complete", "context": context})
            except Exception as ws_exc:
                 # Log error, but don't send another error message over potentially broken ws
                 print(f"Error sending completion message: {ws_exc}")


    def stream_historical_data(self, context: Dict[str, Any], start, end, ws, chunk_size=1000):
        """
        Streams historical data. Runs async version.
        """
        # Always return the awaitable coroutine
        return self._stream_historical_data_async(context, start, end, ws, chunk_size)


    async def _get_most_current_data_async(self, context: Dict[str, Any]):
        """
        Async method to get the most current data entry using context.
        """
        try:
            # Pass context directly to current_fetcher
            return await self._current_fetcher.get_latest_entry(context)
        except Exception as e:
            print(f"Error getting current data for {context}: {e}") # Log error
            return None

    def get_most_current_data(self, context: Dict[str, Any]):
        """
        Gets the most current data entry using context. Runs async version.
        """
        # Always return the awaitable coroutine
        return self._get_most_current_data_async(context)
