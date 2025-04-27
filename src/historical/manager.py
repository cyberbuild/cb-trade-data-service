from historical.fetcher import HistoricalFetcher
from historical.current_fetcher import CurrentDataFetcher
from historical.interfaces import IHistoricalDataManager
import asyncio
import inspect

class HistoricalDataManagerImpl(IHistoricalDataManager):
    def __init__(self, storage_manager):
        self._fetcher = HistoricalFetcher(storage_manager)
        self._current_fetcher = CurrentDataFetcher(storage_manager)

    async def _stream_historical_data_async(self, coin, exchange, start, end, ws, chunk_size=1000):
        """
        Async method to stream historical data in chunks over the websocket.
        """
        offset = 0
        sent_any = False
        while True:
            try:
                fetched = self._fetcher.fetch_data(coin, exchange, start, end, limit=chunk_size, offset=offset)
                data_chunk = await fetched if inspect.isawaitable(fetched) else fetched
            except Exception as e:
                ws.send_json({"type": "historical_data_error", "error": str(e), "coin": coin, "exchange": exchange})
                break
            if not data_chunk:
                break
            for entry in data_chunk:
                try:
                    ws.send_json({"type": "historical_data_chunk", "data": entry, "coin": coin, "exchange": exchange})
                    sent_any = True
                except Exception as ws_exc:
                    ws.send_json({"type": "historical_data_error", "error": str(ws_exc), "coin": coin, "exchange": exchange})
                    return
            offset += len(data_chunk)
            if len(data_chunk) < chunk_size:
                break
        if sent_any:
            try:
                ws.send_json({"type": "historical_data_chunk", "data": {"final": True}, "coin": coin, "exchange": exchange})
            except Exception as ws_exc:
                ws.send_json({"type": "historical_data_error", "error": str(ws_exc), "coin": coin, "exchange": exchange})
                return
        try:
            ws.send_json({"type": "historical_data_complete", "coin": coin, "exchange": exchange})
        except Exception:
            pass

    def stream_historical_data(self, coin, exchange, start, end, ws, chunk_size=1000):
        """
        Wrapper to support sync unit tests and async integration tests.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, run sync
            asyncio.run(self._stream_historical_data_async(coin, exchange, start, end, ws, chunk_size))
        else:
            # Inside async context, return coroutine to be awaited
            return self._stream_historical_data_async(coin, exchange, start, end, ws, chunk_size)

    async def _get_most_current_data_async(self, coin, exchange):
        """
        Async method to get the most current data entry.
        """
        try:
            result = self._current_fetcher.get_latest_entry(coin, exchange)
            return await result if inspect.isawaitable(result) else result
        except Exception:
            return None

    def get_most_current_data(self, coin, exchange):
        """
        Wrapper to support sync and async calls.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self._get_most_current_data_async(coin, exchange))
        else:
            return self._get_most_current_data_async(coin, exchange)
