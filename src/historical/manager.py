from historical.fetcher import HistoricalFetcher
from historical.current_fetcher import CurrentDataFetcher
from historical.interfaces import IHistoricalDataManager

class HistoricalDataManagerImpl(IHistoricalDataManager):
    def __init__(self, storage_manager):
        self._fetcher = HistoricalFetcher(storage_manager)
        self._current_fetcher = CurrentDataFetcher(storage_manager)

    def stream_historical_data(self, coin, exchange, start, end, ws, chunk_size=1000):
        """
        Streams historical data in chunks over the websocket. Handles errors and sends completion message.
        """
        offset = 0
        while True:
            try:
                data_chunk = self._fetcher.fetch_data(coin, exchange, start, end, limit=chunk_size, offset=offset)
            except Exception as e:
                ws.send_json({"type": "historical_data_error", "error": str(e)})
                break
            if not data_chunk:
                break
            for entry in data_chunk:
                try:
                    ws.send_json({"type": "historical_data_chunk", "data": entry})
                except Exception as ws_exc:
                    # WebSocket error, stop streaming
                    ws.send_json({"type": "historical_data_error", "error": str(ws_exc)})
                    return
            if len(data_chunk) < chunk_size:
                break
            offset += chunk_size
        try:
            ws.send_json({"type": "historical_data_complete", "coin": coin, "exchange": exchange})
        except Exception:
            pass

    def get_most_current_data(self, coin, exchange):
        """
        Returns the most current data entry or None if not found.
        """
        try:
            return self._current_fetcher.get_latest_entry(coin, exchange)
        except Exception:
            return None
