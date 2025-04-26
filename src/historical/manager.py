from historical.fetcher import HistoricalFetcher
from historical.current_fetcher import CurrentDataFetcher
from historical.interfaces import IHistoricalDataManager

class HistoricalDataManagerImpl(IHistoricalDataManager):
    def __init__(self, storage_manager):
        self._fetcher = HistoricalFetcher(storage_manager)
        self._current_fetcher = CurrentDataFetcher(storage_manager)

    def stream_historical_data(self, coin, exchange, start, end, ws):
        # Fetch data in chunks and send over websocket
        data = self._fetcher.fetch_data(coin, exchange, start, end)
        for entry in data:
            ws.send_json({"type": "historical_data_chunk", "data": entry})
        ws.send_json({"type": "historical_data_complete", "coin": coin, "exchange": exchange})

    def get_most_current_data(self, coin, exchange):
        return self._current_fetcher.get_latest_entry(coin, exchange)
