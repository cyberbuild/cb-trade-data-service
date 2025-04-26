from historical.current_fetcher import CurrentDataFetcher

class DummyStorageManager:
    def __init__(self, latest=None):
        self.latest = latest
        self.called_with = None
    def get_latest_entry(self, exchange, coin):
        self.called_with = (exchange, coin)
        return self.latest

def test_get_latest_entry_returns_data():
    storage = DummyStorageManager(latest={"ts": 123, "price": 100})
    fetcher = CurrentDataFetcher(storage)
    result = fetcher.get_latest_entry("BTC", "binance")
    assert result == {"ts": 123, "price": 100}
    assert storage.called_with == ("binance", "BTC")

def test_get_latest_entry_none():
    storage = DummyStorageManager(latest=None)
    fetcher = CurrentDataFetcher(storage)
    result = fetcher.get_latest_entry("ETH", "binance")
    assert result is None
    assert storage.called_with == ("binance", "ETH")
