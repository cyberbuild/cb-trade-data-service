import pytest
from unittest.mock import MagicMock
from historical.fetcher import HistoricalFetcher

class DummyStorageManager:
    def get_raw_data_range(self, exchange_name, coin_symbol, start_time, end_time, limit=None, offset=None):
        self.called_with = {
            'exchange_name': exchange_name,
            'coin_symbol': coin_symbol,
            'start_time': start_time,
            'end_time': end_time,
            'limit': limit,
            'offset': offset
        }
        # Simulate data
        if coin_symbol == 'BTC':
            return [{"ts": 1, "price": 100}, {"ts": 2, "price": 101}]
        return []

def test_fetch_data_returns_data():
    storage = DummyStorageManager()
    fetcher = HistoricalFetcher(storage)
    result = fetcher.fetch_data('BTC', 'binance', 1, 2)
    assert result == [{"ts": 1, "price": 100}, {"ts": 2, "price": 101}]
    assert storage.called_with['coin_symbol'] == 'BTC'
    assert storage.called_with['exchange_name'] == 'binance'

def test_fetch_data_empty():
    storage = DummyStorageManager()
    fetcher = HistoricalFetcher(storage)
    result = fetcher.fetch_data('ETH', 'binance', 1, 2)
    assert result == []

def test_fetch_data_with_limit_offset():
    storage = DummyStorageManager()
    fetcher = HistoricalFetcher(storage)
    fetcher.fetch_data('BTC', 'binance', 1, 2, limit=10, offset=5)
    assert storage.called_with['limit'] == 10
    assert storage.called_with['offset'] == 5
