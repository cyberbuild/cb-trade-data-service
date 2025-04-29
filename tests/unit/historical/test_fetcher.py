import pytest
import asyncio
from unittest.mock import MagicMock
from historical.fetcher import HistoricalFetcher

class DummyStorageManager:
    async def get_range(self, context, start_time, end_time, limit=None, offset=None, output_format=None, **kwargs):
        self.called_with = {
            'context': context,
            'start_time': start_time,
            'end_time': end_time,
            'limit': limit,
            'offset': offset,
            'output_format': output_format
        }
        # Simulate data
        if context.get('coin_symbol', context.get('coin')) == 'BTC':
            return [{"ts": 1, "price": 100}, {"ts": 2, "price": 101}]
        return []

@pytest.mark.asyncio
async def test_fetch_data_returns_data():
    storage = DummyStorageManager()
    fetcher = HistoricalFetcher(storage)
    context = {'coin_symbol': 'BTC', 'exchange_name': 'binance'}
    result = await fetcher.fetch_data(context, 1, 2)
    assert result == [{"ts": 1, "price": 100}, {"ts": 2, "price": 101}]
    assert storage.called_with['context']['coin_symbol'] == 'BTC'
    assert storage.called_with['context']['exchange_name'] == 'binance'

@pytest.mark.asyncio
async def test_fetch_data_empty():
    storage = DummyStorageManager()
    fetcher = HistoricalFetcher(storage)
    context = {'coin_symbol': 'ETH', 'exchange_name': 'binance'}
    result = await fetcher.fetch_data(context, 1, 2)
    assert result == []

@pytest.mark.asyncio
async def test_fetch_data_with_limit_offset():
    storage = DummyStorageManager()
    fetcher = HistoricalFetcher(storage)
    context = {'coin_symbol': 'BTC', 'exchange_name': 'binance'}
    await fetcher.fetch_data(context, 1, 2, limit=10, offset=5)
    assert storage.called_with['limit'] == 10
    assert storage.called_with['offset'] == 5
