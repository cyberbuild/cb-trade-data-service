import sys
import os
from unittest.mock import MagicMock
from datetime import datetime
from collection.processor import RealTimeProcessor

class DummyStorageManager:
    def __init__(self):
        self.calls = []
    def store_raw_data(self, exchange_name, coin_symbol, timestamp, data):
        self.calls.append((exchange_name, coin_symbol, timestamp, data))

def test_process_realtime_entry_stores_data():
    storage_manager = DummyStorageManager()
    processor = RealTimeProcessor(storage_manager)
    exchange = 'binance'
    coin = 'BTC'
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    data = {'order_book': 'sample'}
    processor.process_realtime_entry(exchange, coin, timestamp, data)
    assert len(storage_manager.calls) == 1
    call = storage_manager.calls[0]
    assert call[0] == exchange
    assert call[1] == coin
    assert call[2] == timestamp
    assert call[3] == data

def test_process_realtime_entry_handles_multiple_calls():
    storage_manager = DummyStorageManager()
    processor = RealTimeProcessor(storage_manager)
    for i in range(3):
        processor.process_realtime_entry('binance', f'COIN{i}', datetime(2024, 1, 1, 12, i, 0), {'order_book': i})
    assert len(storage_manager.calls) == 3
    for i, call in enumerate(storage_manager.calls):
        assert call[1] == f'COIN{i}'
        assert call[3]['order_book'] == i
