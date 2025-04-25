import sys
import os
import pytest
from unittest.mock import MagicMock
import datetime

# Add the 'data-service' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from src.storage.implementations.storage_manager import StorageManagerImpl

class DummyRawDataStorage:
    def __init__(self):
        self.calls = []
    def save_entry(self, *args, **kwargs):
        self.calls.append(('save_entry', args, kwargs))
        return 'saved'
    def get_range(self, *args, **kwargs):
        self.calls.append(('get_range', args, kwargs))
        return ['entry1', 'entry2']
    def get_latest_entry(self, *args, **kwargs):
        self.calls.append(('get_latest_entry', args, kwargs))
        return {'latest': True}
    def list_coins(self, *args, **kwargs):
        self.calls.append(('list_coins', args, kwargs))
        return ['BTC', 'ETH']
    def check_coin_exists(self, *args, **kwargs):
        self.calls.append(('check_coin_exists', args, kwargs))
        return True

def test_save_entry_delegation():
    raw = DummyRawDataStorage()
    mgr = StorageManagerImpl(raw)
    result = mgr.save_entry('binance', 'BTC', 123456, {'foo': 'bar'})
    assert result == 'saved'
    assert raw.calls[-1][0] == 'save_entry'

def test_get_range_delegation():
    raw = DummyRawDataStorage()
    mgr = StorageManagerImpl(raw)
    result = mgr.get_range('binance', 'BTC', 1, 2)
    assert result == ['entry1', 'entry2']
    assert raw.calls[-1][0] == 'get_range'

def test_get_latest_entry_delegation():
    raw = DummyRawDataStorage()
    mgr = StorageManagerImpl(raw)
    result = mgr.get_latest_entry('binance', 'BTC')
    assert result == {'latest': True}
    assert raw.calls[-1][0] == 'get_latest_entry'

def test_list_coins_delegation():
    raw = DummyRawDataStorage()
    mgr = StorageManagerImpl(raw)
    result = mgr.list_coins('binance')
    assert result == ['BTC', 'ETH']
    assert raw.calls[-1][0] == 'list_coins'

def test_check_coin_exists_delegation():
    raw = DummyRawDataStorage()
    mgr = StorageManagerImpl(raw)
    result = mgr.check_coin_exists('binance', 'BTC')
    assert result is True
    assert raw.calls[-1][0] == 'check_coin_exists'
