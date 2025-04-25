import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from datetime import datetime
import pytest
from storage.path_utility import StoragePathUtility

# Valid inputs (exchange, coin, timestamp)
def test_valid_inputs_datetime():
    dt = datetime(2024, 4, 24, 15, 30, 0)
    path = StoragePathUtility.get_raw_data_path('binance', 'BTC', dt)
    assert path == 'raw_data/binance/BTC/2024/04/24/20240424T153000.json'

def test_valid_inputs_unix_timestamp():
    ts = 1713972600  # 2024-04-24 15:30:00 UTC
    path = StoragePathUtility.get_raw_data_path('binance', 'ETH', ts)
    assert path == 'raw_data/binance/ETH/2024/04/24/20240424T153000.json'

def test_valid_inputs_iso_string():
    iso = '2024-04-24T15:30:00'
    path = StoragePathUtility.get_raw_data_path('binance', 'SOL', iso)
    assert path == 'raw_data/binance/SOL/2024/04/24/20240424T153000.json'

def test_valid_inputs_numeric_string():
    ts_str = '1713972600'
    path = StoragePathUtility.get_raw_data_path('binance', 'ADA', ts_str)
    assert path == 'raw_data/binance/ADA/2024/04/24/20240424T153000.json'

# Edge cases (start/end of year/month)
def test_edge_case_start_of_year():
    dt = datetime(2023, 1, 1, 0, 0, 0)
    path = StoragePathUtility.get_raw_data_path('binance', 'BTC', dt)
    assert path == 'raw_data/binance/BTC/2023/01/01/20230101T000000.json'

def test_edge_case_end_of_month():
    dt = datetime(2023, 2, 28, 23, 59, 59)
    path = StoragePathUtility.get_raw_data_path('binance', 'BTC', dt)
    assert path == 'raw_data/binance/BTC/2023/02/28/20230228T235959.json'

# Different data types for inputs (already covered above)

# Invalid input types
def test_invalid_timestamp_type():
    with pytest.raises(TypeError):
        StoragePathUtility.get_raw_data_path('binance', 'BTC', [2024, 4, 24])

def test_invalid_timestamp_string():
    with pytest.raises(ValueError):
        StoragePathUtility.get_raw_data_path('binance', 'BTC', 'not-a-timestamp')
