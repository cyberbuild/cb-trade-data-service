import pytest
import os
import shutil
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.storage.implementations.local_file_storage import LocalFileStorage
from src.storage.path_utility import StoragePathUtility

# Test constants
TEST_EXCHANGE = "test_exchange"
TEST_COIN = "BTC"
TEST_ROOT_DIR = Path("./temp_test_storage") # Relative to project root

@pytest.fixture(scope="function")
def local_storage():
    """Fixture to set up and tear down LocalFileStorage for each test function."""
    # Ensure the test directory is clean before starting
    if TEST_ROOT_DIR.exists():
        shutil.rmtree(TEST_ROOT_DIR)
    TEST_ROOT_DIR.mkdir(parents=True, exist_ok=True)

    # Corrected argument name from base_directory to storage_root
    storage = LocalFileStorage(storage_root=str(TEST_ROOT_DIR))
    yield storage

    # Clean up the test directory after the test
    shutil.rmtree(TEST_ROOT_DIR)

def test_save_and_get_latest_entry(local_storage: LocalFileStorage):
    """Test saving an entry and retrieving the latest one."""
    ts1 = datetime.now(timezone.utc)
    data1 = {"price": 50000, "volume": 10}
    local_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts1, data1)

    # Add a slight delay to ensure distinct timestamps if needed by file system resolution
    time.sleep(0.01)

    ts2 = datetime.now(timezone.utc)
    data2 = {"price": 50050, "volume": 12}
    local_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts2, data2)

    latest_entry = local_storage.get_latest_entry(TEST_EXCHANGE, TEST_COIN)

    assert latest_entry is not None
    # Compare timestamps with tolerance, handling the 'Z' suffix
    latest_ts_str = latest_entry["timestamp"].replace('Z', '+00:00')
    assert abs(datetime.fromisoformat(latest_ts_str) - ts2) < timedelta(seconds=1)
    assert latest_entry["data"] == data2

def test_get_range(local_storage: LocalFileStorage):
    """Test retrieving entries within a specific time range."""
    base_time = datetime.now(timezone.utc)
    timestamps = [base_time + timedelta(seconds=i) for i in range(5)]
    entries = [{"price": 50000 + i * 10, "volume": 10 + i} for i in range(5)]

    for ts, data in zip(timestamps, entries):
        local_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts, data)
        time.sleep(0.01) # Ensure distinct file names if timestamp resolution is low

    start_time_orig = timestamps[1]
    end_time_orig = timestamps[3]

    # Truncate microseconds to match filename precision for the query
    start_time_query = start_time_orig.replace(microsecond=0)
    # For the end time, we need to be inclusive. Since filenames truncate,
    # an end time like 12:00:03.500 should include the file 120003.json.
    # Comparing against the truncated file timestamp (12:00:03.000) works correctly.
    end_time_query = end_time_orig # Keep original end time for comparison

    retrieved_entries = local_storage.get_range(TEST_EXCHANGE, TEST_COIN, start_time_query, end_time_query)

    assert len(retrieved_entries) == 3 # Entries at index 1, 2, 3
    retrieved_prices = [e["data"]["price"] for e in retrieved_entries]
    expected_prices = [entries[i]["price"] for i in range(1, 4)]
    assert sorted(retrieved_prices) == sorted(expected_prices)

def test_list_coins(local_storage: LocalFileStorage):
    """Test listing available coins for an exchange."""
    coins = ["BTC", "ETH", "LTC"]
    ts = datetime.now(timezone.utc)
    data = {"price": 100}

    for coin in coins:
        local_storage.save_entry(TEST_EXCHANGE, coin, ts, data)

    listed_coins = local_storage.list_coins(TEST_EXCHANGE)
    assert sorted(listed_coins) == sorted(coins)

def test_check_coin_exists(local_storage: LocalFileStorage):
    """Test checking if a coin directory exists."""
    ts = datetime.now(timezone.utc)
    data = {"price": 100}

    assert not local_storage.check_coin_exists(TEST_EXCHANGE, TEST_COIN)
    local_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts, data)
    assert local_storage.check_coin_exists(TEST_EXCHANGE, TEST_COIN)

def test_get_latest_entry_no_data(local_storage: LocalFileStorage):
    """Test getting the latest entry when no data exists."""
    latest_entry = local_storage.get_latest_entry(TEST_EXCHANGE, "NONEXISTENT")
    assert latest_entry is None

def test_get_range_no_data(local_storage: LocalFileStorage):
    """Test getting a range when no data exists."""
    start_time = datetime.now(timezone.utc) - timedelta(days=1)
    end_time = datetime.now(timezone.utc)
    retrieved_entries = local_storage.get_range(TEST_EXCHANGE, "NONEXISTENT", start_time, end_time)
    assert len(retrieved_entries) == 0

def test_list_coins_no_exchange(local_storage: LocalFileStorage):
    """Test listing coins when the exchange directory doesn't exist."""
    listed_coins = local_storage.list_coins("NONEXISTENT_EXCHANGE")
    assert len(listed_coins) == 0

def test_check_coin_exists_no_exchange(local_storage: LocalFileStorage):
    """Test checking coin existence when the exchange directory doesn't exist."""
    assert not local_storage.check_coin_exists("NONEXISTENT_EXCHANGE", TEST_COIN)

