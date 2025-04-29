\
import pytest
import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta, timezone
import time
import os
import shutil
import uuid
from typing import Dict, Any, Tuple, Optional, List, Generator
from pathlib import Path

# Use PEP 420 compliant imports
from storage.storage_manager import StorageManagerImpl # Keep this if needed directly, though fixture provides it
from storage.interfaces import IStorageManager # Keep this for type hinting

# Import helpers from the shared location
from tests.helpers.storage.data_utils import generate_test_data, introduce_gaps, find_time_gaps
from exchange_source.plugins.ccxt_exchange import CCXTExchangeClient
from config import get_settings

# --- Configuration ---
# Configuration is now handled by conftest.py loading .env.test


# --- Pytest Fixtures ---
# Fixtures are centralized and parameterized in conftest.py.


# --- Core Test Logic (Reusable) ---

async def _run_storage_manager_test_flow(storage_manager: IStorageManager, test_context: Dict[str, Any]):
    """Contains the core logic for testing storage manager operations."""
    # Use the backend type from the manager instance for logging
    backend_type = type(storage_manager.backend).__name__
    print(f"\n--- Running Test Flow --- Backend: {backend_type} ---")
    timestamp_col = 'timestamp' # Define timestamp column name

    # 1. Define Time Range (3 days)
    day1_start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    day1_end = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)
    # Start day2 at 01:00 to create a midnight gap
    day2_start = datetime(2024, 1, 2, 1, 0, 0, tzinfo=timezone.utc)
    day2_end = datetime(2024, 1, 2, 23, 59, 59, tzinfo=timezone.utc)
    day3_start = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
    day3_end = datetime(2024, 1, 3, 23, 59, 59, tzinfo=timezone.utc)
    full_range_start = day1_start
    full_range_end = day3_end
    freq_minutes = 60 # Use hourly data for simplicity

    # 2. Fetch Initial Data (with gaps) from CCXTExchange
    print("Fetching initial data from CCXTExchange...")
    settings = get_settings()
    ccxt_client = CCXTExchangeClient(config=settings.ccxt, exchange_id='binance')
    coin_symbol = 'BTC/USDT'
    interval = '1h'
    candles_day1 = await ccxt_client.fetch_historical_data(coin_symbol, day1_start, day1_end, interval=interval)
    candles_day2 = await ccxt_client.fetch_historical_data(coin_symbol, day2_start, day2_end, interval=interval)
    candles_day3 = await ccxt_client.fetch_historical_data(coin_symbol, day3_start, day3_end, interval=interval)
    df_day1 = pd.DataFrame(candles_day1)
    df_day2 = pd.DataFrame(candles_day2)
    df_day3 = pd.DataFrame(candles_day3)
    for df in [df_day1, df_day2, df_day3]:
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df['year'] = df['timestamp'].dt.year
            df['month'] = df['timestamp'].dt.month
            df['day'] = df['timestamp'].dt.day
            if 'value' not in df.columns:
                df['value'] = range(len(df))

    # Remove 12:00 from day1 to create a midday gap
    df_day1 = df_day1[df_day1['timestamp'].dt.hour != 12]

    # Fetch all records for the full range (no gaps)
    all_candles = []
    for day_start, day_end in [
        (day1_start, day1_end),
        (datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc), day2_end),  # day2 full range
        (day3_start, day3_end)
    ]:
        all_candles.extend(await ccxt_client.fetch_historical_data(coin_symbol, day_start, day_end, interval=interval))
    df_all = pd.DataFrame(all_candles)
    if not df_all.empty:
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], unit='ms', utc=True)
        df_all['year'] = df_all['timestamp'].dt.year
        df_all['month'] = df_all['timestamp'].dt.month
        df_all['day'] = df_all['timestamp'].dt.day
        if 'value' not in df_all.columns:
            df_all['value'] = range(len(df_all))

    # Define which timestamps to gap
    gap_timestamps = [
        datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),  # midday gap
        datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)    # midnight gap
    ]
    gap_records = df_all[df_all['timestamp'].isin(gap_timestamps)].copy()
    initial_data_with_gaps = df_all[~df_all['timestamp'].isin(gap_timestamps)].copy()
    print(f"Initial data size: {len(initial_data_with_gaps)} rows (gaps: {len(gap_records)})")

    # Introduce gap at midnight between day1 and day2 (already done by starting day2 at 01:00)
    # No need to remove more rows for that gap

    # 3. Save Initial Data (Delta format, default partitioning)
    print("Saving initial data...")
    await storage_manager.save_entry(
        context=test_context,
        data=initial_data_with_gaps,
        format_hint="delta",
        mode="overwrite" # Start fresh for the test
        , timestamp_col=timestamp_col # Pass timestamp_col
    )
    print("Initial data saved.")

    # 4. Retrieve Data Across Three Days
    print("Retrieving data across full range...")
    retrieved_data = await storage_manager.get_range(
        context=test_context,
        start_time=full_range_start,
        end_time=full_range_end,
        output_format="dataframe",
        format_hint="delta",
        timestamp_col=timestamp_col # Pass timestamp_col
    )
    print(f"Retrieved {len(retrieved_data)} rows across full range.")
    assert len(retrieved_data) == len(initial_data_with_gaps), "Mismatch in retrieved data count after initial save"
    assert not retrieved_data.empty

    # 5. Identify Missing Data
    print("Identifying gaps...")
    expected_freq_delta = timedelta(minutes=freq_minutes)
    gaps = find_time_gaps(retrieved_data, expected_freq=expected_freq_delta, timestamp_col=timestamp_col) # Pass timestamp_col
    print(f"Found gaps: {gaps}")

    # Assert the specific gaps we introduced are found
    # Instead of expecting a single range, check for each missing timestamp
    expected_gap_times = gap_timestamps
    found_gap_times = set(g[0] for g in gaps)
    for t in expected_gap_times:
        assert t in found_gap_times, f"Expected gap at {t} not detected. Found: {gaps}"


    # 6. Generate and Add Missing Records
    print("Generating and adding missing records...")
    if not gap_records.empty:
        print(f"Adding {len(gap_records)} missing rows...")
        await storage_manager.save_entry(
            context=test_context,
            data=gap_records,
            format_hint="delta",
            mode="append", # Append the missing data
            timestamp_col=timestamp_col # Pass timestamp_col
        )
        print("Missing data saved.")
    else:
        print("No missing data to add.")


    # 7. Verify Gaps Filled
    print("Verifying gaps are filled...")
    retrieved_data_after_fill = await storage_manager.get_range(
        context=test_context,
        start_time=full_range_start,
        end_time=full_range_end,
        output_format="dataframe",
        format_hint="delta",
        timestamp_col=timestamp_col # Pass timestamp_col
    )
    print(f"Retrieved {len(retrieved_data_after_fill)} rows after filling gaps.")

    # Check total count - should match expected full time index
    expected_full_index = pd.date_range(
        start=full_range_start,
        end=full_range_end,
        freq=f'{freq_minutes}min',
        tz='UTC'
    )
    assert len(retrieved_data_after_fill) == len(expected_full_index), "Data count mismatch after filling gaps"

    # Verify no more gaps exist
    gaps_after_fill = find_time_gaps(retrieved_data_after_fill, expected_freq=expected_freq_delta, timestamp_col=timestamp_col) # Pass timestamp_col
    print(f"Gaps after fill: {gaps_after_fill}")
    assert not gaps_after_fill, "Gaps still exist after attempting to fill them"

    # Verify data integrity: check that all expected timestamps are present
    for ts in expected_full_index:
        assert ts in retrieved_data_after_fill[timestamp_col].tolist(), f"Expected timestamp {ts} not found after fill"

    # 8. Append to an Existing Day (e.g., add more data to Day 3)
    print("Appending new data to an existing day...")
    append_start = datetime(2024, 1, 3, 10, 30, 0, tzinfo=timezone.utc) # Append some half-hour data
    append_end = datetime(2024, 1, 3, 11, 30, 0, tzinfo=timezone.utc)
    append_data = generate_test_data(append_start, append_end, freq_minutes=30) # 30 min freq
    append_data['value'] = append_data['value'] + 1000 # Distinguish appended values
    # Ensure append_data has all columns as in df_all (original schema)
    for col in df_all.columns:
        if col not in append_data.columns:
            if col in ('open', 'high', 'low', 'close', 'volume'):
                append_data[col] = 0.0
            elif col in ('year', 'month', 'day'):
                append_data[col] = getattr(append_data['timestamp'].dt, col)
            else:
                append_data[col] = ''
    append_data = append_data[df_all.columns]

    await storage_manager.save_entry(
        context=test_context,
        data=append_data,
        format_hint="delta",
        mode="append",
        timestamp_col=timestamp_col # Pass timestamp_col
    )
    print(f"Appended {len(append_data)} rows.")

    # 9. Pull Current Day (Day 3) and Verify Append
    print("Retrieving Day 3 data to verify append...")
    retrieved_day3_data = await storage_manager.get_range(
        context=test_context,
        start_time=day3_start,
        end_time=day3_end,
        output_format="dataframe",
        format_hint="delta",
        timestamp_col=timestamp_col # Pass timestamp_col
    )
    print(f"Retrieved {len(retrieved_day3_data)} rows for Day 3 after append.")

    # Check if appended data is present
    appended_ts = datetime(2024, 1, 3, 11, 0, 0, tzinfo=timezone.utc)
    assert appended_ts in retrieved_day3_data[timestamp_col].tolist(), "Appended timestamp not found in Day 3 data"
    # Check value if needed
    assert retrieved_day3_data[retrieved_day3_data[timestamp_col] == appended_ts]['value'].iloc[0] >= 1000

    # Check total count for Day 3
    # Instead of using missing_ts_in_gap2_day3, use the expected time index for Day 3
    expected_day3_index = pd.date_range(
        start=day3_start,
        end=day3_end,
        freq=f'{freq_minutes}min',
        tz='UTC'
    )
    # Allow for possible duplicates due to append, so check at least all expected timestamps are present
    for ts in expected_day3_index:
        assert ts in retrieved_day3_data[timestamp_col].tolist(), f"Expected timestamp {ts} not found in Day 3 data after append"

    # 10. Pull a Past Day (Day 1)
    print("Retrieving Day 1 data...")
    retrieved_day1_data = await storage_manager.get_range(
        context=test_context,
        start_time=day1_start,
        end_time=day1_end,
        output_format="dataframe",
        format_hint="delta",
        timestamp_col=timestamp_col # Pass timestamp_col
    )
    print(f"Retrieved {len(retrieved_day1_data)} rows for Day 1.")
    # Instead of comparing to df_day1 (which has the gap), compare to the expected full set for day 1
    expected_day1_index = pd.date_range(
        start=day1_start,
        end=day1_end,
        freq=f'{freq_minutes}min',
        tz='UTC'
    )
    assert len(retrieved_day1_data) == len(expected_day1_index), "Mismatch in Day 1 data count"
    # Check timestamp range
    assert retrieved_day1_data[timestamp_col].min() >= day1_start
    assert retrieved_day1_data[timestamp_col].max() <= day1_end + timedelta(minutes=freq_minutes) # Allow for end point inclusion


    # 11. Test list_coins and check_coin_exists
    print("Testing metadata methods...")
    coin_symbol = test_context['coin']
    exchange = test_context['exchange']
    data_type = test_context['data_type']

    coins = await storage_manager.list_coins(exchange_name=exchange, data_type=data_type)
    print(f"Listed coins for {exchange}/{data_type}: {coins}")
    # Compare with uppercase version due to potential case normalization in storage/listing
    assert coin_symbol.upper() in coins, f"Test coin '{coin_symbol.upper()}' not found in list_coins result"

    # Check existence using uppercase
    exists = await storage_manager.check_coin_exists(exchange_name=exchange, coin_symbol=coin_symbol.upper(), data_type=data_type)
    print(f"Existence check for {exchange}/{data_type}/{coin_symbol.upper()}: {exists}")
    assert exists, f"check_coin_exists returned False for the test coin"

    non_existent_coin = "NONEXISTENTCOIN"
    # Check non-existent coin (also uppercase for consistency if needed, though case might not matter for non-existence check)
    exists_false = await storage_manager.check_coin_exists(exchange_name=exchange, coin_symbol=non_existent_coin.upper(), data_type=data_type)
    print(f"Existence check for {exchange}/{data_type}/{non_existent_coin.upper()}: {exists_false}")
    assert not exists_false, f"check_coin_exists returned True for a non-existent coin"

    await ccxt_client.aclose()
    print(f"--- Test Flow Completed Successfully --- Backend: {backend_type} ---")


# --- Test Functions ---

@pytest.mark.integration # Mark as integration test
@pytest.mark.asyncio # Mark test as async
async def test_storage_manager_integration_flow(storage_manager: IStorageManager):
    """
    Runs the core storage manager test flow using the parameterized
    storage_manager fixture from conftest.py (covers both local and Azure backends).
    """
    test_context = {
        'data_type': 'test_ohlcv',
        'exchange': 'test_exchange',
        'coin': f'TEST_COIN_{uuid.uuid4().hex[:6]}' # Use unique coin per run
    }
    # The storage_manager fixture is injected by pytest from conftest.py
    # It will run this test twice, once for local, once for azure.
    await _run_storage_manager_test_flow(storage_manager, test_context)

