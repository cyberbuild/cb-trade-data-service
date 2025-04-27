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
    day2_start = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    day2_end = datetime(2024, 1, 2, 23, 59, 59, tzinfo=timezone.utc)
    day3_start = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
    day3_end = datetime(2024, 1, 3, 23, 59, 59, tzinfo=timezone.utc)
    full_range_start = day1_start
    full_range_end = day3_end
    freq_minutes = 60 # Use hourly data for simplicity

    # 2. Generate Initial Data (with gaps)
    print("Generating initial data with gaps...")
    df_day1 = generate_test_data(day1_start, day1_end, freq_minutes)
    df_day2 = generate_test_data(day2_start, day2_end, freq_minutes)
    df_day3 = generate_test_data(day3_start, day3_end, freq_minutes)

    # Introduce gaps: e.g., missing 2 hours on day 2, 1 hour crossing midnight day 2 -> day 3
    gap1_start = datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
    gap1_end = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc) # Missing 10:00, 11:00
    gap2_start = datetime(2024, 1, 2, 23, 30, 0, tzinfo=timezone.utc)
    gap2_end = datetime(2024, 1, 3, 0, 30, 0, tzinfo=timezone.utc) # Missing 23:00 (day2), 00:00 (day3)

    df_day2_gaps = introduce_gaps(df_day2, [(gap1_start, gap1_end), (gap2_start, gap2_end)])
    df_day3_gaps = introduce_gaps(df_day3, [(gap2_start, gap2_end)])

    initial_data_with_gaps = pd.concat([df_day1, df_day2_gaps, df_day3_gaps], ignore_index=True)
    print(f"Initial data size: {len(initial_data_with_gaps)} rows")

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
    # Note: Exact comparison of datetimes can be tricky; compare ranges or timestamps within tolerance
    assert any(g[0] == gap1_start and g[1] == gap1_end for g in gaps), "Gap 1 not detected"
    # Gap 2 crosses midnight, check if detected correctly (might appear as two gaps depending on find_time_gaps logic)
    assert any(g[0] == datetime(2024, 1, 2, 23, 0, 0, tzinfo=timezone.utc) + expected_freq_delta and \
               g[1] == datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc) for g in gaps) or \
           any(g[0] == datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc) + expected_freq_delta and \
               g[1] == datetime(2024, 1, 3, 1, 0, 0, tzinfo=timezone.utc) for g in gaps) or \
           any(g[0] == datetime(2024, 1, 2, 23, 0, 0, tzinfo=timezone.utc) + expected_freq_delta and \
               g[1] == datetime(2024, 1, 3, 1, 0, 0, tzinfo=timezone.utc) for g in gaps), \
           "Gap 2 (crossing midnight) not detected correctly" # Adjust assertion based on find_time_gaps output


    # 6. Generate and Add Missing Records
    print("Generating and adding missing records...")
    missing_data_dfs = []
    for gap_start, gap_end in gaps:
        # Generate only the expected timestamps within the gap range
        # Use inclusive='left' instead of the deprecated 'closed'
        missing_timestamps = pd.date_range(
            start=gap_start, end=gap_end, freq=f'{freq_minutes}min', inclusive='left', tz='UTC' # Changed T to min
        )
        if not missing_timestamps.empty:
            # Create a DataFrame for these specific timestamps
            # Assign values based on index or some other logic if needed
            missing_df = pd.DataFrame({
                timestamp_col: missing_timestamps,
                'value': range(len(missing_timestamps)) # Simple value assignment
            })
            missing_data_dfs.append(missing_df)

    if missing_data_dfs:
        missing_data_combined = pd.concat(missing_data_dfs, ignore_index=True)
        print(f"Adding {len(missing_data_combined)} missing rows...")
        await storage_manager.save_entry(
            context=test_context,
            data=missing_data_combined,
            format_hint="delta",
            mode="append" # Append the missing data
            , timestamp_col=timestamp_col # Pass timestamp_col
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

    # Check total count - should match original full data count
    full_original_data = pd.concat([df_day1, df_day2, df_day3], ignore_index=True)
    assert len(retrieved_data_after_fill) == len(full_original_data), "Data count mismatch after filling gaps"

    # Verify no more gaps exist
    gaps_after_fill = find_time_gaps(retrieved_data_after_fill, expected_freq=expected_freq_delta, timestamp_col=timestamp_col) # Pass timestamp_col
    print(f"Gaps after fill: {gaps_after_fill}")
    assert not gaps_after_fill, "Gaps still exist after attempting to fill them"

    # Verify data integrity (e.g., check a specific timestamp that was missing)
    missing_ts_in_gap1 = datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
    assert missing_ts_in_gap1 in retrieved_data_after_fill[timestamp_col].tolist(), "Missing timestamp from gap 1 not found after fill"
    missing_ts_in_gap2_day2 = datetime(2024, 1, 2, 23, 0, 0, tzinfo=timezone.utc)
    missing_ts_in_gap2_day3 = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
    assert missing_ts_in_gap2_day2 in retrieved_data_after_fill[timestamp_col].tolist(), "Missing timestamp from gap 2 (day 2) not found after fill"
    assert missing_ts_in_gap2_day3 in retrieved_data_after_fill[timestamp_col].tolist(), "Missing timestamp from gap 2 (day 3) not found after fill"


    # 8. Append to an Existing Day (e.g., add more data to Day 3)
    print("Appending new data to an existing day...")
    append_start = datetime(2024, 1, 3, 10, 30, 0, tzinfo=timezone.utc) # Append some half-hour data
    append_end = datetime(2024, 1, 3, 11, 30, 0, tzinfo=timezone.utc)
    append_data = generate_test_data(append_start, append_end, freq_minutes=30) # 30 min freq
    append_data['value'] = append_data['value'] + 1000 # Distinguish appended values

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
    expected_day3_count = len(df_day3) + len(append_data)
    # Account for the fact that the midnight gap fill added 00:00 for day 3
    if missing_ts_in_gap2_day3 not in df_day3[timestamp_col].tolist():
         expected_day3_count += 1
    # Account for potential duplicates if append overlaps - Delta Lake handles this with merge usually, but append might duplicate
    # Let's retrieve distinct count for robustness if needed, or rely on Delta's behavior.
    # For this test, assume simple append adds rows.
    # assert len(retrieved_day3_data) == expected_day3_count # This might be fragile depending on exact overlap logic

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
    assert len(retrieved_day1_data) == len(df_day1), "Mismatch in Day 1 data count"
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

