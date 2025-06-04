from exchange_source.models import ExchangeData, OHLCVRecord

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
from storage.storage_manager import StorageManager, IStorageManager
from config import get_settings


# Import helpers from the shared location
from tests.helpers.storage.data_utils import generate_test_data, find_time_gaps
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings

# --- Configuration ---
# Configuration is now handled by conftest.py loading .env.test


# --- Pytest Fixtures ---
# Fixtures are centralized and parameterized in conftest.py.


# --- Core Test Logic (Reusable) ---


async def _run_storage_manager_test_flow(
    storage_manager: IStorageManager, test_context: Dict[str, Any]
):
    """Contains the core logic for testing storage manager operations."""
    # Use the backend type from the manager instance for logging
    backend_type = type(storage_manager.backend).__name__
    print(f"\n--- Running Test Flow --- Backend: {backend_type} ---")
    timestamp_col = test_context.get(
        "timestamp_col", "timestamp"
    )  # 1. Define Time Range (3 days)
    day1_start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    day1_end = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)
    # Start day2 at 01:00 to create a midnight gap
    day2_start = datetime(2024, 1, 2, 1, 0, 0, tzinfo=timezone.utc)
    day2_end = datetime(2024, 1, 2, 23, 59, 59, tzinfo=timezone.utc)
    day3_start = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
    day3_end = datetime(2024, 1, 3, 23, 59, 59, tzinfo=timezone.utc)
    full_range_start = day1_start
    full_range_end = day3_end
    freq_minutes = 60  # Use hourly data for simplicity
    # 2. Fetch Initial Data (with gaps) from CCXTExchange
    print("Fetching initial data from CCXTExchange...")
    settings = get_settings()
    real_exchange = settings.exchange_id
    ccxt_client = CCXTExchangeClient(config=settings.ccxt, exchange_id=real_exchange)

    try:
        coin_symbol = "BTC/USDT"
        interval = "1h"
        candles_day1 = await ccxt_client.fetch_ohlcv_data(
            coin_symbol, day1_start, day1_end, interval=interval
        )
        candles_day2 = await ccxt_client.fetch_ohlcv_data(
            coin_symbol, day2_start, day2_end, interval=interval
        )
        candles_day3 = await ccxt_client.fetch_ohlcv_data(
            coin_symbol, day3_start, day3_end, interval=interval
        )
        df_day1 = candles_day1.to_arrow().to_pandas()
        df_day2 = candles_day2.to_arrow().to_pandas()
        df_day3 = candles_day3.to_arrow().to_pandas()
        for df in [df_day1, df_day2, df_day3]:
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                df["year"] = df["timestamp"].dt.year
                df["month"] = df["timestamp"].dt.month
                df["day"] = df["timestamp"].dt.day
                if "value" not in df.columns:
                    df["value"] = range(len(df))

        # Remove 12:00 from day1 to create a midday gap
        df_day1 = df_day1[df_day1["timestamp"].dt.hour != 12]

        # Fetch all records for the full range (no gaps)
        all_candles = []
        for day_start, day_end in [
            (day1_start, day1_end),
            (
                datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                day2_end,
            ),  # day2 full range
            (day3_start, day3_end),
        ]:
            all_candles.append(
                (
                    await ccxt_client.fetch_ohlcv_data(
                        coin_symbol, day_start, day_end, interval=interval
                    )
                )
                .to_arrow()
                .to_pandas()
            )
        df_all = pd.concat(all_candles, ignore_index=True)
        if not df_all.empty:
            df_all["timestamp"] = pd.to_datetime(
                df_all["timestamp"], unit="ms", utc=True
            )
            df_all["year"] = df_all["timestamp"].dt.year
            df_all["month"] = df_all["timestamp"].dt.month
            df_all["day"] = df_all["timestamp"].dt.day
            if "value" not in df_all.columns:
                df_all["value"] = range(len(df_all))

        # Define which timestamps to gap
        gap_timestamps = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),  # midday gap
            datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),  # midnight gap
        ]
        gap_records = df_all[df_all["timestamp"].isin(gap_timestamps)].copy()
        initial_data_with_gaps = df_all[
            ~df_all["timestamp"].isin(gap_timestamps)
        ].copy()
        print(
            f"Initial data size: {len(initial_data_with_gaps)} rows (gaps: {len(gap_records)})"
        )
        print("Timestamps in initial_data_with_gaps before save:")
        print(sorted([ts for ts in initial_data_with_gaps["timestamp"]]))
        print("Timestamps in gap_records (should be gaps):")
        print(sorted([ts for ts in gap_records["timestamp"]]))

        # Introduce gap at midnight between day1 and day2 (already done by starting day2 at 01:00)
        # No need to remove more rows for that gap

        # 3. Save Initial Data (Delta format, default partitioning)
        print("Saving initial data...")

        # Convert DataFrame to list of dicts for ExchangeData
        def convert_row(row):
            d = row._asdict() if hasattr(row, "_asdict") else dict(row)
            if isinstance(d.get("timestamp"), pd.Timestamp):
                d["timestamp"] = int(d["timestamp"].timestamp() * 1000)
            return OHLCVRecord(d)

        records = [
            convert_row(row) for row in initial_data_with_gaps.to_dict(orient="records")
        ]
        exchange_data = ExchangeData(records, test_context)
        await storage_manager.save_entry(
            exchange_data,
            format_hint="delta",
            mode="overwrite",
            timestamp_col=timestamp_col,
        )
        print("Initial data saved.")

        # 4. Retrieve Data Across Three Days
        print("Retrieving data across full range...")
        retrieved_data = await storage_manager.get_range(
            metadata=test_context,
            start_date=full_range_start,
            end_date=full_range_end,
            columns=None,
        )
        print(
            f"Retrieved {len(retrieved_data.data) if retrieved_data else 0} rows across full range."
        )
        print("Timestamps in loaded data after roundtrip:")
        print(sorted([r[timestamp_col] for r in retrieved_data.data]))
        expected_count = len(initial_data_with_gaps)
        actual_count = len(retrieved_data.data)
        print(
            f"Expected record count: {expected_count}, Actual record count: {actual_count}"
        )
        print("Expected timestamps:")
        print(
            sorted(
                [
                    (
                        int(pd.Timestamp(ts).timestamp() * 1000)
                        if isinstance(ts, datetime)
                        else ts
                    )
                    for ts in initial_data_with_gaps["timestamp"]
                ]
            )
        )
        print("Actual timestamps:")
        print(
            sorted(
                [
                    (
                        int(pd.Timestamp(ts).timestamp() * 1000)
                        if isinstance(ts, datetime)
                        else ts
                    )
                    for ts in pd.DataFrame([r for r in retrieved_data.data])[
                        "timestamp"
                    ]
                ]
            )
        )
        assert (
            actual_count == expected_count
        ), f"Mismatch in retrieved data count after initial save: expected {expected_count}, got {actual_count}"
        assert hasattr(retrieved_data, "data") and len(retrieved_data.data) > 0

        # 5. Identify Missing Data
        print("Identifying gaps...")
        expected_freq_delta = timedelta(minutes=freq_minutes)
        df = pd.DataFrame([r for r in retrieved_data.data])
        # Normalize timestamp column to pandas.Timestamp (UTC) if needed
        if not df.empty and pd.api.types.is_integer_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit="ms", utc=True)
        elif not df.empty and pd.api.types.is_float_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit="ms", utc=True)
        df = df.sort_values(by=timestamp_col).reset_index(drop=True)
        print("Timestamps in retrieved data:")
        print(df[timestamp_col].tolist())
        print("Timestamps as datetimes:")
        print(
            [
                (
                    pd.to_datetime(ts, unit="ms", utc=True)
                    if isinstance(ts, (int, float))
                    else ts
                )
                for ts in df[timestamp_col]
            ]
        )
        gaps = find_time_gaps(
            df, expected_freq=expected_freq_delta, timestamp_col=timestamp_col
        )
        print(f"Found gaps: {gaps}")

        # Assert the specific gaps we introduced are found
        expected_gap_times = [
            pd.Timestamp(t).tz_convert("UTC") if isinstance(t, datetime) else t
            for t in gap_timestamps
        ]
        found_gap_times = set(
            pd.Timestamp(g[0]).tz_convert("UTC") if isinstance(g[0], datetime) else g[0]
            for g in gaps
        )
        print(f"Expected gap times: {expected_gap_times}")
        print(f"Found gap times: {found_gap_times}")
        for t in expected_gap_times:
            t_norm = pd.Timestamp(t).tz_convert("UTC") if isinstance(t, datetime) else t
            assert any(
                abs((t_norm - fg).total_seconds()) < 1 for fg in found_gap_times
            ), f"Expected gap at {t_norm} not detected. Found: {gaps}"

        # 6. Generate and Add Missing Records
        print("Generating and adding missing records...")
        if not gap_records.empty:
            print(f"Adding {len(gap_records)} missing rows...")
            gap_records_list = [
                convert_row(row) for row in gap_records.to_dict(orient="records")
            ]
            gap_exchange_data = ExchangeData(gap_records_list, test_context)
            await storage_manager.save_entry(
                gap_exchange_data,
                format_hint="delta",
                mode="append",
                timestamp_col=timestamp_col,
            )
            print("Missing data saved.")
        else:
            print("No missing data to add.")

        # 7. Verify Gaps Filled
        print("Verifying gaps are filled...")
        retrieved_data_after_fill = await storage_manager.get_range(
            metadata=test_context,
            start_date=full_range_start,
            end_date=full_range_end,
            columns=None,
        )
        print(
            f"Retrieved {len(retrieved_data_after_fill.data)} rows after filling gaps."
        )

        # Normalize timestamp column to pandas.Timestamp (UTC) if needed
        df_after_fill = pd.DataFrame([r for r in retrieved_data_after_fill.data])
        if not df_after_fill.empty and pd.api.types.is_integer_dtype(
            df_after_fill[timestamp_col]
        ):
            df_after_fill[timestamp_col] = pd.to_datetime(
                df_after_fill[timestamp_col], unit="ms", utc=True
            )
        elif not df_after_fill.empty and pd.api.types.is_float_dtype(
            df_after_fill[timestamp_col]
        ):
            df_after_fill[timestamp_col] = pd.to_datetime(
                df_after_fill[timestamp_col], unit="ms", utc=True
            )

        # Check total count - should match expected full time index
        expected_full_index = pd.date_range(
            start=full_range_start,
            end=full_range_end,
            freq=f"{freq_minutes}min",
            tz="UTC",
        )
        assert len(retrieved_data_after_fill.data) == len(
            expected_full_index
        ), "Data count mismatch after filling gaps"

        # Verify no more gaps exist
        gaps_after_fill = find_time_gaps(
            df_after_fill,
            expected_freq=expected_freq_delta,
            timestamp_col=timestamp_col,
        )
        print(f"Gaps after fill: {gaps_after_fill}")
        assert not gaps_after_fill, "Gaps still exist after attempting to fill them"

        # Verify data integrity: check that all expected timestamps are present
        for ts in expected_full_index:
            assert (
                ts in df_after_fill[timestamp_col].tolist()
            ), f"Expected timestamp {ts} not found after fill"

        # 8. Append to an Existing Day (e.g., add more data to Day 3)
        print("Appending new data to an existing day...")
        append_start = datetime(
            2024, 1, 3, 10, 30, 0, tzinfo=timezone.utc
        )  # Append some half-hour data
        append_end = datetime(2024, 1, 3, 11, 30, 0, tzinfo=timezone.utc)
        append_data = generate_test_data(
            append_start, append_end, freq_minutes=30
        )  # 30 min freq
        append_data["value"] = (
            append_data["value"] + 1000
        )  # Distinguish appended values
        # Ensure append_data has all columns as in df_all (original schema)
        for col in df_all.columns:
            if col not in append_data.columns:
                if col in ("open", "high", "low", "close", "volume"):
                    append_data[col] = 0.0
                elif col in ("year", "month", "day"):
                    append_data[col] = getattr(append_data["timestamp"].dt, col)
                else:
                    append_data[col] = ""
        append_data = append_data[df_all.columns]

        append_records = [
            convert_row(row) for row in append_data.to_dict(orient="records")
        ]
        append_exchange_data = ExchangeData(append_records, test_context)
        await storage_manager.save_entry(
            append_exchange_data,
            format_hint="delta",
            mode="append",
            timestamp_col=timestamp_col,
        )
        print(f"Appended {len(append_data)} rows.")

        # 9. Pull Current Day (Day 3) and Verify Append
        print("Retrieving Day 3 data to verify append...")
        retrieved_day3_data = await storage_manager.get_range(
            metadata=test_context,
            start_date=day3_start,
            end_date=day3_end,
            columns=None,
        )
        df_day3 = (
            pd.DataFrame([r for r in retrieved_day3_data.data])
            if retrieved_day3_data
            else pd.DataFrame()
        )
        if not df_day3.empty and pd.api.types.is_integer_dtype(df_day3[timestamp_col]):
            df_day3[timestamp_col] = pd.to_datetime(
                df_day3[timestamp_col], unit="ms", utc=True
            )
        elif not df_day3.empty and pd.api.types.is_float_dtype(df_day3[timestamp_col]):
            df_day3[timestamp_col] = pd.to_datetime(
                df_day3[timestamp_col], unit="ms", utc=True
            )
        print(f"Retrieved {len(df_day3)} rows for Day 3 after append.")

        appended_ts = datetime(2024, 1, 3, 11, 0, 0, tzinfo=timezone.utc)
        assert (
            appended_ts in df_day3[timestamp_col].tolist()
        ), "Appended timestamp not found in Day 3 data"
        assert df_day3[df_day3[timestamp_col] == appended_ts]["value"].iloc[0] >= 1000

        # Check total count for Day 3
        expected_day3_index = pd.date_range(
            start=day3_start, end=day3_end, freq=f"{freq_minutes}min", tz="UTC"
        )
        for ts in expected_day3_index:
            assert (
                ts in df_day3[timestamp_col].tolist()
            ), f"Expected timestamp {ts} not found in Day 3 data after append"

        # 10. Pull a Past Day (Day 1)
        print("Retrieving Day 1 data...")
        retrieved_day1_data = await storage_manager.get_range(
            metadata=test_context,
            start_date=day1_start,
            end_date=day1_end,
            columns=None,
        )
        df_day1 = (
            pd.DataFrame([r for r in retrieved_day1_data.data])
            if retrieved_day1_data
            else pd.DataFrame()
        )
        if not df_day1.empty and pd.api.types.is_integer_dtype(df_day1[timestamp_col]):
            df_day1[timestamp_col] = pd.to_datetime(
                df_day1[timestamp_col], unit="ms", utc=True
            )
        elif not df_day1.empty and pd.api.types.is_float_dtype(df_day1[timestamp_col]):
            df_day1[timestamp_col] = pd.to_datetime(
                df_day1[timestamp_col], unit="ms", utc=True
            )
        print(f"Retrieved {len(df_day1)} rows for Day 1.")
        expected_day1_index = pd.date_range(
            start=day1_start, end=day1_end, freq=f"{freq_minutes}min", tz="UTC"
        )
        assert len(df_day1) == len(expected_day1_index), "Mismatch in Day 1 data count"
        assert df_day1[timestamp_col].min() >= day1_start
        assert df_day1[timestamp_col].max() <= day1_end + timedelta(
            minutes=freq_minutes
        )

        # 11. Test list_coins and check_coin_exists
        print("Testing metadata methods...")
        coin_symbol = test_context["coin"]
        exchange = test_context["exchange"]
        data_type = test_context["data_type"]

        coins = await storage_manager.list_coins(
            exchange_name=exchange, data_type=data_type
        )
        print(f"Listed coins for {exchange}/{data_type}: {coins}")
        # Compare with uppercase version due to potential case normalization in storage/listing
        assert (
            coin_symbol.upper() in coins
        ), f"Test coin '{coin_symbol.upper()}' not found in list_coins result"

        # Check existence using uppercase
        exists = await storage_manager.check_coin_exists(
            exchange_name=exchange,
            coin_symbol=coin_symbol.upper(),
            data_type=data_type,
            interval=test_context["interval"],
        )
        print(
            f"Existence check for {exchange}/{data_type}/{coin_symbol.upper()}: {exists}"
        )
        assert exists, f"check_coin_exists returned False for the test coin"

        non_existent_coin = "NONEXISTENTCOIN"  # Check non-existent coin (also uppercase for consistency if needed, though case might not matter for non-existence check)
        exists_false = await storage_manager.check_coin_exists(
            exchange_name=exchange,
            coin_symbol=non_existent_coin.upper(),
            data_type=data_type,
            interval=test_context["interval"],
        )
        print(
            f"Existence check for {exchange}/{data_type}/{non_existent_coin.upper()}: {exists_false}"
        )
        assert (
            not exists_false
        ), f"check_coin_exists returned True for a non-existent coin"

        print(f"--- Test Flow Completed Successfully --- Backend: {backend_type} ---")

    finally:
        # Ensure ccxt_client is always closed, even if an exception occurs
        try:
            await ccxt_client.close()
        except Exception:
            pass  # Ignore errors during cleanup


# --- Test Functions ---


@pytest.mark.integration  # Mark as integration test
@pytest.mark.asyncio  # Mark test as async
@pytest.mark.parametrize("storage_manager", ["local", "azure"], indirect=True)
async def test_storage_manager_integration_flow(storage_manager: IStorageManager):

    settings = get_settings()
    exchange = settings.exchange_id

    test_context = {
        "data_type": "ohlcv",
        "exchange": exchange,
        "coin": "BTC_USD",
        "interval": "5m",
    }
    # The storage_manager fixture is injected by pytest from conftest.py
    # It will run this test twice, once for local, once for azure.
    await _run_storage_manager_test_flow(storage_manager, test_context)
