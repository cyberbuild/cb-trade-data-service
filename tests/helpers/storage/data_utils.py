import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

def create_sample_dataframe(start_time: datetime, periods: int, freq: str = 'T') -> pd.DataFrame:
    """Creates a sample OHLCV DataFrame for testing.

    Args:
        start_time: The starting timestamp (timezone-aware recommended).
        periods: Number of data points (rows).
        freq: Pandas frequency string (e.g., 'T' for minutes, 'H' for hours).

    Returns:
        A Pandas DataFrame with timestamp and OHLCV columns.
    """
    # Ensure start_time is timezone-aware (UTC)
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    # Use 'min' instead of deprecated 'T'
    timestamps = pd.date_range(start=start_time, periods=periods, freq='min', tz=timezone.utc)

    data = {
        'timestamp': timestamps,
        'open': np.random.rand(periods) * 100 + 50000,
        'high': np.random.rand(periods) * 10 + 50100, # Ensure high >= open
        'low': np.random.rand(periods) * 10 + 49900,  # Ensure low <= open
        'close': np.random.rand(periods) * 100 + 50000,
        'volume': np.random.rand(periods) * 10 + 100
    }
    df = pd.DataFrame(data)

    # Ensure high is max and low is min of ohlc
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)

    # Convert timestamp to epoch milliseconds (int) as often stored
    # Or keep as pd.Timestamp depending on storage expectation
    # df['timestamp'] = (df['timestamp'].astype(np.int64) // 10**6)

    # Add partitioning columns often used with Parquet/Delta
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day

    return df

def generate_test_data(start_time: datetime, end_time: datetime, freq_minutes: int = 60) -> pd.DataFrame:
    """Generate OHLCV DataFrame from start_time to end_time with given frequency in minutes."""
    timestamps = pd.date_range(start=start_time, end=end_time, freq=f'{freq_minutes}min', tz=start_time.tzinfo or timezone.utc)
    periods = len(timestamps)
    data = {
        'timestamp': timestamps,
        'open': np.random.rand(periods) * 100 + 50000,
        'high': np.random.rand(periods) * 10 + 50100,
        'low': np.random.rand(periods) * 10 + 49900,
        'close': np.random.rand(periods) * 100 + 50000,
        'volume': np.random.rand(periods) * 10 + 100,
        'value': np.arange(periods) # For integration test value column
    }
    df = pd.DataFrame(data)
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    return df

def introduce_gaps(df: pd.DataFrame, gap_ranges: list) -> pd.DataFrame:
    """Remove rows in the DataFrame whose timestamps fall within any of the given (start, end) ranges."""
    mask = pd.Series([True] * len(df))
    for start, end in gap_ranges:
        mask &= ~df['timestamp'].between(start, end, inclusive='left')
    return df[mask].reset_index(drop=True)

def find_time_gaps(df: pd.DataFrame, expected_freq=None, timestamp_col: str = 'timestamp'):
    """Find missing intervals in the DataFrame's timestamp column."""
    if df.empty:
        return []
    # Normalize timestamps to pandas.Timestamp with UTC and remove duplicates
    ts = pd.to_datetime(df[timestamp_col], utc=True).drop_duplicates().sort_values()
    if expected_freq is None:
        expected_freq = pd.Timedelta(hours=1)
    expected = pd.date_range(start=ts.iloc[0], end=ts.iloc[-1], freq=expected_freq, tz='UTC')
    missing = expected.difference(ts)
    gaps = []
    if not missing.empty:
        from itertools import groupby
        from operator import itemgetter
        missing_list = list(missing)
        for k, g in groupby(enumerate(missing_list), lambda ix: ix[0] - ix[1].value):
            group = list(map(itemgetter(1), g))
            # Return as pandas.Timestamp with UTC for robust comparison
            gaps.append((pd.Timestamp(group[0]).tz_convert('UTC'), pd.Timestamp(group[-1]).tz_convert('UTC')))
    return gaps
