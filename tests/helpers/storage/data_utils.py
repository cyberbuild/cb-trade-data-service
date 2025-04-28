import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

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
