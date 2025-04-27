import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

# --- Test Data Generation ---

def generate_test_data(start_dt: datetime, end_dt: datetime, freq_minutes: int = 1) -> pd.DataFrame:
    """Generates a DataFrame with timestamp and value columns."""
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    # Generate timestamps strictly based on frequency
    timestamps = pd.date_range(start=start_dt, end=end_dt, freq=f'{freq_minutes}min', tz='UTC')

    if timestamps.empty:
        # Handle cases where the range is smaller than the frequency
        # Check if start_dt should be included if it's the only point
        if start_dt <= end_dt:
             timestamps = pd.DatetimeIndex([start_dt])
        else:
             return pd.DataFrame({'timestamp': pd.Series(dtype='datetime64[ns, UTC]'), 'value': pd.Series(dtype='float64')})

    data = {'timestamp': timestamps, 'value': range(len(timestamps))}
    df = pd.DataFrame(data)
    # Ensure timestamp column is timezone-aware UTC
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

def introduce_gaps(df: pd.DataFrame, gaps_to_introduce: List[Tuple[datetime, datetime]]) -> pd.DataFrame:
    """Removes data within specified time ranges to create gaps."""
    df_with_gaps = df.copy()
    for gap_start, gap_end in gaps_to_introduce:
        if gap_start.tzinfo is None: gap_start = gap_start.replace(tzinfo=timezone.utc)
        if gap_end.tzinfo is None: gap_end = gap_end.replace(tzinfo=timezone.utc)
        df_with_gaps = df_with_gaps[~((df_with_gaps['timestamp'] >= gap_start) & (df_with_gaps['timestamp'] < gap_end))]
    return df_with_gaps

# --- Data Auditing Helper ---

def find_time_gaps(df: pd.DataFrame, expected_freq: timedelta = timedelta(minutes=1), timestamp_col: str = 'timestamp') -> List[Tuple[datetime, datetime]]:
    """Identifies time gaps in a DataFrame based on expected frequency."""
    if df.empty or len(df) < 2:
        return []

    df = df.sort_values(by=timestamp_col).reset_index(drop=True)
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True) # Ensure UTC

    gaps = []
    time_diffs = df[timestamp_col].diff()

    # Check gap after the first element relative to expected freq (diff is NaT here)
    # This simple check assumes the first record *should* be preceded by one 'expected_freq' earlier
    # A more robust check might need a known series start time.

    # Check gaps between subsequent elements
    gap_indices = time_diffs[time_diffs > expected_freq].index

    for idx in gap_indices:
        gap_start_time = df.loc[idx - 1, timestamp_col] + expected_freq
        gap_end_time = df.loc[idx, timestamp_col]
        if gap_start_time < gap_end_time: # Ensure the calculated gap is valid
             gaps.append((gap_start_time, gap_end_time))

    return gaps
