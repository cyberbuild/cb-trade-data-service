# Storage Path Utility for cb-trade-data-service
# Implements standardized path construction for raw order book data files.

import os
from datetime import datetime, timezone
from typing import Union, Optional

def _parse_timestamp(timestamp: Union[str, int, float, datetime]) -> Optional[datetime]:
    """
    Parses various timestamp formats into a timezone-aware UTC datetime object.

    Args:
        timestamp: The timestamp to parse (datetime, int/float Unix timestamp, or ISO string).

    Returns:
        A timezone-aware datetime object in UTC, or None if parsing fails.
    """
    dt = None
    if isinstance(timestamp, datetime):
        dt = timestamp
    elif isinstance(timestamp, (int, float)):
        try:
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OSError, ValueError): # Handle potential errors like out-of-range timestamps
            return None
    elif isinstance(timestamp, str):
        try:
            # Try ISO format first (ensure it handles timezone info correctly or assumes UTC)
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00')) # Handle 'Z' for UTC
        except ValueError:
            # Try parsing as integer/float string (Unix timestamp)
            try:
                dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
            except ValueError:
                return None # Invalid string format
    else:
        # Or raise TypeError("Unsupported timestamp type")
        return None

    # Ensure the datetime object is timezone-aware and in UTC
    if dt:
        if dt.tzinfo is None:
            # If parsed from a naive format, assume UTC (or raise error if ambiguity is critical)
            # This assumption might need review based on expected inputs
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC if it's not already
            dt = dt.astimezone(timezone.utc)
    return dt


class StoragePathUtility:
    @staticmethod
    def get_raw_data_path(exchange_name: str, coin_symbol: str, timestamp: Union[str, int, float, datetime]) -> str:
        """
        Constructs the standardized path for raw 5-minute order book data.
        Path format (excluding storage_root):
        raw_data/{exchange_name}/{coin_symbol}/{year}/{month}/{day}/{timestamp_filename}.json
        """
        dt = _parse_timestamp(timestamp)
        if dt is None:
            raise ValueError(f"Invalid or unsupported timestamp format: {timestamp}")

        # ... rest of the method remains the same ...
        year = f"{dt.year:04d}"
        month = f"{dt.month:02d}"
        day = f"{dt.day:02d}"
        # Use full ISO timestamp for filename (to seconds)
        timestamp_filename = dt.strftime("%Y%m%dT%H%M%S") # Consider adding %f for microseconds if needed
        path = os.path.join(
            "raw_data",
            exchange_name,
            coin_symbol,
            year,
            month,
            day,
            f"{timestamp_filename}.json"
        )
        # Always use forward slashes for storage paths
        return path.replace(os.sep, "/")

    # Potentially add a static method wrapper if preferred
    # @staticmethod
    # def parse_timestamp(timestamp: Union[str, int, float, datetime]) -> Optional[datetime]:
    #     return _parse_timestamp(timestamp)
