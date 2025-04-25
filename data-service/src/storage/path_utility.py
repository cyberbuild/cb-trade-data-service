# Storage Path Utility for cb-trade-data-service
# Implements standardized path construction for raw order book data files.

import os
from datetime import datetime
from typing import Union

class StoragePathUtility:
    @staticmethod
    def get_raw_data_path(exchange_name: str, coin_symbol: str, timestamp: Union[str, int, float, datetime]) -> str:
        """
        Constructs the standardized path for raw 5-minute order book data.
        Path format (excluding storage_root):
        raw_data/{exchange_name}/{coin_symbol}/{year}/{month}/{day}/{timestamp_filename}.json
        """
        # Parse timestamp
        if isinstance(timestamp, datetime):
            dt = timestamp
        elif isinstance(timestamp, (int, float)):
            dt = datetime.utcfromtimestamp(timestamp)
        elif isinstance(timestamp, str):
            try:
                # Try ISO format first
                dt = datetime.fromisoformat(timestamp)
            except ValueError:
                # Try parsing as integer/float string
                try:
                    dt = datetime.utcfromtimestamp(float(timestamp))
                except Exception:
                    raise ValueError(f"Invalid timestamp string: {timestamp}")
        else:
            raise TypeError("timestamp must be datetime, int, float, or str")

        year = f"{dt.year:04d}"
        month = f"{dt.month:02d}"
        day = f"{dt.day:02d}"
        # Use full ISO timestamp for filename (to seconds)
        timestamp_filename = dt.strftime("%Y%m%dT%H%M%S")
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
