# Storage Path Utility for cb-trade-data-service
# Implements standardized path construction for raw order book data files.

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Union, Optional

def _parse_timestamp(timestamp: Union[str, int, float, datetime]) -> Optional[datetime]:
    """Parse various timestamp formats into a timezone-aware UTC datetime object."""
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)

    if isinstance(timestamp, (int, float)):
        try:
            return datetime.fromtimestamp(timestamp, timezone.utc)
        except (ValueError, TypeError):
            logging.warning(f"Could not parse numeric timestamp: {timestamp}")
            return None

    if isinstance(timestamp, str):
        try:
            # Try standard ISO format (handles Z and offsets)
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Try custom format YYYYMMDDTHHMMSSZ
                return datetime.strptime(timestamp, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    # Try custom format YYYYMMDDTHHMMSS (assume UTC)
                    return datetime.strptime(timestamp, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    try:
                        # Try parsing as numeric string (Unix timestamp)
                        return datetime.fromtimestamp(float(timestamp), timezone.utc)
                    except (ValueError, TypeError):
                        logging.warning(f"Could not parse string timestamp: {timestamp}")
                        return None
        except Exception as e:
            logging.error(f"Unexpected error parsing string timestamp {timestamp}: {e}")
            return None

    logging.warning(f"Unsupported timestamp type: {type(timestamp)}")
    return None


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
