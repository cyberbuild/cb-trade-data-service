import os
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Any

from config import StorageConfig # Import config
from ..interfaces import IRawDataStorage
from ..path_utility import StoragePathUtility, _parse_timestamp

logger = logging.getLogger(__name__) # Add logger

class LocalFileStorage(IRawDataStorage):
    def __init__(self, config: StorageConfig): # Accept StorageConfig
        self.root_path = Path(config.local_root_path) # Use config value
        self.path_utility = StoragePathUtility() # Instantiate utility
        # Ensure root path exists
        try:
            self.root_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Local storage root initialized at: {self.root_path}")
        except OSError as e:
            logger.error(f"Failed to create local storage root directory {self.root_path}: {e}")
            raise # Re-raise critical initialization error

    def _parse_timestamp_from_filename(self, filename: str) -> Optional[datetime]:
        """Parse timestamp from filename, trying ISO format then Unix timestamp."""
        ts_str = filename.replace('.json', '')
        try:
            # Try ISO-like format first
            return datetime.strptime(ts_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                # Fallback to Unix timestamp
                return datetime.fromtimestamp(int(ts_str), timezone.utc)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse timestamp from filename: {filename}")
                return None
        except Exception as e:
            logger.error(f"Unexpected error parsing timestamp from {filename}: {e}")
            return None

    async def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: Any, data: dict) -> None:
        # Use the path utility instance
        relative_path = self.path_utility.get_raw_data_path(exchange_name, coin_symbol, timestamp)
        full_path = self.root_path / relative_path
        try:
            # Use async file operations (requires aiofiles)
            import aiofiles
            full_path.parent.mkdir(parents=True, exist_ok=True) # Still sync for dir creation
            async with aiofiles.open(full_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data))
            logger.debug(f"Saved entry to {full_path}")
        except OSError as e:
            logger.error(f"Error creating directory or writing file {full_path}: {e}")
            # Decide if to raise StorageError
        except Exception as e:
            logger.error(f"Unexpected error saving entry to {full_path}: {e}")
            # Decide if to raise StorageError

    async def get_latest_entry(self, exchange_name: str, coin_symbol: str) -> Optional[dict]:
        """Retrieve the latest entry, optimized by searching directories in reverse."""
        base_path = self.root_path / 'raw_data' / exchange_name / coin_symbol
        latest_ts = None
        latest_entry_data = None
        latest_file_path = None

        try:
            if not base_path.is_dir():
                 logger.info(f"Base path not found for {exchange_name}/{coin_symbol}. No entries exist.")
                 return None

            # Iterate years in reverse
            years = sorted([d.name for d in base_path.iterdir() if d.is_dir() and d.name.isdigit()], reverse=True)
            for year in years:
                year_path = base_path / year
                # Iterate months in reverse
                months = sorted([d.name for d in year_path.iterdir() if d.is_dir() and d.name.isdigit()], reverse=True)
                for month in months:
                    month_path = year_path / month
                    # Iterate days in reverse
                    days = sorted([d.name for d in month_path.iterdir() if d.is_dir() and d.name.isdigit()], reverse=True)
                    for day in days:
                        day_path = month_path / day
                        # Find the latest file within the day
                        files = sorted([f.name for f in day_path.iterdir() if f.is_file() and f.name.endswith('.json')], reverse=True)
                        if files:
                            latest_day_file = files[0]
                            file_ts = self._parse_timestamp_from_filename(latest_day_file)
                            if file_ts:
                                latest_file_path = day_path / latest_day_file
                                latest_ts = file_ts
                                try:
                                    # Use async file operations
                                    import aiofiles
                                    async with aiofiles.open(latest_file_path, 'r', encoding='utf-8') as f:
                                        content = await f.read()
                                        latest_entry_data = json.loads(content)
                                    return {
                                        "timestamp": latest_ts.isoformat().replace('+00:00', 'Z'),
                                        "data": latest_entry_data
                                    }
                                except (OSError, json.JSONDecodeError) as e:
                                    logger.error(f"Error reading or parsing latest file candidate {latest_file_path}: {e}")
                                    latest_ts = None
                                    latest_file_path = None
        except OSError as e:
            logger.error(f"Error traversing directory structure for {exchange_name}/{coin_symbol}: {e}")
            return None

        return None

    async def get_range(self, exchange_name: str, coin_symbol: str, start_time: Any, end_time: Any, limit: int = 1000, offset: int = 0) -> List[dict]:
        """Retrieve entries within a time range, optimized for directory structure."""
        # Ensure start_time and end_time are timezone-aware UTC datetimes
        if not isinstance(start_time, datetime):
            # Use the imported module-level function
            start_time = _parse_timestamp(start_time)
        if not isinstance(end_time, datetime):
            # Use the imported module-level function
            end_time = _parse_timestamp(end_time)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        base_path = self.root_path / 'raw_data' / exchange_name / coin_symbol
        entries_with_ts = []
        files_processed = 0
        files_yielded = 0

        current_date = start_time.date()
        end_date = end_time.date()

        while current_date <= end_date:
            year_str = f"{current_date.year:04d}"
            month_str = f"{current_date.month:02d}"
            day_str = f"{current_date.day:02d}"
            day_path = base_path / year_str / month_str / day_str

            if day_path.is_dir():
                try:
                    # Use pathlib iteration, sort names
                    day_files = sorted([f.name for f in day_path.iterdir() if f.is_file()])
                except OSError as e:
                    logger.error(f"Error listing directory {day_path}: {e}")
                    current_date += timedelta(days=1)
                    continue

                for filename in day_files:
                    if filename.endswith('.json'):
                        file_ts = self._parse_timestamp_from_filename(filename)
                        if file_ts and start_time <= file_ts <= end_time:
                            files_processed += 1
                            if files_processed > offset:
                                file_path = day_path / filename
                                try:
                                    # Use async file operations
                                    import aiofiles
                                    async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                                        content = await f.read()
                                        entry_data = json.loads(content)
                                        entries_with_ts.append((file_ts, entry_data))
                                        files_yielded += 1
                                        if files_yielded >= limit:
                                            # Sort and format
                                            entries_with_ts.sort(key=lambda x: x[0])
                                            return [
                                                {
                                                    "timestamp": ts.isoformat().replace('+00:00', 'Z'),
                                                    "data": data
                                                }
                                                for ts, data in entries_with_ts
                                            ]
                                except (OSError, json.JSONDecodeError) as e:
                                    logger.error(f"Error reading or parsing file {file_path}: {e}")
                                    continue
            current_date += timedelta(days=1)

        # Final sort and format
        entries_with_ts.sort(key=lambda x: x[0])
        return [
            {
                "timestamp": ts.isoformat().replace('+00:00', 'Z'),
                "data": data
            }
            for ts, data in entries_with_ts
        ]

    async def list_coins(self, exchange_name: str) -> List[str]:
        coins_path = self.root_path / 'raw_data' / exchange_name
        if not coins_path.is_dir():
            return []
        try:
            return [d.name for d in coins_path.iterdir() if d.is_dir()]
        except OSError as e:
            logger.error(f"Error listing directories in {coins_path}: {e}")
            return []

    async def check_coin_exists(self, exchange_name: str, coin_symbol: str) -> bool:
        coin_path = self.root_path / 'raw_data' / exchange_name / coin_symbol
        # Check if it exists and is a directory
        return coin_path.is_dir()
