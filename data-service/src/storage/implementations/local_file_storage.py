import os
import json
import logging
from typing import Any, List, Optional, Tuple
from storage.interfaces import IRawDataStorage
from storage.path_utility import StoragePathUtility
from datetime import datetime, timezone, timedelta
import glob

# Configure logging (basic example)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LocalFileStorage(IRawDataStorage):
    def __init__(self, storage_root: str):
        self.storage_root = storage_root

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
                logging.warning(f"Could not parse timestamp from filename: {filename}")
                return None
        except Exception as e:
            logging.error(f"Unexpected error parsing timestamp from {filename}: {e}")
            return None

    def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: Any, data: dict) -> None:
        path = StoragePathUtility.get_raw_data_path(exchange_name, coin_symbol, timestamp)
        full_path = os.path.join(self.storage_root, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)

    def get_range(self, exchange_name: str, coin_symbol: str, start_time: Any, end_time: Any, limit: int = 1000, offset: int = 0) -> List[dict]:
        """Retrieve entries within a time range, optimized for directory structure."""
        # Ensure start_time and end_time are timezone-aware UTC datetimes
        if not isinstance(start_time, datetime):
            start_time = StoragePathUtility._parse_timestamp(start_time)
        if not isinstance(end_time, datetime):
            end_time = StoragePathUtility._parse_timestamp(end_time)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        base_path = os.path.join(self.storage_root, 'raw_data', exchange_name, coin_symbol)
        entries = []
        files_processed = 0
        files_yielded = 0

        # Iterate through relevant date directories
        current_date = start_time.date()
        end_date = end_time.date()

        while current_date <= end_date:
            year_str = f"{current_date.year:04d}"
            month_str = f"{current_date.month:02d}"
            day_str = f"{current_date.day:02d}"
            day_path = os.path.join(base_path, year_str, month_str, day_str)

            if os.path.isdir(day_path):
                # Use glob for potentially faster file listing if needed, or os.listdir
                # Sort files to process them chronologically within the day
                try:
                    day_files = sorted(os.listdir(day_path))
                except OSError as e:
                    logging.error(f"Error listing directory {day_path}: {e}")
                    current_date += timedelta(days=1)
                    continue

                for filename in day_files:
                    if filename.endswith('.json'):
                        file_ts = self._parse_timestamp_from_filename(filename)
                        if file_ts and start_time <= file_ts <= end_time:
                            files_processed += 1
                            if files_processed > offset:
                                file_path = os.path.join(day_path, filename)
                                try:
                                    with open(file_path, 'r', encoding='utf-8') as f:
                                        entry = json.load(f)
                                        # Store with timestamp for potential later re-sorting if needed
                                        entries.append((file_ts, entry))
                                        files_yielded += 1
                                        if files_yielded >= limit:
                                            # Return only the data part, sorted by timestamp
                                            entries.sort(key=lambda x: x[0])
                                            return [entry for _, entry in entries]
                                except (OSError, json.JSONDecodeError) as e:
                                    logging.error(f"Error reading or parsing file {file_path}: {e}")
                                    continue # Skip corrupted file
            current_date += timedelta(days=1)

        # Final sort if limit wasn't reached during iteration
        entries.sort(key=lambda x: x[0])
        return [entry for _, entry in entries]

    def get_latest_entry(self, exchange_name: str, coin_symbol: str) -> Optional[dict]:
        """Retrieve the latest entry, optimized by searching directories in reverse."""
        base_path = os.path.join(self.storage_root, 'raw_data', exchange_name, coin_symbol)
        latest_ts = None
        latest_entry = None
        latest_file_path = None

        try:
            # Iterate years in reverse
            years = sorted([d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d)) and d.isdigit()], reverse=True)
            for year in years:
                year_path = os.path.join(base_path, year)
                # Iterate months in reverse
                months = sorted([d for d in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, d)) and d.isdigit()], reverse=True)
                for month in months:
                    month_path = os.path.join(year_path, month)
                    # Iterate days in reverse
                    days = sorted([d for d in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, d)) and d.isdigit()], reverse=True)
                    for day in days:
                        day_path = os.path.join(month_path, day)
                        # Find the latest file within the day
                        files = sorted([f for f in os.listdir(day_path) if f.endswith('.json')], reverse=True)
                        if files:
                            # The first file in the reverse sorted list is the latest for this day
                            latest_day_file = files[0]
                            file_ts = self._parse_timestamp_from_filename(latest_day_file)
                            if file_ts:
                                # Since we iterate in reverse, the first valid timestamp found is the latest overall
                                latest_file_path = os.path.join(day_path, latest_day_file)
                                latest_ts = file_ts
                                # Now read the file content
                                try:
                                    with open(latest_file_path, 'r', encoding='utf-8') as f:
                                        latest_entry = json.load(f)
                                    return latest_entry # Found the latest, return immediately
                                except (OSError, json.JSONDecodeError) as e:
                                    logging.error(f"Error reading or parsing latest file candidate {latest_file_path}: {e}")
                                    # Continue searching in case this file was corrupt
                                    latest_ts = None # Reset to allow finding older files
                                    latest_file_path = None
        except FileNotFoundError:
            logging.info(f"Base path not found for {exchange_name}/{coin_symbol}. No entries exist.")
            return None
        except OSError as e:
            logging.error(f"Error traversing directory structure for {exchange_name}/{coin_symbol}: {e}")
            return None # Indicate error or inability to determine latest

        return None # Return None if no valid entries found

    def list_coins(self, exchange_name: str) -> List[str]:
        coins_path = os.path.join(self.storage_root, 'raw_data', exchange_name)
        if not os.path.exists(coins_path):
            return []
        return [d for d in os.listdir(coins_path) if os.path.isdir(os.path.join(coins_path, d))]

    def check_coin_exists(self, exchange_name: str, coin_symbol: str) -> bool:
        coin_path = os.path.join(self.storage_root, 'raw_data', exchange_name, coin_symbol)
        return os.path.isdir(coin_path)
