# Azure Blob Storage implementation for IRawDataStorage
# File: data-service/src/storage/implementations/azure_blob_storage.py

from typing import Any, List, Optional
# Use relative import
from ..interfaces import IRawDataStorage
from ..path_utility import _parse_timestamp, StoragePathUtility
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
import json
import os
import logging
from datetime import datetime, timezone

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AzureBlobStorage(IRawDataStorage):
    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        try:
            self.service_client = BlobServiceClient.from_connection_string(connection_string)
            self.container_client = self.service_client.get_container_client(container_name)
            try:
                self.container_client.create_container()
                logger.info(f"Container '{container_name}' created.")
            except ResourceExistsError:
                logger.info(f"Container '{container_name}' already exists.")
            except HttpResponseError as e:
                logger.error(f"Error interacting with container '{container_name}': {e}")
                raise # Re-raise after logging if it's a critical error during init
        except Exception as e:
            logger.error(f"Failed to initialize Azure Blob Storage client: {e}")
            raise # Re-raise critical initialization errors

    def _get_blob_path(self, exchange_name: str, coin_symbol: str, timestamp: Any) -> Optional[str]:
        dt = _parse_timestamp(timestamp)
        if dt is None:
            logger.error(f"Could not parse timestamp: {timestamp}")
            return None
        return StoragePathUtility.get_raw_data_path(exchange_name, coin_symbol, dt)

    def _parse_timestamp_from_blob_name(self, blob_name: str) -> Optional[datetime]:
        """Extracts filename from blob path and parses timestamp, handling Z suffix."""
        filename = blob_name.split('/')[-1]
        ts_str = filename.replace('.json', '')
        # Remove trailing 'Z' if present
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1]
        try:
            # Try ISO-like format first (YYYYMMDDTHHMMSS)
            return datetime.strptime(ts_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                # Fallback to Unix timestamp (if filenames could be Unix timestamps)
                return datetime.fromtimestamp(int(ts_str), timezone.utc)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse timestamp from blob filename part: {filename}")
                return None
        except Exception as e:
            logger.error(f"Unexpected error parsing timestamp from blob name {blob_name}: {e}")
            return None

    def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: Any, data: dict) -> None:
        blob_path = self._get_blob_path(exchange_name, coin_symbol, timestamp)
        if blob_path is None:
            logger.error(f"Skipping save due to invalid timestamp for {exchange_name}/{coin_symbol}.")
            return # Or raise an error if saving must always succeed
        try:
            blob_client = self.container_client.get_blob_client(blob_path)
            blob_client.upload_blob(json.dumps(data), overwrite=True)
            logger.debug(f"Saved entry to {blob_path}")
        except HttpResponseError as e:
            logger.error(f"Azure error saving entry to {blob_path}: {e}")
            # Decide if to raise the error or just log it
        except Exception as e:
            logger.error(f"Unexpected error saving entry to {blob_path}: {e}")
            # Decide if to raise the error

    def get_range(self, exchange_name: str, coin_symbol: str, start_time: Any, end_time: Any, limit: int = 1000, offset: int = 0) -> List[dict]:
        # Ensure start_time and end_time are timezone-aware UTC datetimes
        parsed_start_time = _parse_timestamp(start_time)
        parsed_end_time = _parse_timestamp(end_time)

        if not parsed_start_time or not parsed_end_time:
            logger.error("Invalid start or end time provided for get_range")
            return []

        # Truncate microseconds from start_time for comparison with blob names
        compare_start_time = parsed_start_time.replace(microsecond=0)
        compare_end_time = parsed_end_time # Keep original end time for comparison

        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        entries = []
        blob_count = 0
        yielded_count = 0

        try:
            blob_iterator = self.container_client.list_blobs(name_starts_with=prefix)
            sorted_blobs = sorted(blob_iterator, key=lambda b: b.name)

            for blob in sorted_blobs:
                if not blob.name.endswith('.json'):
                    continue

                file_ts = self._parse_timestamp_from_blob_name(blob.name)
                # Use truncated start time for comparison
                if file_ts and compare_start_time <= file_ts <= compare_end_time:
                    blob_count += 1
                    if blob_count > offset:
                        try:
                            # Use blob.name instead of blob object (Fix DeprecationWarning)
                            blob_client = self.container_client.get_blob_client(blob.name)
                            downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
                            data = json.loads(downloader.readall())
                            entries.append({
                                "timestamp": file_ts.isoformat().replace('+00:00', 'Z'),
                                "data": data
                            })
                            yielded_count += 1
                            if yielded_count >= limit:
                                break
                        except (HttpResponseError, json.JSONDecodeError) as e:
                            logger.error(f"Error reading or parsing blob {blob.name}: {e}")
                            continue
        except HttpResponseError as e:
            logger.error(f"Azure error listing blobs for range {prefix}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_range for {prefix}: {e}")
            return []

        return entries

    def get_latest_entry(self, exchange_name: str, coin_symbol: str) -> Optional[dict]:
        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        latest_entry = None
        latest_ts = None

        try:
            blob_iterator = self.container_client.list_blobs(name_starts_with=prefix)
            # Find the latest blob client-side
            latest_blob = None
            for blob in blob_iterator:
                # Ensure we only consider actual data files
                if not blob.name.endswith('.json'):
                    continue
                current_ts = self._parse_timestamp_from_blob_name(blob.name)
                if current_ts:
                    if latest_ts is None or current_ts > latest_ts:
                        latest_ts = current_ts
                        latest_blob = blob

            if latest_blob:
                try:
                    # Use latest_blob.name instead of latest_blob object (Fix DeprecationWarning)
                    blob_client = self.container_client.get_blob_client(latest_blob.name)
                    downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
                    data = json.loads(downloader.readall())
                    latest_entry = {
                        "timestamp": latest_ts.isoformat().replace('+00:00', 'Z'),
                        "data": data
                    }
                except (HttpResponseError, json.JSONDecodeError) as e:
                    logger.error(f"Error reading or parsing latest blob {latest_blob.name}: {e}")
                    # latest_entry remains None
        except HttpResponseError as e:
            logger.error(f"Azure error listing blobs for latest entry with prefix {prefix}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in get_latest_entry for {prefix}: {e}")

        return latest_entry

    def list_coins(self, exchange_name: str) -> List[str]:
        prefix = f"raw_data/{exchange_name}/"
        coins = set()
        try:
            # Use walk_blobs to correctly list prefixes (virtual directories)
            for blob in self.container_client.walk_blobs(name_starts_with=prefix, delimiter='/'):
                 # walk_blobs yields prefixes ending in '/'
                 if blob.name.endswith('/'):
                     parts = blob.name.strip('/').split('/')
                     # Expecting raw_data/{exchange_name}/{coin_symbol}
                     if len(parts) == 3:
                         coins.add(parts[2])
            logger.debug(f"Found coins for {exchange_name}: {list(coins)}")
            return list(coins)
        except HttpResponseError as e:
            logger.error(f"Azure error listing coins for exchange {exchange_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing coins for exchange {exchange_name}: {e}")
            return []


    def check_coin_exists(self, exchange_name: str, coin_symbol: str) -> bool:
        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        try:
            # Check if any blob exists with this prefix
            blobs_iterator = self.container_client.list_blobs(name_starts_with=prefix)
            # Check if the iterator has at least one item
            for _ in blobs_iterator:
                logger.debug(f"Coin exists: {exchange_name}/{coin_symbol}")
                return True
            logger.debug(f"Coin does not exist: {exchange_name}/{coin_symbol}")
            return False
        except HttpResponseError as e:
            logger.error(f"Azure error checking coin existence for {exchange_name}/{coin_symbol}: {e}")
            return False # Treat error as coin not existing or handle differently
        except Exception as e:
            logger.error(f"Unexpected error checking coin existence for {exchange_name}/{coin_symbol}: {e}")
            return False
