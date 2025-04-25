# Azure Blob Storage implementation for IRawDataStorage
# File: data-service/src/storage/implementations/azure_blob_storage.py

from typing import Any, List, Optional
from ..interfaces import IRawDataStorage
from ..path_utility import _parse_timestamp, StoragePathUtility
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
import json
import os
import logging
from datetime import datetime

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
        start_dt = _parse_timestamp(start_time)
        end_dt = _parse_timestamp(end_time)

        if start_dt is None or end_dt is None:
            logger.error(f"Invalid start or end time for get_range: {start_time}, {end_time}")
            return []
        if start_dt > end_dt:
            logger.warning(f"Start time {start_dt} is after end time {end_dt}. Returning empty list.")
            return []

        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        logger.debug(f"Listing blobs with prefix: {prefix}")

        filtered_blob_names = []
        try:
            blobs_iterator = self.container_client.list_blobs(name_starts_with=prefix)
            for blob in blobs_iterator:
                try:
                    # Extract timestamp string from the filename part of the blob name
                    filename = os.path.basename(blob.name)
                    ts_str = os.path.splitext(filename)[0]
                    ts_dt = _parse_timestamp(ts_str)

                    if ts_dt and start_dt <= ts_dt <= end_dt:
                        filtered_blob_names.append((ts_dt, blob.name)) # Store tuple for sorting
                except Exception as e:
                    logger.warning(f"Could not parse timestamp or process blob name {blob.name}: {e}")
                    continue # Skip this blob if its name is problematic

            # Sort by timestamp
            filtered_blob_names.sort(key=lambda x: x[0])

            # Apply pagination to the names list
            paginated_blob_names = filtered_blob_names[offset : offset + limit]

            entries = []
            for _, blob_name in paginated_blob_names:
                try:
                    blob_client = self.container_client.get_blob_client(blob_name)
                    downloader = blob_client.download_blob()
                    data_bytes = downloader.readall()
                    data = json.loads(data_bytes)
                    entries.append(data)
                    logger.debug(f"Successfully retrieved and parsed {blob_name}")
                except HttpResponseError as e:
                    logger.error(f"Azure error downloading blob {blob_name}: {e}")
                    # Decide whether to continue or stop if one blob fails
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON from blob {blob_name}: {e}")
                    # Decide whether to continue or stop
                except Exception as e:
                    logger.error(f"Unexpected error processing blob {blob_name}: {e}")
                    # Decide whether to continue or stop

            return entries

        except HttpResponseError as e:
            logger.error(f"Azure error listing blobs with prefix {prefix}: {e}")
            return [] # Return empty list on listing error
        except Exception as e:
            logger.error(f"Unexpected error in get_range for {prefix}: {e}")
            return []

    def get_latest_entry(self, exchange_name: str, coin_symbol: str) -> Optional[dict]:
        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        logger.debug(f"Getting latest entry for prefix: {prefix}")
        latest_blob = None
        latest_dt = None

        try:
            blobs_iterator = self.container_client.list_blobs(name_starts_with=prefix)
            # Iterate through blobs to find the one with the latest timestamp based on name
            for blob in blobs_iterator:
                 try:
                    filename = os.path.basename(blob.name)
                    ts_str = os.path.splitext(filename)[0]
                    ts_dt = _parse_timestamp(ts_str)
                    if ts_dt:
                        if latest_dt is None or ts_dt > latest_dt:
                            latest_dt = ts_dt
                            latest_blob = blob
                 except Exception as e:
                     logger.warning(f"Could not parse timestamp or process blob name {blob.name} while finding latest: {e}")
                     continue # Skip blobs with problematic names

            if latest_blob:
                try:
                    blob_client = self.container_client.get_blob_client(latest_blob.name)
                    downloader = blob_client.download_blob()
                    data_bytes = downloader.readall()
                    data = json.loads(data_bytes)
                    logger.debug(f"Successfully retrieved latest entry: {latest_blob.name}")
                    return data
                except HttpResponseError as e:
                    logger.error(f"Azure error downloading latest blob {latest_blob.name}: {e}")
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON from latest blob {latest_blob.name}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error processing latest blob {latest_blob.name}: {e}")
            else:
                 logger.info(f"No valid blobs found for latest entry under prefix {prefix}")

        except HttpResponseError as e:
            logger.error(f"Azure error listing blobs for latest entry with prefix {prefix}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in get_latest_entry for {prefix}: {e}")

        return None # Return None if no entry found or error occurred

    def list_coins(self, exchange_name: str) -> List[str]:
        prefix = f"raw_data/{exchange_name}/"
        coins = set()
        try:
            # Use walk_blobs which implicitly uses delimiter if needed, or list_blobs with explicit delimiter
            # For listing immediate "directories", list_blobs with delimiter is more direct
            blob_iterator = self.container_client.list_blobs(name_starts_with=prefix, delimiter='/')

            for blob in blob_iterator:
                 # When using delimiter='/', blob.name for a "directory" will end with '/'
                 # Extract the directory name part
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
