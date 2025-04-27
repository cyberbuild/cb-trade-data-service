# Azure Blob Storage implementation for IRawDataStorage
# File: data-service/src/storage/implementations/azure_blob_storage.py

from typing import Any, List, Optional
from config import StorageConfig
from ..interfaces import IRawDataStorage
from ..path_utility import _parse_timestamp, StoragePathUtility
# Use async client
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class AzureBlobStorage(IRawDataStorage):
    # Accept StorageConfig
    def __init__(self, config: StorageConfig):
        if not config.azure_connection_string:
            msg = "Azure connection string is required for AzureBlobStorage"
            logger.error(msg)
            # Consider raising ConfigurationError from src.exceptions here
            raise ValueError(msg)

        # Use SecretStr.get_secret_value()
        self.connection_string = config.azure_connection_string.get_secret_value()
        self.container_name = config.azure_container_name
        self.service_client: Optional[BlobServiceClient] = None
        self.path_utility = StoragePathUtility() # Instantiate utility

    async def initialize(self):
        """Initializes the async BlobServiceClient and ensures container exists."""
        try:
            self.service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = self.service_client.get_container_client(self.container_name)
            try:
                await container_client.create_container()
                logger.info(f"Container '{self.container_name}' created.")
            except ResourceExistsError:
                logger.info(f"Container '{self.container_name}' already exists.")
            except HttpResponseError as e:
                logger.error(f"Error interacting with container '{self.container_name}': {e}")
                raise # Re-raise critical error
        except Exception as e:
            logger.error(f"Failed to initialize Azure Blob Storage client: {e}")
            self.service_client = None # Ensure client is None if init fails
            raise # Re-raise critical initialization errors

    async def _get_blob_client(self, blob_path: str):
        """Helper to get an async blob client."""
        if not self.service_client:
            await self.initialize() # Initialize if not already done
            if not self.service_client:
                raise RuntimeError("AzureBlobStorage client not initialized.")
        return self.service_client.get_blob_client(self.container_name, blob_path)

    # Use path_utility instance
    def _get_blob_path(self, exchange_name: str, coin_symbol: str, timestamp: Any) -> Optional[str]:
        dt = _parse_timestamp(timestamp)
        if dt is None:
            logger.error(f"Could not parse timestamp: {timestamp}")
            return None
        return self.path_utility.get_raw_data_path(exchange_name, coin_symbol, dt)

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

    # Make methods async
    async def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: Any, data: dict) -> None:
        blob_path = self._get_blob_path(exchange_name, coin_symbol, timestamp)
        if blob_path is None:
            logger.error(f"Skipping save due to invalid timestamp for {exchange_name}/{coin_symbol}.")
            return
        try:
            blob_client = await self._get_blob_client(blob_path)
            await blob_client.upload_blob(json.dumps(data), overwrite=True)
            logger.debug(f"Saved entry to {blob_path}")
        except HttpResponseError as e:
            logger.error(f"Azure error saving entry to {blob_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error saving entry to {blob_path}: {e}")

    async def get_range(self, exchange_name: str, coin_symbol: str, start_time: Any, end_time: Any, limit: int = 1000, offset: int = 0) -> List[dict]:
        parsed_start_time = _parse_timestamp(start_time)
        parsed_end_time = _parse_timestamp(end_time)

        if not parsed_start_time or not parsed_end_time:
            logger.error("Invalid start or end time provided for get_range")
            return []

        compare_start_time = parsed_start_time.replace(microsecond=0)
        compare_end_time = parsed_end_time

        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        entries = []
        blob_count = 0
        yielded_count = 0

        if not self.service_client:
            await self.initialize()
            if not self.service_client:
                raise RuntimeError("AzureBlobStorage client not initialized.")

        try:
            container_client = self.service_client.get_container_client(self.container_name)
            blob_iterator = container_client.list_blobs(name_starts_with=prefix)
            blobs_list = []
            async for blob in blob_iterator:
                blobs_list.append(blob)
            sorted_blobs = sorted(blobs_list, key=lambda b: b.name)
            for blob in sorted_blobs:
                if not blob.name.endswith('.json'):
                    continue
                file_ts = self._parse_timestamp_from_blob_name(blob.name)
                if file_ts and compare_start_time <= file_ts <= compare_end_time:
                    blob_count += 1
                    if blob_count > offset:
                        try:
                            blob_client = await self._get_blob_client(blob.name)
                            downloader = await blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
                            data_bytes = await downloader.readall()
                            data = json.loads(data_bytes)
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

    async def get_latest_entry(self, exchange_name: str, coin_symbol: str) -> Optional[dict]:
        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        latest_entry = None
        latest_ts = None

        if not self.service_client:
            await self.initialize()
            if not self.service_client:
                raise RuntimeError("AzureBlobStorage client not initialized.")

        try:
            container_client = self.service_client.get_container_client(self.container_name)
            blob_iterator = container_client.list_blobs(name_starts_with=prefix)
            latest_blob = None
            async for blob in blob_iterator:
                if not blob.name.endswith('.json'):
                    continue
                current_ts = self._parse_timestamp_from_blob_name(blob.name)
                if current_ts:
                    if latest_ts is None or current_ts > latest_ts:
                        latest_ts = current_ts
                        latest_blob = blob
            if latest_blob:
                try:
                    blob_client = await self._get_blob_client(latest_blob.name)
                    downloader = await blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
                    data_bytes = await downloader.readall()
                    data = json.loads(data_bytes)
                    latest_entry = {
                        "timestamp": latest_ts.isoformat().replace('+00:00', 'Z'),
                        "data": data
                    }
                except (HttpResponseError, json.JSONDecodeError) as e:
                    logger.error(f"Error reading or parsing latest blob {latest_blob.name}: {e}")
        except HttpResponseError as e:
            logger.error(f"Azure error listing blobs for latest entry with prefix {prefix}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in get_latest_entry for {prefix}: {e}")
        return latest_entry

    async def list_coins(self, exchange_name: str) -> List[str]:
        prefix = f"raw_data/{exchange_name}/"
        coins = set()
        if not self.service_client:
            await self.initialize()
            if not self.service_client:
                raise RuntimeError("AzureBlobStorage client not initialized.")
        try:
            container_client = self.service_client.get_container_client(self.container_name)
            async for blob in container_client.walk_blobs(name_starts_with=prefix, delimiter='/'):
                if blob.name.endswith('/'):
                    parts = blob.name.strip('/').split('/')
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

    async def check_coin_exists(self, exchange_name: str, coin_symbol: str) -> bool:
        prefix = f"raw_data/{exchange_name}/{coin_symbol}/"
        if not self.service_client:
            await self.initialize()
            if not self.service_client:
                raise RuntimeError("AzureBlobStorage client not initialized.")
        try:
            container_client = self.service_client.get_container_client(self.container_name)
            blobs_iterator = container_client.list_blobs(name_starts_with=prefix)
            async for _ in blobs_iterator:
                logger.debug(f"Coin exists: {exchange_name}/{coin_symbol}")
                return True
            logger.debug(f"Coin does not exist: {exchange_name}/{coin_symbol}")
            return False
        except HttpResponseError as e:
            logger.error(f"Azure error checking coin existence for {exchange_name}/{coin_symbol}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking coin existence for {exchange_name}/{coin_symbol}: {e}")
            return False

    async def close(self): # Add async close method
        """Closes the async BlobServiceClient."""
        if self.service_client:
            await self.service_client.close()
            self.service_client = None
            logger.info("Azure Blob Storage client closed.")
