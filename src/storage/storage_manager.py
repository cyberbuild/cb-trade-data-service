import logging
from typing import List, Optional

# Use absolute import from package root
from config import StorageConfig
from .interfaces import IRawDataStorage, IStorageManager

class StorageManagerImpl(IStorageManager):
    # Accept StorageConfig, though it might not be directly used here yet
    # Pass the specific IRawDataStorage implementation
    def __init__(self, storage_config: StorageConfig, raw_data_storage: IRawDataStorage):
        self.config = storage_config # Store config if needed later
        self.raw_data_storage = raw_data_storage

    # ... existing methods delegate to self.raw_data_storage ...
    async def save_entry(self, exchange_name, coin_symbol, timestamp, data):
        # Assuming IRawDataStorage methods become async
        return await self.raw_data_storage.save_entry(exchange_name, coin_symbol, timestamp, data)

    async def get_range(self, exchange_name, coin_symbol, start_time, end_time, limit=100, offset=0):
        # Assuming IRawDataStorage methods become async
        return await self.raw_data_storage.get_range(exchange_name, coin_symbol, start_time, end_time, limit, offset)

    async def get_latest_entry(self, exchange_name, coin_symbol):
        # Assuming IRawDataStorage methods become async
        return await self.raw_data_storage.get_latest_entry(exchange_name, coin_symbol)

    async def list_coins(self, exchange_name):
        # Assuming IRawDataStorage methods become async
        return await self.raw_data_storage.list_coins(exchange_name)

    async def check_coin_exists(self, exchange_name, coin_symbol):
        # Assuming IRawDataStorage methods become async
        return await self.raw_data_storage.check_coin_exists(exchange_name, coin_symbol)
