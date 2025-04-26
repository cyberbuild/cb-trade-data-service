"""
DataCollectionManagerImpl: Implements IDataCollectionManager for managing real-time data collection.
"""
from typing import Any
from collection.interfaces import IDataCollectionManager
from collection.processor import RealTimeProcessor
from exchange_source.interfaces import IDataSourceConnector
from storage.storage_manager import IStorageManager

class CoinCollectionList:
    def __init__(self):
        self._coins = {}  # (exchange, coin) -> True
    def add_coin(self, exchange, coin):
        self._coins[(exchange, coin)] = True
    def has_coin(self, exchange, coin):
        return (exchange, coin) in self._coins

class DataCollectionManagerImpl(IDataCollectionManager):
    def __init__(self, datasource_connector: IDataSourceConnector, storage_manager: IStorageManager):
        self.datasource_connector = datasource_connector
        self.storage_manager = storage_manager
        self.coin_list = CoinCollectionList()
        self.processor = RealTimeProcessor(storage_manager)

    def add_coin(self, exchange_name: str, coin_symbol: str) -> bool:
        if self.coin_list.has_coin(exchange_name, coin_symbol):
            return True  # Already collecting
        client = self.datasource_connector.get_client(exchange_name)
        if not client.check_coin_availability(coin_symbol):
            return False
        client.start_realtime_stream(coin_symbol, lambda coin, data: self.processor.process_realtime_entry(exchange_name, coin, data['timestamp'], data))
        self.coin_list.add_coin(exchange_name, coin_symbol)
        return True

    def stream_realtime_data(self, exchange_name: str, coin_symbol: str, ws: Any):
        # Placeholder: would implement streaming to ws subscribers
        pass

    def check_availability(self, exchange_name: str, coin_symbol: str) -> bool:
        client = self.datasource_connector.get_client(exchange_name)
        return client.check_coin_availability(coin_symbol)
