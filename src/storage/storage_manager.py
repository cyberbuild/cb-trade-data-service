from .interfaces import IRawDataStorage, IStorageManager

class StorageManagerImpl(IStorageManager):
    def __init__(self, raw_data_storage: IRawDataStorage):
        self.raw_data_storage = raw_data_storage

    def save_entry(self, exchange_name, coin_symbol, timestamp, data):
        return self.raw_data_storage.save_entry(exchange_name, coin_symbol, timestamp, data)

    def get_range(self, exchange_name, coin_symbol, start_time, end_time, limit=100, offset=0):
        return self.raw_data_storage.get_range(exchange_name, coin_symbol, start_time, end_time, limit, offset)

    def get_latest_entry(self, exchange_name, coin_symbol):
        return self.raw_data_storage.get_latest_entry(exchange_name, coin_symbol)

    def list_coins(self, exchange_name):
        return self.raw_data_storage.list_coins(exchange_name)

    def check_coin_exists(self, exchange_name, coin_symbol):
        return self.raw_data_storage.check_coin_exists(exchange_name, coin_symbol)
