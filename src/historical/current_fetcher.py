from storage.storage_manager import IStorageManager

class CurrentDataFetcher:
    def __init__(self, storage_manager: IStorageManager):
        self._storage_manager = storage_manager

    def get_latest_entry(self, coin: str, exchange: str):
        """
        Retrieve the latest entry for a given coin and exchange from storage.
        Returns the latest data entry or None if not found.
        """
        return self._storage_manager.get_latest_entry(exchange, coin)
