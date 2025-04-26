from abc import ABC, abstractmethod

class IHistoricalDataManager(ABC):
    @abstractmethod
    def stream_historical_data(self, coin, exchange, start, end, ws):
        pass

    @abstractmethod
    def get_most_current_data(self, coin, exchange):
        pass
