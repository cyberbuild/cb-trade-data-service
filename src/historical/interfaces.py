from abc import ABC, abstractmethod
from exchange_source.models import Metadata

class IHistoricalDataManager(ABC):
    @abstractmethod
    def stream_historical_data(self, metadata: Metadata, start, end, ws, chunk_size=1000):
        pass

    @abstractmethod
    def get_most_current_data(self, metadata: Metadata):
        pass
