from typing import List, Optional
from abc import ABC, abstractmethod
\
from exchange_source.models import Metadata

class IPartitionStrategy(ABC):
    """
    Abstract base class for defining partitioning strategies for data storage.
    """
    @abstractmethod
    def get_partition_cols(self, metadata: Metadata) -> Optional[List[str]]:
        """
        Determine the list of columns to partition by based on metadata.

        Args:
            metadata: The metadata associated with the data being saved.

        Returns:
            A list of column names to partition by, or None/empty list if no partitioning.
        """
        pass

class YearMonthDayPartitionStrategy(IPartitionStrategy):
    def get_partition_cols(self, metadata: Metadata) -> Optional[List[str]]:     
        return ["year", "month", "day"]

class NoPartitionStrategy(IPartitionStrategy):
    def get_partition_cols(self, metadata: Metadata) -> Optional[List[str]]:
        return None
