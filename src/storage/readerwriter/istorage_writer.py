from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import pyarrow as pa

from exchange_source.models import ExchangeData, IExchangeRecord


# Interface for storage writers
class IStorageWriter(ABC):
    @abstractmethod
    async def load_range(
        self,
        backend,
        base_path: str,
        start_time: datetime,
        end_time: datetime,
        filters: Optional[List[tuple]] = None,
        columns: Optional[List[str]] = None,
    ) -> Optional[ExchangeData[IExchangeRecord]]:
        pass

    @abstractmethod
    async def save_data(
        self,
        backend,
        base_path: str,
        data: pa.Table,
        context: Dict[str, Any],
        mode: str = "append",
        partition_cols: Optional[List[str]] = None,
        storage_options: Dict[str, Any] = None
    ) -> None:
        pass
