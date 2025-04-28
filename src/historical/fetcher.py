from typing import List, Any, Optional, Dict, Union
from storage.interfaces import IStorageManager
import pandas as pd
import pyarrow as pa
from datetime import datetime

class HistoricalFetcher:
    def __init__(self, storage_manager: IStorageManager):
        self._storage_manager = storage_manager

    async def fetch_data(
        self,
        context: Dict[str, Any],
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
        offset: int = 0,
        output_format: str = "dataframe",
        **kwargs # Allow extra args like filters, columns for storage manager
    ) -> Union[pd.DataFrame, pa.Table, List[Dict[str, Any]]]:
        """
        Fetch historical data for a given context from storage within a time range.
        Returns data in the specified format (dataframe, arrow, dict).
        """
        # Pass context and other relevant arguments directly to storage manager
        return await self._storage_manager.get_range(
            context=context,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset,
            output_format=output_format,
            **kwargs # Pass any other filters/columns etc.
        )
