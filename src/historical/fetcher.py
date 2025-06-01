from typing import Optional
from storage.storage_manager import IStorageManager
from exchange_source.models import ExchangeData, Metadata
from datetime import datetime
import datetime as dt


class HistoricalFetcher:
    def __init__(self, storage_manager: IStorageManager):
        self._storage_manager = storage_manager

    async def fetch_data(
        self,
        metadata: Metadata,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
        offset: int = 0,
        **kwargs
    ) -> ExchangeData:
        """
        Fetch historical data for given metadata from storage within a time range.
        Returns data wrapped in an ExchangeData container.
        Supports pagination through limit and offset parameters.
        """
        # Get the data using storage manager with correct parameter names
        result_data = await self._storage_manager.get_range(
            metadata=metadata,
            start_date=start_time,
            end_date=end_time,
            columns=kwargs.get('columns'))
        # Return with metadata (or return None if no data found)
        if result_data is None:
            return ExchangeData(data=[], metadata=dict(metadata))

        # Calculate pagination boundaries
        total_records = len(result_data.data)

        # If offset is beyond the data, return empty result
        if offset >= total_records:
            return ExchangeData(data=[], metadata=dict(metadata))

        # Calculate end index for slicing
        start_idx = offset
        end_idx = min(offset + limit, total_records) if limit is not None else total_records

        # Slice the data
        paginated_data = result_data.data[start_idx:end_idx]
        # Create new ExchangeData with paginated data
        paginated_result = ExchangeData(data=paginated_data, metadata=result_data.metadata)

        return paginated_result

    async def get_latest_entry(self, metadata: Metadata):
        """
        Retrieve the latest entry for given metadata (exchange, coin, data_type) from storage.
        Fetches data from the last 24 hours and returns the entry with the latest timestamp.
        Returns the latest record as a dictionary or None if not found.
        """
        # Define a time range (e.g., last 24 hours)
        end_time = dt.datetime.now(dt.timezone.utc)
        start_time = end_time - dt.timedelta(days=1)

        try:
            # Fetch data for the range using the correct parameter names
            exchange_data = await self._storage_manager.get_range(
                metadata=metadata,
                start_date=start_time,
                end_date=end_time
            )

            if exchange_data is None or not exchange_data.data:
                return None

            # Find the record with the maximum timestamp
            latest_record = max(exchange_data.data, key=lambda x: x.timestamp)

            # Return the record directly since it inherits from dict
            return dict(latest_record)

        except Exception as e:
            # Log error and return None
            print(f"Error fetching latest entry: {e}")
            return None
