from storage.interfaces import IStorageManager
from typing import Dict, Any, Optional
import datetime
import pandas as pd

class CurrentDataFetcher:
    def __init__(self, storage_manager: IStorageManager):
        self._storage_manager = storage_manager

    async def get_latest_entry(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Retrieve the latest entry for a given context (exchange, coin, data_type) from storage.
        Fetches data from the last 24 hours and returns the entry with the latest timestamp.
        Returns the latest data entry or None if not found.
        """
        # Define a time range (e.g., last 24 hours)
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(days=1)

        try:
            # Ensure required keys are in context
            required_keys = ['exchange', 'coin', 'data_type']
            if not all(key in context for key in required_keys):
                raise ValueError(f"Context must contain keys: {required_keys}")

            # Fetch data for the range
            df = await self._storage_manager.get_range(
                context=context,
                start_time=start_time,
                end_time=end_time,
                output_format="dataframe" # Assuming timestamp is accessible
            )

            if df.empty:
                return None

            # Find the row with the maximum timestamp
            # Assuming a 'timestamp' column exists and is comparable/sortable
            # If timestamp is milliseconds epoch, it should work directly.
            # If it's datetime objects, it should also work.
            latest_entry_row = df.loc[df['timestamp'].idxmax()]

            # Convert the row (Pandas Series) to a dictionary
            return latest_entry_row.to_dict()

        except Exception as e:
            # Log the error appropriately
            print(f"Error fetching latest entry for context {context}: {e}")
            # Depending on requirements, you might want to return None or re-raise
            return None
