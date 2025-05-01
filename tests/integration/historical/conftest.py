# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\tests\integration\historical\conftest.py
import pytest
import pytest_asyncio
import datetime
import pandas as pd
import logging
from typing import Generator    
from storage.storage_manager import StorageManager, IStorageManager
from storage.backends.istorage_backend import IStorageBackend
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings

logger = logging.getLogger(__name__)

# Parametrize the storage_manager fixture to run tests against both backends
# Note: We request 'local_backend' and 'azure_backend' which are defined in the root conftest.py
@pytest.fixture(params=["local_backend", "azure_backend"], scope="function")
def storage_manager(request) -> Generator[IStorageManager, None, None]:
    backend_fixture_name = request.param
    backend: IStorageBackend = request.getfixturevalue(backend_fixture_name)
    manager = StorageManager(backend=backend)
    yield manager

@pytest_asyncio.fixture(scope="function")
async def setup_historical_data(storage_manager: IStorageManager):
    """
    Fetches recent historical data using CCXT and saves it using the storage_manager.
    This fixture runs before tests that require pre-populated data.
    It fetches 1 hour of 1m BTC/USD data from cryptocom.
    """
    settings = get_settings()
    # Ensure cryptocom is configured in settings if needed, or rely on public access
    ccxt_client = CCXTExchangeClient(exchange_id='cryptocom', config=settings.ccxt)
    context = {'exchange': 'cryptocom', 'coin': 'BTC/USD', 'data_type': 'ohlcv_1m'}
    interval = '1m'
    fetch_duration_hours = 1

    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=fetch_duration_hours)

    logger.info(f"Setup: Fetching data for {context['coin']} from {start_time} to {end_time}")
    try:
        fetched_data = await ccxt_client.fetch_historical_data(
            coin_symbol=context['coin'],
            start_time=start_time,
            end_time=end_time,
            interval=interval
        )

        if fetched_data:
            logger.info(f"Setup: Fetched {len(fetched_data)} records.")
            # Convert list of dicts to DataFrame
            df_to_save = pd.DataFrame(fetched_data)
            # Ensure timestamp is datetime and timezone-aware (UTC)
            df_to_save['timestamp'] = pd.to_datetime(df_to_save['timestamp'], unit='ms', utc=True)
            # Add partitioning columns if needed by storage backend (example)
            df_to_save['year'] = df_to_save['timestamp'].dt.year
            df_to_save['month'] = df_to_save['timestamp'].dt.month
            df_to_save['day'] = df_to_save['timestamp'].dt.day

            await storage_manager.save_entry(
                context=context,
                data=df_to_save,
                partition_cols=['year', 'month', 'day'] # Adjust if partitioning changes
            )
            logger.info(f"Setup: Saved {len(df_to_save)} records to storage.")
            # Yield control to the test function
            yield context # Pass context if needed by tests
        else:
            logger.warning("Setup: No data fetched from exchange.")
            yield context # Still yield context even if no data

    except Exception as e:
        logger.error(f"Setup: Error during data fetching/saving: {e}", exc_info=True)
        pytest.fail(f"Failed to set up historical data: {e}")
    finally:
        # Cleanup: Close the CCXT client
        await ccxt_client.aclose()
        logger.info("Setup: Closed CCXT client.")
        # Storage cleanup is handled by the storage_manager fixture itself
