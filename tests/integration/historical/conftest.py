# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\tests\integration\historical\conftest.py
import pytest
import pytest_asyncio
import datetime
import pandas as pd
import logging
from typing import Generator    
from storage.storage_manager import StorageManager, IStorageManager, OHLCVStorageManager
from storage.backends.istorage_backend import IStorageBackend
from storage.path_strategy import OHLCVPathStrategy
from storage.partition_strategy import YearMonthDayPartitionStrategy
from storage.readerwriter.delta import DeltaReaderWriter
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings

logger = logging.getLogger(__name__)

# Parametrize the storage_manager fixture to run tests against both backends
# Note: We request 'local_backend' and 'azure_backend' which are defined in the root conftest.py
@pytest.fixture(params=["local_backend", "azure_backend"], scope="function")
def storage_manager(request) -> Generator[IStorageManager, None, None]:
    backend_fixture_name = request.param
    backend: IStorageBackend = request.getfixturevalue(backend_fixture_name)
    manager = OHLCVStorageManager(
        backend=backend,
        writer=DeltaReaderWriter(backend),
        path_strategy=OHLCVPathStrategy(),
        partition_strategy=YearMonthDayPartitionStrategy()
    )
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
    context = {'exchange': 'cryptocom', 'coin': 'BTC/USD', 'data_type': 'ohlcv', 'interval': '1m'}
    interval = '1m'
    fetch_duration_hours = 1

    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=fetch_duration_hours)

    logger.info(f"Setup: Fetching data for {context['coin']} from {start_time} to {end_time}")
    try:
        exchange_data = await ccxt_client.fetch_ohlcv_data(
            coin_symbol=context['coin'],
            start_time=start_time,
            end_time=end_time,
            interval=interval
        )
        
        if exchange_data.data:
            logger.info(f"Setup: Fetched {len(exchange_data.data)} records.")
            
            await storage_manager.save_entry(exchange_data)
            logger.info(f"Setup: Saved {len(exchange_data.data)} records to storage.")
            yield context
        else:
            logger.warning("Setup: No data fetched from exchange.")
            yield context

    except Exception as e:
        logger.error(f"Setup: Error during data fetching/saving: {e}", exc_info=True)
        pytest.fail(f"Failed to set up historical data: {e}")
    finally:
        await ccxt_client.close()
        logger.info("Setup: Closed CCXT client.")
