# Ensure the src directory is in sys.path for test discovery and imports
import sys
import os
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:    sys.path.insert(0, src_path)

import pytest
import pytest_asyncio
from dotenv import load_dotenv
import shutil
import asyncio
import time # Import time for sleep
import uuid # Import uuid
from azure.storage.blob.aio import BlobServiceClient # Use async client
from pathlib import Path # Import Path
from typing import Generator, AsyncGenerator # Import Generator and AsyncGenerator

# Import backend classes and interfaces
from storage.backends.local_file_backend import LocalFileBackend
from storage.backends.azure_blob_backend import AzureBlobBackend

# Additional imports for setup_historical_data fixture
import datetime
import pandas as pd
import logging
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings
from storage.backends.istorage_backend import IStorageBackend
from storage.storage_manager import IStorageManager, IExchangeRecord, OHLCVStorageManager
from storage.path_strategy import OHLCVPathStrategy
from storage.partition_strategy import YearMonthDayPartitionStrategy
from storage.readerwriter.delta import DeltaReaderWriter
from storage.readerwriter.delta import DeltaReaderWriter
from storage.partition_strategy import YearMonthDayPartitionStrategy
from storage.path_strategy import OHLCVPathStrategy
from storage.storage_manager import OHLCVStorageManager
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings
import datetime

# Set SelectorEventLoopPolicy for Windows to fix aiodns/aiohttp/azure async issues
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    """
    Automatically load .env.test for all tests in this session.
    """
    # Correct path relative to conftest.py
    env_path = Path(__file__).parent.parent / '.env.test'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded test environment from: {env_path}")
    else:
        print(f"Warning: .env.test file not found at {env_path}")

# --- Backend Fixtures ---

@pytest.fixture(scope="function")
def local_backend() -> Generator[IStorageBackend, None, None]:
    """Fixture for LocalFileBackend using settings from .env.test, cleans up afterwards."""
    root_path_str = os.environ.get("STORAGE_LOCAL_ROOT_PATH", "./test_data_fallback")
    # Resolve the path relative to the project root if it's relative
    local_test_root_dir = Path(root_path_str)
    if not local_test_root_dir.is_absolute():
         # Assuming conftest.py is in tests/, project root is parent.parent
         project_root = Path(__file__).parent.parent
         local_test_root_dir = (project_root / local_test_root_dir).resolve()
    else:
         local_test_root_dir = local_test_root_dir.resolve()


    if local_test_root_dir.exists():
        shutil.rmtree(local_test_root_dir, ignore_errors=True) # Use ignore_errors
    local_test_root_dir.mkdir(parents=True, exist_ok=True)
    print(f"Using local test directory: {local_test_root_dir}")

    backend = LocalFileBackend(root_path=str(local_test_root_dir))
    yield backend

    # Teardown: Remove the test directory with retries
    attempts = 3
    while attempts > 0:
        try:
            if local_test_root_dir.exists():
                shutil.rmtree(local_test_root_dir)
                print(f"Cleaned up local test directory: {local_test_root_dir}")
            break
        except OSError as e:
            attempts -= 1
            if attempts == 0:
                print(f"Warning: Failed to remove test directory {local_test_root_dir}: {e}")
            else:
                print(f"Retrying cleanup for {local_test_root_dir}...")
                time.sleep(0.5) # Wait briefly before retrying

@pytest.fixture(scope="function")
async def azure_backend() -> AsyncGenerator[IStorageBackend, None]: # Async fixture
    """Fixture for AzureBlobBackend using service principal authentication."""
    account_name = os.environ.get("STORAGE_AZURE_ACCOUNT_NAME")
    use_managed_identity = os.environ.get("STORAGE_AZURE_USE_MANAGED_IDENTITY", "false").lower() == "true"
    base_container_name = os.environ.get("STORAGE_AZURE_CONTAINER_NAME", "test-container-fallback")

    # Validate service principal authentication configuration
    if not (account_name and use_managed_identity):
        pytest.skip("Service principal authentication requires STORAGE_AZURE_ACCOUNT_NAME and STORAGE_AZURE_USE_MANAGED_IDENTITY=true")

    # Generate a unique container name for this specific test function execution
    unique_container_name = f"{base_container_name}-{uuid.uuid4().hex[:8]}"

    print(f"Using service principal authentication with unique Azure test container: {unique_container_name}")
    
    # Create backend with service principal authentication
    backend = AzureBlobBackend(
        account_name=account_name, 
        container_name=unique_container_name, 
        use_managed_identity=True
    )
    
    # Use DefaultAzureCredential for service principal authentication
    from azure.identity import DefaultAzureCredential
    credential = DefaultAzureCredential()
    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_created = False
    try:
        # Create container asynchronously
        try:
            await blob_service_client.create_container(unique_container_name)
            container_created = True
            print(f"Created test container: {unique_container_name}")
        except Exception as e:
            # Catch specific exception if possible (e.g., ResourceExistsError)
            # If it already exists (unlikely with UUID), log warning but proceed
            print(f"Warning: Could not create container {unique_container_name} (may already exist?): {e}")
            # Decide if this should be a failure or just a warning
            # If creation MUST succeed, re-raise or pytest.fail()

        yield backend # Provide the backend instance to the test

    finally:
        # Teardown: Delete the test container asynchronously only if we attempted creation
        # This avoids trying to delete if setup failed early
        # Although the unique name should prevent conflicts, cleanup is good practice
        try:
            print(f"Attempting to delete test container: {unique_container_name}")
            # Add a small delay before attempting deletion (might help with eventual consistency)
            await asyncio.sleep(2) # Wait 2 seconds
            await blob_service_client.delete_container(unique_container_name)
            print(f"Deleted test container: {unique_container_name}")
        except Exception as e:
            # Catch specific exception (e.g., ResourceNotFoundError)
            print(f"Warning: Failed to delete test container {unique_container_name}: {e}")
        finally:
            # Ensure the async client is closed
            await blob_service_client.close()


# --- Parameterized StorageManager Fixture ---

@pytest.fixture(params=["local", "azure"], scope="function")
async def storage_manager(request, local_backend, azure_backend) -> AsyncGenerator[IStorageManager[IExchangeRecord], None]: # Correct type hint for async generator
    """Fixture providing a StorageManager configured with either local or azure backend."""
    # Use OHLCVStorageManager with DeltaReaderWriter, OHLCVPathStrategy, and YearMonthDayPartitionStrategy
    def make_strategy_kwargs(backend):
        return {
            'writer': DeltaReaderWriter(backend),
            'path_strategy': OHLCVPathStrategy(),
            'partition_strategy': YearMonthDayPartitionStrategy()
        }
    if request.param == "local":
        print("Configuring OHLCVStorageManager with Local Backend")        
        manager = OHLCVStorageManager(backend=local_backend, **make_strategy_kwargs(local_backend))
        yield manager

    elif request.param == "azure":
        # Check for service principal authentication configuration
        account_name = os.environ.get("STORAGE_AZURE_ACCOUNT_NAME")
        use_managed_identity = os.environ.get("STORAGE_AZURE_USE_MANAGED_IDENTITY", "false").lower() == "true"
        
        if not (account_name and use_managed_identity):
            pytest.skip("Skipping Azure test: Service principal authentication requires STORAGE_AZURE_ACCOUNT_NAME and STORAGE_AZURE_USE_MANAGED_IDENTITY=true")
        
        print("Configuring OHLCVStorageManager with Azure Backend (Service Principal)")
        manager = OHLCVStorageManager(backend=azure_backend, **make_strategy_kwargs(azure_backend))
        yield manager

    else:
        raise ValueError(f"Unknown backend type requested: {request.param}")


# --- Cleanup Fixture ---
# Keep the cleanup_test_system fixture if it's used elsewhere, otherwise remove if only for old local setup
@pytest.fixture(scope="session", autouse=True)
def cleanup_test_system():
    """
    Cleanup the old test_system directory if it exists (legacy).
    """
    yield
    # Adjust path if necessary, assuming it's relative to project root
    project_root = Path(__file__).parent.parent
    test_system_path = project_root / 'test_system'
    if test_system_path.exists():
        print(f"Cleaning up legacy test_system directory: {test_system_path}")
        shutil.rmtree(test_system_path, ignore_errors=True)

import pytest
from pathlib import Path

@pytest.fixture(scope="session", autouse=True)
def check_no_data_dirs_after_tests():
    yield  # Let all tests run
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    test_data_dir = project_root / "test_data"
    found = []
    if data_dir.exists():
        found.append(str(data_dir))
    if test_data_dir.exists():
        found.append(str(test_data_dir))
    if found:
        print(f"\n[WARNING] The following data directories exist after tests: {found}\n")
        # Optionally, uncomment to fail the test suite if these exist:
        # assert False, f"Unexpected data directories found after tests: {found}"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def setup_historical_data(storage_manager) -> AsyncGenerator[dict, None]:
    """
    Fetches recent historical data using CCXT and saves it using the storage_manager.
    This fixture runs before tests that require pre-populated data.
    It fetches 1 hour of 1m BTC/USD data from cryptocom.
    """

    settings = get_settings()
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
