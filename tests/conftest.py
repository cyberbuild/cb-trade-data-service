# Ensure the src directory is in sys.path for test discovery and imports
import sys
import os

src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import pytest
import pytest_asyncio
from dotenv import load_dotenv
import shutil
import asyncio
import time
import uuid
from azure.storage.blob.aio import BlobServiceClient
from pathlib import Path
from typing import Generator, AsyncGenerator, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.backends.azure_blob_backend import AzureBlobBackend
import datetime
import pandas as pd
import logging  # Ensure logging is imported

# Import backend classes and interfaces
from storage.backends.local_file_backend import LocalFileBackend
from storage.backends.azure_blob_backend import AzureBlobBackend
from storage.backends.istorage_backend import IStorageBackend

# Import components for StorageManager and setup_historical_data
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings, Settings
from storage.storage_manager import (
    IStorageManager,
    IExchangeRecord,
    OHLCVStorageManager,
)
from storage.path_strategy import OHLCVPathStrategy
from storage.partition_strategy import YearMonthDayPartitionStrategy
from storage.readerwriter.delta import DeltaReaderWriter

# Define logger for potential use in fixtures or other conftest utilities
logger = logging.getLogger(__name__)

# Set SelectorEventLoopPolicy for Windows to fix aiodns/aiohttp/azure async issues
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    """
    Automatically load .env.test for all tests in this session.
    """
    env_path = Path(__file__).parent.parent / ".env.test"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded test environment from: {env_path}")
    else:
        print(f"Warning: .env.test file not found at {env_path}")


def log_relevant_env_vars(logger_instance, context_message=""):
    """Logs relevant environment variables for debugging configuration issues."""
    if context_message:
        logger_instance.info(f"Logging environment variables: {context_message}")
    relevant_vars = {
        k: v
        for k, v in os.environ.items()
        if k.startswith(("STORAGE_", "CCXT_", "PYTEST_"))
    }
    if not relevant_vars:
        logger_instance.info(
            "No relevant (STORAGE_*, CCXT_*, PYTEST_*) environment variables found."
        )
    else:
        for key, value in relevant_vars.items():
            logger_instance.info(f"ENV: {key}={value}")


@pytest.fixture
def env_logger_func():
    """Provides the log_relevant_env_vars function as a fixture."""
    return log_relevant_env_vars


# --- Backend Fixtures ---
def test_settings() -> Settings:
    """Load test settings, fallback to defaults if .env.test doesn't exist."""
    import os
    if os.path.exists(".env.test"):
        settings = get_settings(env_file=".env.test")
    else:
        # No env file - just use environment variables and defaults
        settings = get_settings(env_file=None)
    return settings


@pytest.fixture(scope="function")
def local_backend() -> Generator[IStorageBackend, None, None]:
    """Fixture for LocalFileBackend using settings from .env.test, cleans up afterwards."""
    settings = test_settings()

    # Get STORAGE_ROOT_PATH from settings
    storage_root_path = settings.storage.root_path
   
    project_root = Path(__file__).parent.parent.resolve()

    # Check if STORAGE_ROOT_PATH is relative or absolute
    if Path(storage_root_path).is_absolute():
        # Use the absolute path from STORAGE_ROOT_PATH
        local_test_root_dir = Path(storage_root_path)
    else:
        # Make relative path relative to the parent folder (project root)
        local_test_root_dir = project_root / storage_root_path

    # Add unique identifier to avoid conflicts between test runs
    local_test_root_dir = local_test_root_dir / f"run_{uuid.uuid4().hex[:8]}"

    print(f"Project root: {project_root}")
    print(f"STORAGE_ROOT_PATH: {storage_root_path}")
    print(f"Test directory: {local_test_root_dir}")

    if local_test_root_dir.exists():
        shutil.rmtree(local_test_root_dir, ignore_errors=True)
    local_test_root_dir.mkdir(parents=True, exist_ok=True)

    backend = LocalFileBackend(root_path=str(local_test_root_dir))
    yield backend

    attempts = 3
    while attempts > 0:
        try:
            if local_test_root_dir.exists():
                shutil.rmtree(local_test_root_dir)
                print(f"Cleaned up local test directory: {local_test_root_dir}")

            # Clean up parent test_data directory if it's empty
            parent_test_data = project_root / storage_root_path
            if parent_test_data.exists() and parent_test_data.name == "test_data":
                try:
                    parent_test_data.rmdir()  # Only removes if empty
                    print(f"Cleaned up empty parent directory: {parent_test_data}")
                except OSError:
                    # Directory not empty, which is fine
                    pass
            break
        except OSError as e:
            attempts -= 1
            if attempts == 0:
                print(
                    f"Warning: Failed to remove test directory {local_test_root_dir}: {e}"
                )
            else:
                print(f"Retrying cleanup for {local_test_root_dir}...")
                time.sleep(0.5)


class AzureBackendWrapper:
    """Wrapper for Azure backend that provides explicit async cleanup."""
    
    def __init__(self, backend: "AzureBlobBackend", container_name: str):
        self.backend = backend
        self.container_name = container_name
        self._is_setup = False
        self._is_closed = False
    
    async def __aenter__(self):
        if not self._is_setup:
            await self.backend.__aenter__()
            self._is_setup = True
        return self.backend
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._is_setup and not self._is_closed:
            await self._cleanup()
    
    async def _cleanup(self):
        """Robust cleanup of Azure resources."""
        if self._is_closed:
            return
        
        self._is_closed = True
        cleanup_errors = []
        
        try:
            # Close the backend context manager
            if self._is_setup:
                await self.backend.__aexit__(None, None, None)
        except Exception as e:
            cleanup_errors.append(f"Backend context cleanup: {e}")
        
        try:
            # Additional container cleanup attempt
            if self.backend._service_client and not self.backend._service_client._client._session.closed:
                print(f"Final cleanup: deleting test container {self.container_name}")
                await self.backend._service_client.delete_container(self.container_name)
                print(f"Successfully deleted test container: {self.container_name}")
        except Exception as e:
            cleanup_errors.append(f"Container cleanup: {e}")
        
        try:
            # Ensure backend close is called
            await self.backend.close()
        except Exception as e:
            cleanup_errors.append(f"Backend close: {e}")
        
        if cleanup_errors:
            print(f"Cleanup warnings for container {self.container_name}: {'; '.join(cleanup_errors)}")


@pytest.fixture(scope="function")
async def azure_backend() -> IStorageBackend:
    """Fixture for AzureBlobBackend using settings from .env.test, manages container lifecycle with unique names."""
    settings = test_settings()

    # Get Azure settings from settings object if available, otherwise fallback to environment
    if hasattr(settings.storage, "container_name"):
        base_container_name = settings.storage.container_name
        use_identity = str(settings.storage.use_managed_identity)
        az_account_name = settings.storage.account_name
    else:
        base_container_name = os.environ.get(
            "STORAGE_AZURE_CONTAINER_NAME", "test-container-fallback"
        )
        use_identity = os.environ.get("STORAGE_AZURE_USE_MANAGED_IDENTITY", "true")
        az_account_name = os.environ.get("STORAGE_AZURE_ACCOUNT_NAME", "unknown")

    # Create unique container name per test to avoid thread safety issues
    unique_container_name = f"{base_container_name}-{uuid.uuid4().hex[:8]}"

    backend = AzureBlobBackend(
        container_name=unique_container_name,
        use_managed_identity=use_identity,
        account_name=az_account_name,
    )
    
    wrapper = AzureBackendWrapper(backend, unique_container_name)
    
    try:
        # Setup the backend
        async with wrapper as configured_backend:
            return configured_backend
    except Exception as e:
        # Ensure cleanup happens even if setup fails
        await wrapper._cleanup()
        raise


# --- Async Azure cleanup helper for thread safety ---


async def _cleanup_azure_test_data_async(container_name: str, account_name: str):
    """
    Async cleanup of test folders in Azure container for thread safety.
    """
    try:
        from azure.identity.aio import DefaultAzureCredential
        from azure.storage.blob.aio import BlobServiceClient

        async with DefaultAzureCredential() as credential:
            account_url = f"https://{account_name}.blob.core.windows.net"

            async with BlobServiceClient(
                account_url=account_url, credential=credential
            ) as blob_service_client:
                container_client = blob_service_client.get_container_client(
                    container_name
                )

                # List and delete test folders
                test_folders = set()
                async for blob in container_client.list_blobs(name_starts_with="test-"):
                    parts = blob.name.split("/")
                    if len(parts) > 1 and parts[0].startswith("test-"):
                        test_folders.add(parts[0])

                if test_folders:
                    print(f"Found {len(test_folders)} test folders to clean up")
                    for folder in test_folders:
                        try:
                            deleted_count = 0
                            async for blob in container_client.list_blobs(
                                name_starts_with=f"{folder}/"
                            ):
                                try:
                                    await container_client.delete_blob(blob.name)
                                    deleted_count += 1
                                except Exception:
                                    pass  # Ignore individual blob deletion failures

                            if deleted_count > 0:
                                print(
                                    f"Cleaned up folder '{folder}' ({deleted_count} items)"
                                )
                        except Exception as e:
                            print(f"Warning: Failed to clean folder '{folder}': {e}")

    except Exception as e:
        print(f"Warning: Async Azure cleanup failed: {e}")


# --- Original sync fixtures ---

# --- Shared Path Strategy Fixture ---


@pytest.fixture(scope="function")
def test_path_strategy():
    """Provides a single test path strategy instance for use across setup and test fixtures."""
    from tests.helpers.path_test_strategy import OHLCVTestPathStrategy

    return OHLCVTestPathStrategy()


# --- Parameterized StorageManager Fixture ---


@pytest.fixture(scope="function")
async def storage_manager(
    request, local_backend, azure_backend, test_path_strategy
) -> AsyncGenerator[IStorageManager[IExchangeRecord], None]:
    """Fixture providing a StorageManager configured with either local or azure backend."""

    def make_strategy_kwargs(backend_instance):  # Renamed param for clarity
        from storage.backends.azure_blob_backend import AzureBlobBackend

        # Always use the shared test_path_strategy for consistency across setup and test fixtures
        # This ensures cross-backend tests work properly by using the same path strategy
        path_strategy = test_path_strategy

        return {
            "writer": DeltaReaderWriter(backend_instance),
            "path_strategy": path_strategy,
            "partition_strategy": YearMonthDayPartitionStrategy(),
        }

    # Get backend type from request parameter (passed via indirect parametrization)
    backend_type = getattr(request, "param", "local")  # Default to local if no param

    current_backend = None
    if backend_type == "local":
        print("Configuring OHLCVStorageManager with Local Backend")
        current_backend = local_backend
    elif backend_type == "azure":
        print("Configuring OHLCVStorageManager with Azure Backend")
        current_backend = azure_backend
    else:
        raise ValueError(f"Unknown backend type requested: {backend_type}")

    manager = OHLCVStorageManager(
        backend=current_backend, **make_strategy_kwargs(current_backend)
    )
    yield manager
    # No explicit close for manager here; its backend is closed by its own fixture (local_backend/azure_backend)


@pytest.fixture(scope="function")
async def setup_historical_data(
    request, local_backend, azure_backend, test_path_strategy
) -> AsyncGenerator[dict, None]:
    """
    Fetches recent historical data using CCXT and saves it using a dedicated storage_manager instance.
    This fixture runs before tests that require pre-populated data.
    It fetches 1 hour of 1m BTC/USD data from cryptocom."""

    log_relevant_env_vars(
        logger, context_message="Inside setup_historical_data before settings load"
    )
    # Get backend type from request parameter (passed via indirect parametrization)
    backend_type = getattr(request, "param", "local")  # Default to local if no param

    current_backend_for_setup = None
    if backend_type == "local":
        current_backend_for_setup = local_backend
    elif backend_type == "azure":
        current_backend_for_setup = azure_backend
    else:
        raise ValueError(
            f"Unknown backend type requested for setup_historical_data: {backend_type}"
        )

    def make_strategy_kwargs_for_setup(backend_instance):
        from storage.backends.azure_blob_backend import AzureBlobBackend

        # Always use the shared test_path_strategy for consistency across setup and test fixtures
        # This ensures cross-backend tests work properly by using the same path strategy
        path_strategy = test_path_strategy

        return {
            "writer": DeltaReaderWriter(backend_instance),
            "path_strategy": path_strategy,
            "partition_strategy": YearMonthDayPartitionStrategy(),
        }

    # This is a storage manager instance specifically for this setup task.
    # It uses the resolved backend fixture (local_backend or azure_backend).
    # Its backend (current_backend_for_setup) will be closed by the respective local_backend/azure_backend fixture.
    setup_sm = OHLCVStorageManager(
        backend=current_backend_for_setup,
        **make_strategy_kwargs_for_setup(current_backend_for_setup),
    )

    # Explicitly load .env.test for test configurations
    settings = test_settings()
    ccxt_client = CCXTExchangeClient(exchange_id="cryptocom", config=settings.ccxt)
    context = {
        "exchange": "cryptocom",
        "coin": "BTC/USD",
        "data_type": "ohlcv",
        "interval": "1m",
    }
    interval = "1m"
    fetch_duration_hours = 1

    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=fetch_duration_hours)

    print(
        f"Setup ({backend_type}): Fetching data for {context['coin']} from {start_time} to {end_time}"
    )
    try:
        exchange_data = await ccxt_client.fetch_ohlcv_data(
            coin_symbol=context["coin"],
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )

        if exchange_data.data:
            print(f"Setup ({backend_type}): Fetched {len(exchange_data.data)} records.")
            await setup_sm.save_entry(exchange_data)
            print(
                f"Setup ({backend_type}): Saved {len(exchange_data.data)} records to storage."
            )
            yield context
        else:
            print(f"Setup ({backend_type}): No data fetched from exchange.")
            yield context

    except Exception as e:
        print(f"Setup ({backend_type}): Error during data fetching/saving: {e}")
        pytest.fail(f"Failed to set up historical data for {backend_type}: {e}")
    finally:
        if ccxt_client:  # Ensure client exists before closing
            await ccxt_client.close()
            print(f"Setup ({backend_type}): Closed CCXT client.")
        # The setup_sm's backend (current_backend_for_setup) is managed by its own fixture (local_backend or azure_backend)
        # No need to explicitly close setup_sm here if its backend is managed elsewhere.


# --- Cleanup Fixture ---
@pytest.fixture(scope="session", autouse=True)
def cleanup_local_system():
    """
    Cleanup the old test_system directory if it exists (legacy).
    """
    yield

    project_root = Path(__file__).parent.parent
    test_system_path = project_root / "test_system"

    if test_system_path.exists():
        print(f"Cleaning up legacy test_system directory: {test_system_path}")
        shutil.rmtree(test_system_path, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def cleanup_data_dirs_after_tests():
    yield  # Let all tests run
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    test_data_dir = project_root / "test_data"

    cleaned = []
    if test_data_dir.exists():
        try:
            shutil.rmtree(test_data_dir, ignore_errors=True)
            cleaned.append(str(test_data_dir))
        except Exception as e:
            print(f"Warning: Failed to clean up {test_data_dir}: {e}")

    if data_dir.exists():
        # Only clean if it looks like test data (contains run_ directories)
        run_dirs = [
            d for d in data_dir.iterdir() if d.is_dir() and d.name.startswith("run_")
        ]
        if run_dirs:
            try:
                for run_dir in run_dirs:
                    shutil.rmtree(run_dir, ignore_errors=True)
                # Try to remove data dir if it's now empty
                try:
                    data_dir.rmdir()
                    cleaned.append(str(data_dir))
                except OSError:
                    pass  # Not empty, leave it
            except Exception as e:
                print(f"Warning: Failed to clean up test data in {data_dir}: {e}")

    if cleaned:
        print(f"Cleaned up test directories: {cleaned}")


class AzureStorageCleaner:
    def __init__(self, account_name: str, container_name: str, credential=None):
        """
        Initialize the Azure Storage Cleaner using Azure Identity.

        Args:
            account_name: Azure Storage account name
            container_name: Name of the container to clean
            credential: Azure credential (uses DefaultAzureCredential if None)
        """
        from azure.storage.blob import BlobServiceClient
        from azure.identity import DefaultAzureCredential

        if credential is None:
            credential = DefaultAzureCredential()

        account_url = f"https://{account_name}.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        self.container_name = container_name
        self.container_client = self.blob_service_client.get_container_client(
            container_name
        )

    def list_folders_with_prefix(self, prefix: str = "test-"):
        """
        List all folders that start with the given prefix.

        Args:
            prefix: Folder prefix to search for

        Returns:
            List of folder names with the prefix
        """
        folders = set()

        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix)

            for blob in blobs:
                parts = blob.name.split("/")
                if len(parts) > 1:
                    if parts[0].startswith(prefix):
                        folders.add(parts[0])

            return list(folders)

        except Exception as e:
            print(f"Error listing folders: {str(e)}")
            raise

    def get_all_paths_in_folder(self, folder_name: str):
        """
        Get all blob paths and directory paths in a folder for hierarchical namespace.

        Args:
            folder_name: Name of the folder

        Returns:
            Tuple of (blob_paths, directory_paths)
        """
        prefix = f"{folder_name}/" if not folder_name.endswith("/") else folder_name
        blob_paths = []
        directory_paths = set()

        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix)

            for blob in blobs:
                blob_paths.append(blob.name)

                path_parts = blob.name.split("/")
                for i in range(1, len(path_parts)):
                    dir_path = "/".join(path_parts[:i])
                    if dir_path.startswith(prefix):
                        directory_paths.add(dir_path)

            directory_paths.add(folder_name.rstrip("/"))

            return blob_paths, directory_paths

        except Exception as e:
            print(f"Error getting paths in folder: {str(e)}")
            raise

    def delete_blobs_and_directories(self, blob_paths, directory_paths):
        """
        Delete blobs first, then delete directories in reverse order (deepest first).

        Args:
            blob_paths: List of blob paths to delete
            directory_paths: Set of directory paths to delete

        Returns:
            Number of successfully deleted items
        """
        from azure.core.exceptions import ResourceNotFoundError

        deleted_count = 0
        failed_count = 0

        for blob_path in blob_paths:
            try:
                blob_client = self.container_client.get_blob_client(blob_path)
                blob_client.delete_blob()
                deleted_count += 1
            except ResourceNotFoundError:
                pass
            except Exception as e:
                print(f"Warning: Failed to delete blob {blob_path}: {str(e)}")
                failed_count += 1

        sorted_dirs = sorted(directory_paths, key=lambda x: x.count("/"), reverse=True)

        for dir_path in sorted_dirs:
            try:
                blob_client = self.container_client.get_blob_client(dir_path)
                blob_client.delete_blob()
                deleted_count += 1
            except ResourceNotFoundError:
                pass
            except Exception as e:
                pass

        return deleted_count

    def delete_folder_contents(self, folder_name: str):
        """
        Delete all contents of a specific folder, handling hierarchical namespace.

        Args:
            folder_name: Name of the folder to delete

        Returns:
            Number of items deleted
        """
        try:
            blob_paths, directory_paths = self.get_all_paths_in_folder(folder_name)

            if not blob_paths and len(directory_paths) <= 1:
                return 0

            return self.delete_blobs_and_directories(blob_paths, directory_paths)

        except Exception as e:
            print(f"Error deleting folder contents: {str(e)}")
            raise

    def delete_folders_with_prefix(self, prefix: str = "test-"):
        """
        Delete all folders and their contents that start with the given prefix.

        Args:
            prefix: Folder prefix to search for and delete
        """
        try:
            folders = self.list_folders_with_prefix(prefix)

            if not folders:
                return

            total_deleted = 0
            for folder in folders:
                try:
                    deleted_count = self.delete_folder_contents(folder)
                    total_deleted += deleted_count
                    print(
                        f"Successfully deleted folder '{folder}' ({deleted_count} items)"
                    )
                except Exception as e:
                    print(f"Failed to delete folder '{folder}': {str(e)}")

            print(f"Deletion completed. Total items deleted: {total_deleted}")

        except Exception as e:
            print(f"Error in delete operation: {str(e)}")
            raise


@pytest.fixture(scope="session", autouse=True)
def cleanup_azure_test_folders():
    """
    Clean up Azure container folders prefixed with 'test-' at the end of the test session.
    Disabled when running with pytest-xdist (parallel execution) to avoid interference.
    """
    yield  # Let all tests run first

    def _cleanup_azure_test_folders():
        try:
            # Skip cleanup if running with pytest-xdist (parallel execution)
            import os

            if os.environ.get("PYTEST_XDIST_WORKER"):
                print("Skipping session Azure cleanup when running with pytest-xdist")
                return

            from config import get_settings
            from azure.identity import DefaultAzureCredential

            settings = get_settings(env_file=".env.test")

            if not settings.storage.is_azure_storage():
                print("Azure storage not configured, skipping Azure cleanup")
                return

            azure_account_name = settings.storage.account_name
            azure_container_name = settings.storage.container_name

            if not azure_account_name or not azure_container_name:
                print(
                    "Missing Azure account name or container name, skipping Azure cleanup"
                )
                return

            print(
                f"Starting cleanup of Azure container '{azure_container_name}' for folders prefixed with 'test-'"
            )

            credential = DefaultAzureCredential()
            cleaner = AzureStorageCleaner(
                azure_account_name, azure_container_name, credential
            )

            folders = cleaner.list_folders_with_prefix("test-")
            if folders:
                print(f"Found {len(folders)} folders with prefix 'test-'")
                cleaner.delete_folders_with_prefix("test-")
            else:
                print("No test folders found in Azure container")

        except Exception as e:
            print(f"Error during Azure cleanup: {e}")

    try:
        _cleanup_azure_test_folders()
    except Exception as e:
        print(f"Failed to run Azure cleanup: {e}")
