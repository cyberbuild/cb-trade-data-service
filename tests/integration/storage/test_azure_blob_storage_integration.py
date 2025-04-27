import pytest
import os
import uuid
import time
import pytest
from datetime import datetime, timezone, timedelta
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions

from storage.implementations.azure_blob_storage import AzureBlobStorage
from config import StorageConfig

# --- Configuration ---
# Attempt to get connection string from environment variable
CONNECTION_STRING = os.environ.get("STORAGE_AZURE_CONNECTION_STRING")

# Skip all tests in this module if the connection string is not set
pytestmark = pytest.mark.skipif(
    not CONNECTION_STRING,
    reason="STORAGE_AZURE_CONNECTION_STRING environment variable not set"
)

# Test constants
TEST_EXCHANGE = "testexchange" # Lowercase for container names
TEST_COIN = "BTC"
# Shorten base name and use only first 8 chars of UUID to stay within 63 char limit
RUN_UUID = str(uuid.uuid4())[:8]
BASE_CONTAINER_NAME = f"pytst-run-{RUN_UUID}"

@pytest.fixture(scope="module")
def blob_service_client():
    """Fixture to create a BlobServiceClient instance for the test module and clean up containers afterwards."""
    if not CONNECTION_STRING:
        pytest.skip("Azure connection string not available")
    client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    yield client

    # Module teardown: Clean up all containers created during this test run
    print(f"\nModule teardown: Cleaning up containers starting with '{BASE_CONTAINER_NAME}'...")
    try:
        containers = client.list_containers(name_starts_with=BASE_CONTAINER_NAME)
        deleted_count = 0
        for container in containers:
            print(f"Deleting container: {container.name}")
            try:
                client.delete_container(container.name)
                deleted_count += 1
            except Exception as e:
                print(f"Warning: Failed to delete container {container.name}: {e}")
        print(f"Deleted {deleted_count} container(s).")
    except Exception as e:
        print(f"Warning: Error listing or deleting containers during module teardown: {e}")


@pytest.fixture(scope="function")
def azure_storage(blob_service_client: BlobServiceClient):
    """
    Fixture to set up AzureBlobStorage for each test function.
    Creates a unique container for the test. Cleanup is handled by the module-scoped fixture.
    """
    # Generate shorter, unique container name for the test
    test_uuid = str(uuid.uuid4())[:8]
    container_name = f"{BASE_CONTAINER_NAME}-{test_uuid}" # e.g., pytst-run-abcdef12-12345678
    print(f"Creating container: {container_name}")
    try:
        # Ensure container is created before proceeding
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
             container_client.create_container()
    except Exception as e:
        pytest.fail(f"Failed to create container {container_name}: {e}")

    # Create StorageConfig for AzureBlobStorage
    config = StorageConfig(
        type="azure",
        azure_connection_string=CONNECTION_STRING,
        azure_container_name=container_name
    )
    storage_instance = AzureBlobStorage(config)

    yield storage_instance

    # Teardown is now handled by the blob_service_client fixture
    # print(f"Test function teardown for container: {container_name} (actual deletion in module scope)")

# --- Test Cases ---

@pytest.mark.asyncio
async def test_save_and_get_latest_entry(azure_storage: AzureBlobStorage):
    """Test saving an entry and retrieving the latest one."""
    ts1 = datetime.now(timezone.utc)
    data1 = {"price": 50000, "volume": 10}
    await azure_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts1, data1)

    # Azure Blob Storage might have eventual consistency, add a small delay
    time.sleep(2)

    ts2 = datetime.now(timezone.utc)
    data2 = {"price": 50050, "volume": 12}
    await azure_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts2, data2)

    time.sleep(2) # Delay before reading

    latest_entry = await azure_storage.get_latest_entry(TEST_EXCHANGE, TEST_COIN)

    assert latest_entry is not None
    # Timestamps might have slight precision differences after serialization/deserialization
    assert abs(datetime.fromisoformat(latest_entry["timestamp"].replace('Z', '+00:00')) - ts2) < timedelta(seconds=1)
    assert latest_entry["data"] == data2

@pytest.mark.asyncio
async def test_get_range(azure_storage: AzureBlobStorage):
    """Test retrieving entries within a specific time range."""
    base_time = datetime.now(timezone.utc)
    timestamps = [base_time + timedelta(seconds=i * 2) for i in range(5)] # Increase spacing
    entries = [{"price": 50000 + i * 10, "volume": 10 + i} for i in range(5)]

    for ts, data in zip(timestamps, entries):
        await azure_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts, data)
        time.sleep(1) # Delay between writes

    time.sleep(2) # Delay before reading range

    start_time = timestamps[1]
    end_time = timestamps[3]

    retrieved_entries = await azure_storage.get_range(TEST_EXCHANGE, TEST_COIN, start_time, end_time)

    assert len(retrieved_entries) == 3 # Entries at index 1, 2, 3
    retrieved_prices = [e["data"]["price"] for e in retrieved_entries]
    expected_prices = [entries[i]["price"] for i in range(1, 4)]
    # Order might not be guaranteed depending on listing mechanism, sort results
    assert sorted(retrieved_prices) == sorted(expected_prices)

@pytest.mark.asyncio
async def test_list_coins(azure_storage: AzureBlobStorage):
    """Test listing available coins for an exchange."""
    coins = ["BTC", "ETH", "LTC"]
    ts = datetime.now(timezone.utc)
    data = {"price": 100}
    for coin in coins:
        await azure_storage.save_entry(TEST_EXCHANGE, coin, ts, data)
        time.sleep(1)
    time.sleep(2)
    listed_coins = await azure_storage.list_coins(TEST_EXCHANGE)
    assert sorted(listed_coins) == sorted(coins)

@pytest.mark.asyncio
async def test_check_coin_exists(azure_storage: AzureBlobStorage):
    """Test checking if a coin 'folder' (prefix) exists."""
    ts = datetime.now(timezone.utc)
    data = {"price": 100}
    assert not await azure_storage.check_coin_exists(TEST_EXCHANGE, TEST_COIN)
    await azure_storage.save_entry(TEST_EXCHANGE, TEST_COIN, ts, data)
    time.sleep(2)
    assert await azure_storage.check_coin_exists(TEST_EXCHANGE, TEST_COIN)

@pytest.mark.asyncio
async def test_get_latest_entry_no_data(azure_storage: AzureBlobStorage):
    """Test getting the latest entry when no data exists."""
    latest_entry = await azure_storage.get_latest_entry(TEST_EXCHANGE, "NONEXISTENT")
    assert latest_entry is None

@pytest.mark.asyncio
async def test_get_range_no_data(azure_storage: AzureBlobStorage):
    """Test getting a range when no data exists."""
    start_time = datetime.now(timezone.utc) - timedelta(days=1)
    end_time = datetime.now(timezone.utc)
    retrieved_entries = await azure_storage.get_range(TEST_EXCHANGE, "NONEXISTENT", start_time, end_time)
    assert len(retrieved_entries) == 0

@pytest.mark.asyncio
async def test_list_coins_no_exchange(azure_storage: AzureBlobStorage):
    """Test listing coins when the exchange prefix doesn't exist."""
    listed_coins = await azure_storage.list_coins("nonexistentexchange")
    assert len(listed_coins) == 0

@pytest.mark.asyncio
async def test_check_coin_exists_no_exchange(azure_storage: AzureBlobStorage):
    """Test checking coin existence when the exchange prefix doesn't exist."""
    assert not await azure_storage.check_coin_exists("nonexistentexchange", TEST_COIN)

