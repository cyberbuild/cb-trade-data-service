"""
Integration test for TargetManager with Azure Blob Storage backend.
This test requires an Azure Storage account with hierarchical namespace enabled.
"""
import os
import sys
import pytest

# Import needed packages first
import pandas as pd
from datetime import datetime

# Import from project
from target_management.manager import TargetManager

# Skip if azure package is missing
try:
    from azure.storage.blob import ContainerClient
except ImportError:
    pytest.skip("azure-storage-blob SDK not installed, skipping integration test", allow_module_level=True)

# Debug print to help diagnose environment variable issues
print(f"AZURE_STORAGE_CONNECTION_STRING exists: {bool(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))}")
print(f"AZURE_SYSTEM_CONTAINER value: {os.getenv('AZURE_SYSTEM_CONTAINER')}")

# Skip this module if required Azure env vars are not set
pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_STORAGE_CONNECTION_STRING") or not os.getenv("AZURE_SYSTEM_CONTAINER"),
    reason="AZURE_STORAGE_CONNECTION_STRING or AZURE_SYSTEM_CONTAINER not set"
)

@pytest.fixture(scope="module")
def azure_config():
    """Fixture that provides Azure configuration for testing."""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_SYSTEM_CONTAINER")

    # Extract account name from connection string
    account_name = None
    for part in connection_string.split(";"):
        if part.lower().startswith("accountname="):
            account_name = part.split("=", 1)[1]
            break

    if not account_name:
        pytest.skip("Could not extract account name from connection string")

    return {
        "TARGET_STORAGE_TYPE": "azure",
        "TARGET_TABLE_PATH": f"{container}/integration_test_targets_delta",
        "AZURE_STORAGE_OPTIONS": {
            "account_name": account_name,
            "account_key": connection_string.split("AccountKey=")[1].split(";")[0],
            "container": container
        }
    }

@pytest.fixture(scope="module")
def cleanup_blob(azure_config):
    """Fixture that cleans up Azure blobs after tests."""
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_SYSTEM_CONTAINER")
    prefix = "integration_test_targets_delta/"
    print(f"DEBUG: Using container name: '{container}'")
    print(f"DEBUG: Using connection string: '{conn_str}'")
    try:
        client = ContainerClient.from_connection_string(conn_str, container)
        print("Listing all blobs in the container for debugging:")
        all_blobs = list(client.list_blobs())
        for blob in all_blobs:
            print(f"BLOB: {blob.name}")
    except Exception as e:
        if hasattr(e, 'error_code') and e.error_code == 'ContainerNotFound':
            print("Container does not exist, skipping pre-test cleanup.")
        else:
            print(f"Error during pre-test cleanup: {e}")
        client = None
    yield
    try:
        if client:
            blob_list = list(client.list_blobs(name_starts_with=prefix))
            if blob_list:
                print(f"Post-test cleanup: Deleting {len(blob_list)} blobs with prefix '{prefix}'")
                for blob in blob_list:
                    try:
                        client.delete_blob(blob.name)
                        print(f"Deleted blob: {blob.name}")
                    except Exception as e:
                        print(f"Failed to delete {blob.name}: {e}")
    except Exception as e:
        if hasattr(e, 'error_code') and e.error_code == 'ContainerNotFound':
            print("Container does not exist, skipping post-test cleanup.")
        else:
            print(f"Error during post-test cleanup: {e}")


def test_manager_crud_integration(azure_config, cleanup_blob, azure_backend):
    """Test CRUD operations on TargetManager with Azure backend."""
    print(f"Using storage backend: {os.environ.get('STORAGE_BACKEND')}")
    manager = TargetManager(azure_config)

    # Ensure no pre-existing target
    existing = manager.get_target("int1")
    if existing is not None:
        print("Pre-existing target 'int1' found, deleting before test.")
        manager.delete_target("int1")
    assert manager.get_target("int1") is None

    # Add target
    target = {
        "target_id": "int1",
        "coin": "BTC",
        "exchange": "binance",
        "exchange_id": "binance",
        "interval": "5m",
        "enabled": True
    }
    manager.add_target(target)
    result = manager.get_target("int1")
    assert result is not None and result["coin"] == "BTC"

    # Update target
    manager.update_target("int1", {"enabled": False})
    result = manager.get_target("int1")
    assert result["enabled"] is False

    # List targets
    all_targets = manager.list_targets()
    assert any(t["target_id"] == "int1" for t in all_targets)
    enabled_targets = manager.list_targets(enabled=True)
    assert len(enabled_targets) == 0  # Our target is now disabled

    # Delete target
    manager.delete_target("int1")
    assert manager.get_target("int1") is None
