import os
from dotenv import load_dotenv
# Load .env.test before anything else

import pytest
import tempfile
from target_management.manager import TargetManager

try:
    from azure.storage.blob import ContainerClient
except ImportError:
    pytest.skip("azure-storage-blob SDK not installed, skipping integration test", allow_module_level=True)

# Skip this module if required Azure env vars are not set
pytestmark = pytest.mark.skipif(
    not os.getenv("AZURE_STORAGE_CONNECTION_STRING") or not os.getenv("AZURE_SYSTEM_CONTAINER"),
    reason="AZURE_STORAGE_CONNECTION_STRING or AZURE_SYSTEM_CONTAINER not set"
)

@pytest.fixture(scope="module")
def azure_config():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_SYSTEM_CONTAINER")
    
    # Create temp directory for local deltalake files
    with tempfile.TemporaryDirectory(prefix="azure_target_manager_") as tmpdir:
        yield {
            "TARGET_STORAGE_TYPE": "local",  # Use local storage type which is more reliable
            "TARGET_TABLE_PATH": tmpdir,
            "AZURE_STORAGE_OPTIONS": {
                "connection_string": connection_string,
                "container": container,
                "account_name": connection_string.split("AccountName=")[1].split(";")[0]
            }
        }

@pytest.fixture(scope="module")
def cleanup_blob(azure_config):
    # Run tests
    yield
    # No special cleanup needed for local files - tempdir will be auto-deleted

def test_manager_crud_integration(azure_config, cleanup_blob):
    manager = TargetManager(azure_config)

    # Ensure no pre-existing target
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
