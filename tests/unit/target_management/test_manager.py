import pytest
from unittest.mock import patch, MagicMock
from target_management.manager import TargetManager
from datetime import datetime

@pytest.fixture
def config_local():
    return {
        "TARGET_STORAGE_TYPE": "local",
        "TARGET_TABLE_PATH": "./tests/test_targets_delta"
    }

def test_init_table_creates_table(tmp_path, config_local):
    config_local["TARGET_TABLE_PATH"] = str(tmp_path / "delta_table")
    manager = TargetManager(config_local)
    assert manager.table is not None

def test_add_and_get_target(tmp_path, config_local):
    config_local["TARGET_TABLE_PATH"] = str(tmp_path / "delta_table")
    manager = TargetManager(config_local)
    target = {
        "target_id": "t1",
        "coin": "BTC",
        "exchange": "binance",
        "exchange_id": "binance",
        "interval": "5m",
        "enabled": True
    }
    manager.add_target(target)
    result = manager.get_target("t1")
    assert result is not None
    assert result["coin"] == "BTC"

def test_update_target(tmp_path, config_local):
    config_local["TARGET_TABLE_PATH"] = str(tmp_path / "delta_table")
    manager = TargetManager(config_local)
    target = {
        "target_id": "t2",
        "coin": "ETH",
        "exchange": "binance",
        "exchange_id": "binance",
        "interval": "5m",
        "enabled": True
    }
    manager.add_target(target)
    manager.update_target("t2", {"enabled": False})
    result = manager.get_target("t2")
    assert result["enabled"] is False

def test_delete_target(tmp_path, config_local):
    config_local["TARGET_TABLE_PATH"] = str(tmp_path / "delta_table")
    manager = TargetManager(config_local)
    target = {
        "target_id": "t3",
        "coin": "XRP",
        "exchange": "binance",
        "exchange_id": "binance",
        "interval": "5m",
        "enabled": True
    }
    manager.add_target(target)
    manager.delete_target("t3")
    result = manager.get_target("t3")
    assert result is None

def test_list_targets(tmp_path, config_local):
    config_local["TARGET_TABLE_PATH"] = str(tmp_path / "delta_table")
    manager = TargetManager(config_local)
    targets = [
        {"target_id": "t4", "coin": "BTC", "exchange": "binance", "exchange_id": "binance", "interval": "5m", "enabled": True},
        {"target_id": "t5", "coin": "ETH", "exchange": "binance", "exchange_id": "binance", "interval": "5m", "enabled": False}
    ]
    for t in targets:
        manager.add_target(t)
    enabled_targets = manager.list_targets(enabled=True)
    assert len(enabled_targets) == 1
    assert enabled_targets[0]["target_id"] == "t4"
    all_targets = manager.list_targets()
    assert len(all_targets) == 2
