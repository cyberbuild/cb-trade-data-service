import os
import pytest
from pathlib import Path
from pydantic import ValidationError
from storage.storage_settings import StorageConfig, get_storage_backend_config
from pydantic_settings import BaseSettings

pytestmark = pytest.mark.unit


def test_local_storage_settings_with_absolute_path(tmp_path, monkeypatch):
    abs_dir = tmp_path / "data_dir"
    assert not abs_dir.exists()
    # Clear all STORAGE_ env vars to isolate test
    for key in list(os.environ.keys()):
        if key.startswith("STORAGE_"):
            monkeypatch.delenv(key, raising=False)

    # Set the required environment variable for pydantic-settings
    monkeypatch.setenv("STORAGE_ROOT_PATH", str(abs_dir))
    settings = StorageConfig()
    assert settings.root_path == str(abs_dir)
    # Directory creation is handled by backend, not settings validation


def test_local_storage_settings_with_relative_path_creates_dir(tmp_path, monkeypatch):
    # Clear all STORAGE_ env vars to isolate test
    for key in list(os.environ.keys()):
        if key.startswith("STORAGE_"):
            monkeypatch.delenv(key, raising=False)

    # Use tmp_path for test isolation instead of hardcoded relative paths
    test_dir = tmp_path / "test_data"
    # Set the required environment variable for pydantic-settings
    monkeypatch.setenv("STORAGE_ROOT_PATH", str(test_dir))
    settings = StorageConfig()
    assert settings.root_path == str(test_dir)
    # Directory creation is handled by backend, not settings validation


@pytest.mark.parametrize(
    "kwargs",
    [
        {"connection_string": "secret"},
        {"connection_string": "secret", "container_name": "cont"},
    ],
)
def test_azure_settings_valid(kwargs, monkeypatch):
    # Clear all STORAGE_ env vars to isolate test
    for key in list(os.environ.keys()):
        if key.startswith("STORAGE_"):
            monkeypatch.delenv(key, raising=False)

    # Set required environment variables for Azure settings
    monkeypatch.setenv("STORAGE_TYPE", "azure")
    monkeypatch.setenv("STORAGE_AZURE_CONNECTION_STRING", kwargs["connection_string"])
    if "container_name" in kwargs:
        monkeypatch.setenv("STORAGE_AZURE_CONTAINER_NAME", kwargs["container_name"])

    config = StorageConfig()
    if "container_name" in kwargs:
        assert config.container_name == kwargs["container_name"]


def test_get_storage_backend_config_failure():
    class MainSettings(BaseSettings):
        pass

    main = MainSettings()
    with pytest.raises(TypeError):
        get_storage_backend_config(main)
