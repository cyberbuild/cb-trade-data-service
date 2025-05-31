import os
import pytest
from pathlib import Path
from pydantic import ValidationError
from storage.storage_settings import LocalStorageSettings, AzureStorageSettings, get_storage_backend_config
from pydantic_settings import BaseSettings

pytestmark = pytest.mark.unit

def test_local_storage_settings_with_absolute_path(tmp_path, monkeypatch):
    abs_dir = tmp_path / "data_dir"
    assert not abs_dir.exists()
    # Remove env var so constructor arg is used
    monkeypatch.delenv('STORAGE_LOCAL_ROOT_PATH', raising=False)
    settings = LocalStorageSettings(root_path=str(abs_dir))
    assert settings.root_path == str(abs_dir)
    assert Path(settings.root_path).exists()

@ pytest.mark.parametrize("root", ["rel_dir1", ".", "subdir/inner"])
def test_local_storage_settings_with_relative_path_creates_dir(root, tmp_path, monkeypatch):
    # Remove env var so constructor arg is used
    monkeypatch.delenv('STORAGE_LOCAL_ROOT_PATH', raising=False)
    settings = LocalStorageSettings(root_path=root)
    assert settings.root_path == root
    assert Path(settings.root_path).exists()


@pytest.mark.parametrize("kwargs", [
    {'connection_string': 'secret'},
    {'connection_string': 'secret', 'container_name': 'cont'}
])
def test_azure_settings_valid(kwargs, monkeypatch):
    monkeypatch.setenv('STORAGE_AZURE_CONNECTION_STRING', kwargs['connection_string'])
    if 'container_name' in kwargs:
        monkeypatch.setenv('STORAGE_AZURE_CONTAINER_NAME', kwargs['container_name'])
    azure = AzureStorageSettings()
    assert azure.type == 'azure'
    assert azure.connection_string.get_secret_value() == kwargs['connection_string']
    if 'container_name' in kwargs:
        assert azure.container_name == kwargs['container_name']

def test_azure_settings_missing_connection_string():
    import os
    os.environ.pop('STORAGE_AZURE_CONNECTION_STRING', None)
    with pytest.raises(ValidationError):
        AzureStorageSettings()

class Dummy:
    pass

def test_get_storage_backend_config_success():
    from typing import ClassVar
    class MainSettings(BaseSettings):
        storage: ClassVar[LocalStorageSettings] = LocalStorageSettings(root_path=str(Path.cwd()))
    main = MainSettings()
    config = get_storage_backend_config(main)
    assert isinstance(config, LocalStorageSettings)

def test_get_storage_backend_config_failure():
    class MainSettings(BaseSettings):
        pass
    main = MainSettings()
    with pytest.raises(TypeError):
        get_storage_backend_config(main)
