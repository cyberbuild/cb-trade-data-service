import sys
import os
# Add the 'data-service' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../..')))

import pytest
from unittest.mock import MagicMock, patch
# Import relative to the 'data-service' directory
from src.storage.implementations.azure_blob_storage import AzureBlobStorage
from datetime import datetime, timezone

@pytest.fixture
def azure_storage():
    # Patch based on the import path within the module under test (azure_blob_storage.py)
    # Assuming azure_blob_storage.py imports BlobServiceClient like:
    # from azure.storage.blob import BlobServiceClient
    # If it's different, this path needs adjustment.
    with patch('src.storage.implementations.azure_blob_storage.BlobServiceClient') as mock_service_client:
        mock_container = MagicMock()
        mock_service_client.from_connection_string.return_value.get_container_client.return_value = mock_container
        storage = AzureBlobStorage('fake-conn-str', 'test-container')
        yield storage, mock_container

# Patch based on the import path within the module under test
@patch('src.storage.implementations.azure_blob_storage._parse_timestamp', lambda x: x)
def test_save_entry(azure_storage):
    storage, mock_container = azure_storage
    mock_blob = MagicMock()
    mock_container.get_blob_client.return_value = mock_blob
    storage.save_entry('binance', 'BTC', '20240101T000000Z', {'foo': 'bar'})
    mock_blob.upload_blob.assert_called_once()

def test_get_range(azure_storage):
    storage, mock_container = azure_storage
    mock_blob = MagicMock()
    mock_blob.name = 'raw_data/binance/BTC/2024/01/01/20240101T000000Z.json'
    mock_blob_client = MagicMock()
    mock_downloader = MagicMock()
    mock_downloader.readall.return_value = b'{"foo": "bar", "timestamp": 1}'
    mock_blob_client.download_blob.return_value = mock_downloader
    mock_container.list_blobs.return_value = [mock_blob]
    mock_container.get_blob_client.return_value = mock_blob_client

    # Patch _parse_timestamp to always return a timezone-aware datetime
    with patch('src.storage.implementations.azure_blob_storage._parse_timestamp', return_value=datetime(2024, 1, 1, tzinfo=timezone.utc)):
        result = storage.get_range('binance', 'BTC', 0, 2)
        assert isinstance(result, list)
        assert result[0]['data']['foo'] == 'bar'

def test_get_latest_entry(azure_storage):
    storage, mock_container = azure_storage
    mock_blob = MagicMock()
    mock_blob.name = 'raw_data/binance/BTC/2024/01/01/20240101T000000Z.json'
    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b'{"foo": "bar", "timestamp": 1}'
    mock_container.list_blobs.return_value = [mock_blob]
    mock_container.get_blob_client.return_value = mock_blob_client
    result = storage.get_latest_entry('binance', 'BTC')
    assert result['data']['foo'] == 'bar'

def test_list_coins(azure_storage):
        storage, mock_container = azure_storage
        # Simulate a 'directory' blob for coin detection using walk_blobs
        mock_blob_prefix = MagicMock()
        mock_blob_prefix.name = 'raw_data/binance/BTC/'
        # walk_blobs yields prefixes when delimiter is used
        mock_container.walk_blobs.return_value = [mock_blob_prefix]
        coins = storage.list_coins('binance')
        assert 'BTC' in coins
        # Verify walk_blobs was called correctly
        mock_container.walk_blobs.assert_called_once_with(name_starts_with='raw_data/binance/', delimiter='/')

def test_check_coin_exists(azure_storage):
    storage, mock_container = azure_storage
    mock_blob = MagicMock()
    mock_blob.name = 'raw_data/binance/BTC/2024/01/01/20240101T000000Z.json'
    mock_container.list_blobs.return_value = [mock_blob]
    assert storage.check_coin_exists('binance', 'BTC')
    mock_container.list_blobs.return_value = []
    assert not storage.check_coin_exists('binance', 'ETH')

def test_azure_api_error_handling(azure_storage):
    storage, mock_container = azure_storage
    mock_container.get_blob_client.side_effect = Exception('API error')
    try:
        storage.save_entry('binance', 'BTC', '20240101T000000Z', {'foo': 'bar'})
    except Exception as e:
        assert 'API error' in str(e)
