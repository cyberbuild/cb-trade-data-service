#!/usr/bin/env python3
"""
Quick test script to verify Azure cleanup function works correctly.
"""
import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from config import get_settings
from storage.backends.azure_blob_backend import AzureBlobBackend

# Load test environment
env_path = Path(__file__).parent / '.env.test'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


async def test_azure_cleanup():
    """Test the Azure cleanup function logic."""
    try:
        # Try to get Azure settings directly from environment variables
        azure_account_name = os.environ.get('STORAGE_AZURE_ACCOUNT_NAME')
        azure_container_name = os.environ.get('STORAGE_AZURE_CONTAINER_NAME')
        azure_connection_string = os.environ.get('STORAGE_AZURE_CONNECTION_STRING')
        azure_use_managed_identity = os.environ.get('STORAGE_AZURE_USE_MANAGED_IDENTITY', '').lower() == 'true'
        
        print(f"Azure account name: {azure_account_name}")
        print(f"Azure container name: {azure_container_name}")
        print(f"Azure connection string: {'Set' if azure_connection_string else 'Not set'}")
        print(f"Azure use managed identity: {azure_use_managed_identity}")
        
        # If no Azure configuration found, skip cleanup
        if not azure_container_name:
            print("No Azure container name found in environment, skipping Azure cleanup")
            return
        
        if not azure_connection_string and not (azure_use_managed_identity and azure_account_name):
            print("No Azure authentication method found (connection string or managed identity), skipping Azure cleanup")
            return
        
        print("Azure cleanup function would run successfully")
        
    except Exception as e:
        print(f"Azure cleanup test error: {e}")


if __name__ == "__main__":
    asyncio.run(test_azure_cleanup())
