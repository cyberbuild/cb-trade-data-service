#!/usr/bin/env python3
import sys
import os
import asyncio

# Add src to path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from dotenv import load_dotenv
load_dotenv('.env.test')

from storage.backends.azure_blob_backend import AzureBlobBackend

async def test_cleanup_azure_test_folders():
    """Test the Azure cleanup function"""
    try:
        azure_account_name = os.environ.get('STORAGE_AZURE_ACCOUNT_NAME')
        azure_container_name = os.environ.get('STORAGE_AZURE_CONTAINER_NAME')
        azure_connection_string = os.environ.get('STORAGE_AZURE_CONNECTION_STRING')
        azure_use_managed_identity = os.environ.get('STORAGE_AZURE_USE_MANAGED_IDENTITY', '').lower() == 'true'
        
        if not azure_container_name:
            print("No Azure container name found in environment, skipping Azure cleanup")
            return
        
        if not azure_connection_string and not (azure_use_managed_identity and azure_account_name):
            print("No Azure authentication method found (connection string or managed identity), skipping Azure cleanup")
            return
        
        print(f"Starting cleanup of Azure container '{azure_container_name}' for folders prefixed with 'test-'")
        
        backend = AzureBlobBackend(
            account_name=azure_account_name,
            container_name=azure_container_name,
            use_managed_identity=azure_use_managed_identity
        )
        
        container_client = await backend._get_container_client()
        deleted_count = 0
        
        all_test_items = []
        async for blob in container_client.list_blobs(name_starts_with="test-"):
            all_test_items.append(blob.name)
        
        if not all_test_items:
            print("No test items found in Azure container")
            await backend.close()
            return
        
        all_test_items.sort(reverse=True, key=len)
        
        print(f"Found {len(all_test_items)} test items to clean up")
        print("Sample items to delete:", all_test_items[:3] if len(all_test_items) >= 3 else all_test_items)
        
        batch_size = 50
        for i in range(0, len(all_test_items), batch_size):
            batch = all_test_items[i:i + batch_size]
            batch_tasks = []
            
            for blob_name in batch:
                blob_client = container_client.get_blob_client(blob_name)
                batch_tasks.append(blob_client.delete_blob(delete_snapshots="include"))
            
            try:
                await asyncio.gather(*batch_tasks, return_exceptions=True)
                deleted_count += len(batch)
                print(f"Batch deleted {len(batch)} items. Total: {deleted_count}/{len(all_test_items)}")
            except Exception as e:
                print(f"Error in batch delete: {e}")
                for blob_name in batch:
                    try:
                        blob_client = container_client.get_blob_client(blob_name)
                        await blob_client.delete_blob(delete_snapshots="include")
                        deleted_count += 1
                    except Exception as single_error:
                        if "BlobNotFound" not in str(single_error) and "OperationNotSupportedOnDirectory" not in str(single_error):
                            print(f"Error deleting {blob_name}: {single_error}")
        
        print(f"Azure cleanup completed. Successfully deleted {deleted_count} items")
        await backend.close()
        
    except Exception as e:
        print(f"Error during Azure cleanup: {e}")

if __name__ == "__main__":
    asyncio.run(test_cleanup_azure_test_folders())
