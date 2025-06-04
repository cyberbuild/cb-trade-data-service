#!/usr/bin/env python3
"""
Quick test script to check if Azure backend async cleanup is working properly.
"""
import asyncio
import os
import sys
from pathlib import Path

# Add src to path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from storage.backends.azure_blob_backend import AzureBlobBackend
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_azure_backend_cleanup():
    """Test that Azure backend cleanup works properly."""
    print("Testing Azure backend async cleanup...")
    
    # Create unique container name
    container_name = f"test-cleanup-{uuid.uuid4().hex[:8]}"
    
    # Get Azure settings from environment
    account_name = os.environ.get("STORAGE_AZURE_ACCOUNT_NAME", "unknown")
    use_identity = os.environ.get("STORAGE_AZURE_USE_MANAGED_IDENTITY", "true").lower() == "true"
    
    print(f"Using container: {container_name}")
    print(f"Using managed identity: {use_identity}")
    print(f"Account name: {account_name}")
    
    backend = None
    try:
        # Test the new async context manager pattern
        backend = AzureBlobBackend(
            container_name=container_name,
            use_managed_identity=use_identity,
            account_name=account_name,
        )
        
        print("Testing async context manager...")
        async with backend:
            print("Backend initialized successfully")
            # Test a basic operation
            await backend.save_bytes("test-file.txt", b"Hello, Azure!")
            data = await backend.load_bytes("test-file.txt")
            print(f"Data round-trip successful: {data}")
            
        print("Context manager exited successfully")
        
        # Test manual close (should be no-op after context manager)
        await backend.close()
        print("Manual close completed (should be no-op)")
        
        print("✅ Azure backend cleanup test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Azure backend cleanup test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Emergency cleanup if needed
        if backend and not getattr(backend, '_closed', False):
            try:
                await backend.close()
                print("Emergency cleanup completed")
            except Exception as cleanup_error:
                print(f"Warning: Emergency cleanup failed: {cleanup_error}")

async def main():
    """Main test runner."""
    print("Starting Azure backend async cleanup test...")
    
    # Check environment
    if not os.environ.get("STORAGE_AZURE_ACCOUNT_NAME"):
        print("Warning: STORAGE_AZURE_ACCOUNT_NAME not set")
    
    success = await test_azure_backend_cleanup()
    
    if success:
        print("\n🎉 All tests passed! Azure backend async cleanup is working.")
        return 0
    else:
        print("\n💥 Tests failed! There are still issues with Azure backend cleanup.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
