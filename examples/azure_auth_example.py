"""
Example usage of Azure authentication with GitHub Actions OIDC.

This demonstrates how to use both connection string and managed identity authentication
depending on the environment.
"""

import asyncio
import logging
from config import get_storage_backend
from storage.storage_manager import StorageManager
from storage.readerwriter.delta import DeltaStorageWriter
from storage.path_strategy import OHLCVPathStrategy
from storage.partition_strategy import YearMonthDayPartitionStrategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def example_usage():
    """Example of using storage with flexible Azure authentication."""
    
    try:
        # Get the configured storage backend (handles both auth methods automatically)
        backend = get_storage_backend()
        logger.info(f"Using storage backend: {type(backend).__name__}")
        
        # Create storage manager with the backend
        storage_manager = StorageManager(
            backend=backend,
            writer=DeltaStorageWriter(),
            path_strategy=OHLCVPathStrategy(),
            partition_strategy=YearMonthDayPartitionStrategy()
        )
        
        # Test backend connectivity
        logger.info("Testing storage backend connectivity...")
        
        # For Azure backends, this will trigger container creation/connection
        await backend._get_container_client() if hasattr(backend, '_get_container_client') else None
        
        logger.info("Storage backend is ready!")
        
        # Example of listing available data (will be empty for new storage)
        try:
            available_data = await storage_manager.get_available_data()
            logger.info(f"Available data entries: {len(available_data)}")
        except Exception as e:
            logger.warning(f"Could not list available data: {e}")
        
    except Exception as e:
        logger.error(f"Failed to initialize storage: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(example_usage())
