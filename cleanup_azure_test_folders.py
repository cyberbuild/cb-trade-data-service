import os
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, ClientSecretCredential
from azure.core.exceptions import ResourceNotFoundError
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import List, Optional, Set
from datetime import datetime

from config import Settings, get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AzureStorageCleaner:
    def __init__(self, account_name: str, container_name: str, credential=None):
        """
        Initialize the Azure Storage Cleaner using Azure Identity.
        
        Args:
            account_name: Azure Storage account name
            container_name: Name of the container to clean
            credential: Azure credential (uses DefaultAzureCredential if None)
        """
        if credential is None:
            credential = DefaultAzureCredential()
            
        account_url = f"https://{account_name}.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=credential
        )
        self.container_name = container_name
        self.container_client = self.blob_service_client.get_container_client(container_name)
        
    def list_folders_with_prefix(self, prefix: str = "test-") -> List[str]:
        """
        List all folders that start with the given prefix.
        
        Args:
            prefix: Folder prefix to search for
            
        Returns:
            List of folder names with the prefix
        """
        folders = set()
        
        try:
            # List all blobs and extract folder names
            blobs = self.container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blobs:
                # Extract folder name from blob path
                parts = blob.name.split('/')
                if len(parts) > 1:
                    # Get the top-level folder that starts with prefix
                    if parts[0].startswith(prefix):
                        folders.add(parts[0])
                        
            logger.info(f"Found {len(folders)} folders with prefix '{prefix}'")
            return list(folders)
            
        except Exception as e:
            logger.error(f"Error listing folders: {str(e)}")
            raise
            
    def get_all_paths_in_folder(self, folder_name: str) -> tuple[List[str], Set[str]]:
        """
        Get all blob paths and directory paths in a folder for hierarchical namespace.
        
        Args:
            folder_name: Name of the folder
            
        Returns:
            Tuple of (blob_paths, directory_paths)
        """
        prefix = f"{folder_name}/" if not folder_name.endswith('/') else folder_name
        blob_paths = []
        directory_paths = set()
        
        try:
            # List all blobs in the folder
            blobs = self.container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blobs:
                blob_paths.append(blob.name)
                
                # Extract all directory paths from the blob path
                path_parts = blob.name.split('/')
                for i in range(1, len(path_parts)):
                    dir_path = '/'.join(path_parts[:i])
                    if dir_path.startswith(prefix):
                        directory_paths.add(dir_path)
                
            # Add the root folder itself
            directory_paths.add(folder_name.rstrip('/'))
            
            return blob_paths, directory_paths
            
        except Exception as e:
            logger.error(f"Error getting paths in folder: {str(e)}")
            raise
            
    def delete_blobs_and_directories(self, blob_paths: List[str], directory_paths: Set[str]) -> int:
        """
        Delete blobs first, then delete directories in reverse order (deepest first).
        
        Args:
            blob_paths: List of blob paths to delete
            directory_paths: Set of directory paths to delete
            
        Returns:
            Number of successfully deleted items
        """
        deleted_count = 0
        failed_count = 0
        
        # First, delete all blobs
        logger.info(f"Deleting {len(blob_paths)} blobs...")
        for blob_path in blob_paths:
            try:
                blob_client = self.container_client.get_blob_client(blob_path)
                blob_client.delete_blob()
                deleted_count += 1
            except ResourceNotFoundError:
                # Blob might have been already deleted
                pass
            except Exception as e:
                logger.warning(f"Failed to delete blob {blob_path}: {str(e)}")
                failed_count += 1
        
        # Sort directories by depth (deepest first) to delete in correct order
        sorted_dirs = sorted(directory_paths, key=lambda x: x.count('/'), reverse=True)
        
        logger.info(f"Deleting {len(sorted_dirs)} directories...")
        for dir_path in sorted_dirs:
            try:
                # For hierarchical namespace, we need to delete as a directory
                blob_client = self.container_client.get_blob_client(dir_path)
                blob_client.delete_blob()
                deleted_count += 1
                logger.debug(f"Deleted directory: {dir_path}")
            except ResourceNotFoundError:
                # Directory might have been already deleted
                pass
            except Exception as e:
                # Log but continue - some paths might be virtual directories
                logger.debug(f"Could not delete directory {dir_path}: {str(e)}")
        
        logger.info(f"Deleted {deleted_count} items, {failed_count} failures")
        return deleted_count
        
    def delete_folder_contents(self, folder_name: str) -> int:
        """
        Delete all contents of a specific folder, handling hierarchical namespace.
        
        Args:
            folder_name: Name of the folder to delete
            
        Returns:
            Number of items deleted
        """
        try:
            # Get all paths in the folder
            blob_paths, directory_paths = self.get_all_paths_in_folder(folder_name)
            
            if not blob_paths and len(directory_paths) <= 1:
                logger.info(f"No contents found in folder '{folder_name}'")
                return 0
                
            logger.info(f"Found {len(blob_paths)} blobs and {len(directory_paths)} directories in folder '{folder_name}'")
            
            # Delete blobs first, then directories
            return self.delete_blobs_and_directories(blob_paths, directory_paths)
            
        except Exception as e:
            logger.error(f"Error deleting folder contents: {str(e)}")
            raise
            
    def delete_folders_with_prefix(self, prefix: str = "test-", max_workers: int = 5):
        """
        Delete all folders and their contents that start with the given prefix.
        Uses parallel processing for efficiency.
        
        Args:
            prefix: Folder prefix to search for and delete
            max_workers: Maximum number of parallel workers (reduced for hierarchical operations)
        """
        start_time = datetime.now()
        logger.info(f"Starting deletion of folders with prefix '{prefix}'")
        
        try:
            # Get list of folders to delete
            folders = self.list_folders_with_prefix(prefix)
            
            if not folders:
                logger.info("No folders found with the specified prefix")
                return
                
            logger.info(f"Preparing to delete {len(folders)} folders: {', '.join(folders[:5])}{'...' if len(folders) > 5 else ''}")
            
            # Delete folders in parallel (reduced workers for hierarchical operations)
            total_deleted = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all folder deletions
                future_to_folder = {
                    executor.submit(self.delete_folder_contents, folder): folder 
                    for folder in folders
                }
                
                # Process completed deletions
                for future in as_completed(future_to_folder):
                    folder = future_to_folder[future]
                    try:
                        deleted_count = future.result()
                        total_deleted += deleted_count
                        logger.info(f"Successfully deleted folder '{folder}' ({deleted_count} items)")
                    except Exception as e:
                        logger.error(f"Failed to delete folder '{folder}': {str(e)}")
                        
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Deletion completed. Total items deleted: {total_deleted} in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in delete operation: {str(e)}")
            raise

def get_credential(settings: Settings, asUser: bool = False):
    """
    Get the appropriate Azure credential based on environment configuration.
    
    Args:
        settings: Settings object containing storage configuration
        asUser: If True, forces user authentication (disables managed identity)
    
    Returns:
        Azure credential object
    """
    azure_use_managed_identity = settings.storage.use_managed_identity
    
    # Override managed identity if asUser is specified
    if asUser:
        azure_use_managed_identity = False
    
    azure_client_id = settings.storage.client_id
    azure_tenant_id = settings.storage.tenant_id
    
    if azure_use_managed_identity:
        logger.info("Using Managed Identity for authentication...")
        if azure_client_id:
            logger.info(f"Using user-assigned managed identity with client ID: {azure_client_id}")
            return ManagedIdentityCredential(client_id=azure_client_id)
        else:
            logger.info("Using system-assigned managed identity")
            return ManagedIdentityCredential()
    else:
        logger.info("Using DefaultAzureCredential for authentication...")
        return DefaultAzureCredential()

def main():
    """
    Main function to execute the deletion script.
    """
    # Get configuration from environment variables
    settings = get_settings(env_file=".env.test")
    
    # Debug: Print settings values
    logger.info(f"Storage config type: {type(settings.storage)}")
    logger.info(f"Container name from settings: {settings.storage.container_name}")
    logger.info(f"Account name from settings: {settings.storage.account_name}")
    
    azure_container_name = settings.storage.container_name
    azure_account_name = settings.storage.account_name
    azure_subscription_id = settings.storage.subscription_id
    
    # Validate configuration
    if not azure_account_name or not azure_container_name:
        logger.error("Missing required environment variables")
        logger.info("Please set the following environment variables:")
        logger.info("  - STORAGE_AZURE_ACCOUNT_NAME: Your storage account name")
        logger.info("  - STORAGE_AZURE_CONTAINER_NAME: Your container name")
        logger.info("\nOptional environment variables:")
        logger.info("  - STORAGE_AZURE_USE_MANAGED_IDENTITY: Set to 'true' to use managed identity")
        logger.info("  - AZURE_CLIENT_ID: Client ID for service principal or user-assigned managed identity")
        logger.info("  - AZURE_CLIENT_SECRET: Client secret for service principal authentication")
        logger.info("  - AZURE_TENANT_ID: Tenant ID for service principal authentication")
        logger.info("  - AZURE_SUBSCRIPTION_ID: Subscription ID (informational)")
        return
          # Get appropriate credential
    try:
        credential = get_credential(settings, True)
        
        # Create cleaner instance
        cleaner = AzureStorageCleaner(azure_account_name, azure_container_name, credential)
        logger.info("Authentication successful")
        
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        logger.info("\nTroubleshooting:")
        logger.info("- For managed identity: Ensure the identity is enabled and has proper permissions")
        logger.info("- For service principal: Check AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, and AZURE_TENANT_ID")
        logger.info("- For local development: Run 'az login' or set up service principal credentials")
        logger.info("- Ensure the identity has 'Storage Blob Data Contributor' role on the storage account")
        return
    
    # Display configuration
    print(f"\nAzure Storage Folder Deletion Tool (Hierarchical Namespace)")
    print(f"{'='*60}")
    print(f"Storage Account: {azure_account_name}")
    print(f"Container: {azure_container_name}")
    if azure_subscription_id:
        print(f"Subscription ID: {azure_subscription_id}")
    print(f"Current User: {os.getenv('USER', os.getenv('USERNAME', 'Unknown'))}")
    print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*60}")
    
    print(f"\nWARNING: This will delete all folders starting with 'test-'")
    print("This action cannot be undone!")
    print("\nNote: This storage account has hierarchical namespace enabled.")
    print("Directories will be deleted after all their contents are removed.")
    
    # List folders that will be deleted
    try:
        folders = cleaner.list_folders_with_prefix("test-")
        if folders:
            print(f"\nFolders to be deleted ({len(folders)}):")
            for folder in folders[:10]:  # Show first 10
                print(f"  - {folder}")
            if len(folders) > 10:
                print(f"  ... and {len(folders) - 10} more")
                
            confirmation = input("\nType 'DELETE' to confirm deletion: ")
            if confirmation == 'DELETE':
                # Execute deletion with reduced workers for hierarchical operations
                cleaner.delete_folders_with_prefix("test-", max_workers=5)
            else:
                print("Deletion cancelled")
        else:
            print("\nNo folders found with prefix 'test-'")
            
    except Exception as e:
        logger.error(f"Operation failed: {str(e)}")

if __name__ == "__main__":
    main()