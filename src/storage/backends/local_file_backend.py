# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\storage\backends\local_file_backend.py
import os
import shutil
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from ..interfaces import IStorageBackend
import aiofiles
import aiofiles.os

logger = logging.getLogger(__name__)

class LocalFileBackend(IStorageBackend):
    """Implements IStorageBackend for the local filesystem."""

    def __init__(self, root_path: str):
        self.root_path = Path(root_path).resolve()
        # Ensure root directory exists
        os.makedirs(self.root_path, exist_ok=True)
        logger.info(f"Initialized LocalFileBackend with root: {self.root_path}")

    def _get_full_path(self, identifier: str) -> Path:
        """Resolves an identifier to an absolute path, ensuring it's within the root."""
        full_path = (self.root_path / identifier).resolve()
        # Security check: Ensure the path is still within the root directory
        if self.root_path not in full_path.parents and full_path != self.root_path:
             # This check might be too strict if identifier itself contains '..'
             # A better check might be os.path.commonpath
             common = os.path.commonpath([str(self.root_path), str(full_path)])
             if common != str(self.root_path):
                raise ValueError(f"Path traversal attempt detected: {identifier}")
        return full_path

    def get_uri_for_identifier(self, identifier: str) -> str:
        """Returns a file:// URI for the identifier."""
        return self._get_full_path(identifier).as_uri()

    async def get_storage_options(self) -> Dict[str, Any]: # Changed to async and return Dict
        """Local files generally don't require special storage options for libraries."""
        return {} # Return empty dict

    async def save_bytes(self, identifier: str, data: bytes):
        """Saves bytes to a local file asynchronously."""
        full_path = self._get_full_path(identifier)
        try:
            # Ensure parent directory exists
            await aiofiles.os.makedirs(full_path.parent, exist_ok=True)
            async with aiofiles.open(full_path, mode='wb') as f:
                await f.write(data)
            logger.debug(f"Saved {len(data)} bytes to {full_path}")
        except Exception as e:
            logger.error(f"Error saving bytes to {full_path}: {e}")
            raise

    async def load_bytes(self, identifier: str) -> bytes:
        """Loads bytes from a local file asynchronously."""
        full_path = self._get_full_path(identifier)
        try:
            async with aiofiles.open(full_path, mode='rb') as f:
                data = await f.read()
            logger.debug(f"Loaded {len(data)} bytes from {full_path}")
            return data
        except FileNotFoundError:
            logger.warning(f"File not found: {full_path}")
            raise # Re-raise FileNotFoundError
        except Exception as e:
            logger.error(f"Error loading bytes from {full_path}: {e}")
            raise

    async def list_items(self, prefix: str = "") -> List[str]:
        """Lists files and directories under a given prefix relative to the root path."""
        search_path = self._get_full_path(prefix)
        items = []
        try:
            if await aiofiles.os.path.isdir(search_path):
                # Use scandir for potentially better performance
                # Need to await scandir to get the async iterator
                # async for entry in await aiofiles.os.scandir(search_path):
                #     # Return path relative to root_path
                #     relative_path = Path(entry.path).relative_to(self.root_path).as_posix()
                #     items.append(relative_path)
                # Use listdir which returns a list directly
                entries = await aiofiles.os.listdir(search_path)
                for entry_name in entries:
                    entry_path = search_path / entry_name
                    # Return path relative to root_path
                    relative_path = entry_path.relative_to(self.root_path).as_posix()
                    items.append(relative_path)
            # If prefix points to a file, list_items should arguably return that item
            elif await aiofiles.os.path.exists(search_path):
                 items.append(Path(search_path).relative_to(self.root_path).as_posix())

            logger.debug(f"Listed {len(items)} items under prefix '{prefix}' in {search_path}")
        except FileNotFoundError:
             logger.warning(f"Prefix directory not found for listing: {search_path}")
             return [] # Return empty list if prefix doesn't exist
        except Exception as e:
            logger.error(f"Error listing items under prefix '{prefix}' in {search_path}: {e}")
            raise
        return items


    async def exists(self, identifier: str) -> bool:
        """Checks if a file or directory exists asynchronously."""
        full_path = self._get_full_path(identifier)
        exists = await aiofiles.os.path.exists(full_path)
        logger.debug(f"Checked existence for {full_path}: {exists}")
        return exists

    async def delete(self, identifier: str):
        """Deletes a file or directory asynchronously."""
        full_path = self._get_full_path(identifier)
        try:
            if await aiofiles.os.path.isdir(full_path):
                # Use shutil.rmtree for directories, consider making it async if performance critical
                # For now, using sync version within executor or directly if acceptable
                # Note: aiofiles.os doesn't have rmtree
                shutil.rmtree(full_path) # Blocking call
                logger.info(f"Deleted directory: {full_path}")
            elif await aiofiles.os.path.isfile(full_path):
                await aiofiles.os.remove(full_path)
                logger.info(f"Deleted file: {full_path}")
            else:
                logger.warning(f"Attempted to delete non-existent path: {full_path}")
        except Exception as e:
            logger.error(f"Error deleting {full_path}: {e}")
            raise

    async def makedirs(self, identifier: str, exist_ok: bool = True):
        """Ensure that the directory for the identifier exists."""
        full_path = self._get_full_path(identifier)
        # We want the directory *containing* the identifier if it looks like a file path
        dir_path = full_path.parent if '.' in full_path.name else full_path
        try:
            await aiofiles.os.makedirs(dir_path, exist_ok=exist_ok)
            logger.debug(f"Ensured directory exists: {dir_path}")
        except Exception as e:
            logger.error(f"Error creating directory {dir_path}: {e}")
            raise

