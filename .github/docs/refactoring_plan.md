# Refactoring and Improvement Plan: cb-trade-data-service

**Version:** 1.0
**Date:** 2025-04-26

## 1. Introduction

This document outlines a plan for refactoring and improving the `cb-trade-data-service` codebase. The primary goals are to enhance maintainability, testability, configuration management, error handling, and overall robustness by addressing key areas identified during the initial review and aligning with best practices. This plan focuses on improvements applicable to the codebase up to the completion of Iteration 4.

## 2. Configuration Management Refactoring

**Goal:** Centralize application configuration, separate sensitive secrets, support different environments (development, testing, production), and allow runtime selection of components (e.g., storage backend) in a structured and type-safe manner.

**Current State:** Configuration values are primarily sourced from `.env` and `.env.test` files, potentially leading to scattered access (`os.getenv`) throughout the codebase and lack of structure or validation.

**Proposed Solution:**
*   **Technology:** Utilize Pydantic's `BaseSettings` for defining structured, type-hinted configuration models. Leverage `python-dotenv` for loading environment variables from `.env` files.
*   **Structure:**
    *   Define distinct Pydantic `BaseSettings` models for logical configuration groups (e.g., `StorageConfig`, `TargetManagementConfig`, `ExchangeClientConfig`, `AppConfig`) within a central `src/config.py`.
    *   These models will declare expected configuration variables, their types, and default values where applicable.
    *   Secrets (API keys, connection strings, passwords) will be explicitly handled, potentially using Pydantic's `SecretStr` type, and will *only* reside in `.env` files or be sourced from secure locations like Azure Key Vault in production. They will *not* be hardcoded or checked into version control.
*   **Loading:** The application startup process (e.g., in a future `src/main.py` or an initialization module) will load these settings models. Pydantic `BaseSettings` automatically reads from environment variables and `.env` files.
*   **Usage:** Configuration objects will be passed via dependency injection to the components that need them (e.g., `StorageManagerImpl` receives `StorageConfig`, `TargetManager` receives `TargetManagementConfig`). Components will no longer read environment variables directly.

**Steps:**

1.  **Add Dependencies:** Ensure `pydantic-settings` and `python-dotenv` are listed in `pyproject.toml` and installed in the environment.
    ```toml
    # pyproject.toml
    [project.dependencies]
    # ... other dependencies
    pydantic-settings = "^2.0"
    python-dotenv = "^1.0"
    ```
2.  **Define Settings Models (`src/config.py`):**
    ```python
    # src/config.py
    from pydantic_settings import BaseSettings, SettingsConfigDict
    from pydantic import SecretStr, Field
    from typing import Literal, Dict, Optional

    class StorageConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix='STORAGE_')

        type: Literal['local', 'azure'] = 'local'
        local_root_path: str = './data' # Default for local storage
        azure_connection_string: Optional[SecretStr] = None
        azure_container_name: str = 'rawdata'

    class TargetManagementConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix='TARGET_')

        storage_type: Literal['local', 'azure'] = 'local'
        table_path: str = './data/targets_delta' # Default local path
        azure_storage_options: Optional[Dict[str, str]] = None # For Azure credentials if needed separately

    class CCXTConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix='CCXT_')

        default_exchange: str = 'crypto_com' # Example default
        # Add fields for API keys/secrets if needed by specific exchanges
        # crypto_com_api_key: Optional[SecretStr] = None
        # crypto_com_secret: Optional[SecretStr] = None

    # Add other config models as needed (e.g., AppConfig)

    class Settings(BaseSettings):
        # Top-level settings object to hold nested configs
        storage: StorageConfig = Field(default_factory=StorageConfig)
        target: TargetManagementConfig = Field(default_factory=TargetManagementConfig)
        ccxt: CCXTConfig = Field(default_factory=CCXTConfig)
        # Add other nested configs

        # Example: Load .env file explicitly if needed
        # model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    # Instantiate settings once
    settings = Settings()

    # Optional: Function to get settings easily
    def get_settings() -> Settings:
        return settings
    ```
3.  **Update `.env` Files:** Align variable names in `.env` and `.env.test` with the Pydantic models (including prefixes).
    ```dotenv
    # .env example
    STORAGE_TYPE=local
    STORAGE_LOCAL_ROOT_PATH=./runtime_data
    TARGET_STORAGE_TYPE=local
    TARGET_TABLE_PATH=./runtime_data/targets_delta
    CCXT_DEFAULT_EXCHANGE=crypto_com
    # STORAGE_AZURE_CONNECTION_STRING=... # Keep secrets here or in Key Vault
    ```
4.  **Refactor Components:** Modify constructors (`__init__`) of classes like `StorageManagerImpl`, `LocalFileStorage`, `AzureBlobStorage`, `TargetManager`, `CCXTExchangeClient`, etc., to accept the relevant config objects instead of reading `os.getenv`.
    ```python
    # Example: src/storage/storage_manager.py
    from .interfaces import IStorageManager, IRawDataStorage
    from ..config import StorageConfig # Import the specific config model

    class StorageManagerImpl(IStorageManager):
        def __init__(self, storage_config: StorageConfig, raw_storage_impl: IRawDataStorage):
             self.config = storage_config # Store config if needed later
             self.storage = raw_storage_impl
             # Use self.config values if needed for logic here
             # The raw_storage_impl itself would also receive config in its __init__

        # ... other methods delegate to self.storage ...

    # Example: src/storage/implementations/local_file_storage.py
    from ..interfaces import IRawDataStorage
    from ..path_utility import StoragePathUtility
    from ...config import StorageConfig # Import config
    import os
    import json
    from pathlib import Path

    class LocalFileStorage(IRawDataStorage):
        def __init__(self, config: StorageConfig): # Accept config
            self.root_path = Path(config.local_root_path) # Use config value
            self.path_utility = StoragePathUtility()
            # Ensure root path exists
            self.root_path.mkdir(parents=True, exist_ok=True)

        async def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: datetime, data: dict):
            relative_path = self.path_utility.get_raw_data_path(exchange_name, coin_symbol, timestamp)
            full_path = self.root_path / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            # Use async file writing if adopted (see Section 4)
            with open(full_path, 'w') as f:
                 json.dump(data, f)
        # ... other methods ...
    ```
5.  **Update Instantiation/Dependency Injection:** Modify the application setup code (likely in a future `src/main.py` or a dedicated dependency setup module) to instantiate components using the loaded `settings` object.
    ```python
    # Example: src/main.py (conceptual for future use)
    # from fastapi import FastAPI
    from .config import get_settings
    from .storage import StorageManagerImpl, LocalFileStorage, AzureBlobStorage
    from .target_management import TargetManager
    # ... other imports

    settings = get_settings()
    # app = FastAPI() # Future use

    # --- Dependency Setup ---
    if settings.storage.type == 'local':
        raw_storage = LocalFileStorage(config=settings.storage)
    elif settings.storage.type == 'azure':
        # Ensure azure dependencies are installed and config is valid
        raw_storage = AzureBlobStorage(config=settings.storage)
    else:
        raise ConfigurationError(f"Unsupported storage type: {settings.storage.type}")

    storage_manager = StorageManagerImpl(storage_config=settings.storage, raw_storage_impl=raw_storage)
    target_manager = TargetManager(config=settings.target)
    # Instantiate other managers/connectors similarly, passing relevant config parts

    # --- Inject Dependencies into API Routers/Handlers (Future Use) ---
    # (Using FastAPI's Depends or by passing instances during router inclusion)

    # --- Include Routers (Future Use) ---
    # app.include_router(...)
    ```
6.  **Update Tests:** Modify unit tests to provide mock configuration objects or instances of the settings models instead of manipulating environment variables directly (e.g., using `pytest.MonkeyPatch` less often for config). Integration tests might use `.env.test`.

## 3. Error Handling Standardization

**Goal:** Implement a consistent, informative, and maintainable error handling strategy using custom exceptions.

**Proposed Solution:**
*   Define a hierarchy of custom exception classes in `src/exceptions.py` inheriting from a base `DataServiceError`.
*   Refactor service logic (managers, fetchers, processors, plugins) to raise these specific exceptions instead of generic ones.

**Steps:**

1.  **Create `src/exceptions.py`:**
    ```python
    # src/exceptions.py
    class DataServiceError(Exception):
        """Base class for exceptions in this service."""
        pass

    class ConfigurationError(DataServiceError):
        """Error related to service configuration."""
        pass

    class StorageError(DataServiceError):
        """Error related to data storage operations."""
        pass

    class ExchangeAPIError(DataServiceError):
        """Error interacting with an external exchange API."""
        def __init__(self, message, exchange_name: str = "unknown", status_code: int = None):
            super().__init__(message)
            self.exchange_name = exchange_name
            self.status_code = status_code # Optional: HTTP status from exchange

    class CoinNotAvailableError(DataServiceError):
        """Requested coin is not available on the specified exchange."""
        def __init__(self, message, exchange_name: str, coin_symbol: str):
            super().__init__(message)
            self.exchange_name = exchange_name
            self.coin_symbol = coin_symbol

    # Add other specific exceptions as needed
    ```
2.  **Refactor Code:** Review modules and replace generic exceptions (`ValueError`, `KeyError`, `FileNotFoundError`, `Exception`) with specific custom exceptions where appropriate. Pass relevant context (like exchange name, coin symbol) to the exception constructors.

## 4. Asynchronous Operations (Async/Await)

**Goal:** Leverage Python's `asyncio` to improve performance and scalability for I/O-bound operations (network calls, file system access).

**Proposed Solution:**
*   Identify I/O-bound methods in interfaces (`IRawDataStorage`, `IExchangeAPIClient`) and their implementations.
*   Refactor these methods to be `async def`.
*   Utilize asynchronous libraries: `aiofiles` for local file I/O, `azure-storage-blob`'s async client (`azure.storage.blob.aio`), `ccxt`'s async methods or `ccxt.pro` for exchange interactions, `httpx` for general async HTTP requests if needed, `deltalake`'s async capabilities if available/applicable.
*   Update all calling code to use `await` when invoking these async methods.

**Steps:**

1.  **Identify Async Candidates:** Review methods in `storage/interfaces.py`, `storage/implementations/`, `exchange_source/interfaces.py`, `exchange_source/plugins/`. Focus on file reads/writes and network calls.
2.  **Add Async Dependencies:** Add `aiofiles`, `httpx` (if needed) to `pyproject.toml`. Ensure async versions of Azure SDKs are used if applicable.
3.  **Refactor Interfaces/Implementations:** Change method signatures from `def` to `async def`. Update implementation logic to use `await` with async library calls.
    ```python
    # Example: src/storage/interfaces.py
    from abc import ABC, abstractmethod
    from datetime import datetime
    # ... other imports

    class IRawDataStorage(ABC):
        @abstractmethod
        async def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: datetime, data: dict):
            pass

        @abstractmethod
        async def get_range(self, exchange_name: str, coin_symbol: str, start_time: datetime, end_time: datetime, limit: int = 1000, offset: int = 0) -> list[dict]:
            pass
        # ... other methods async as well ...

    # Example: src/storage/implementations/local_file_storage.py
    import aiofiles
    import json
    # ... other imports

    class LocalFileStorage(IRawDataStorage):
        # ... __init__ ...
        async def save_entry(self, exchange_name: str, coin_symbol: str, timestamp: datetime, data: dict):
            relative_path = self.path_utility.get_raw_data_path(exchange_name, coin_symbol, timestamp)
            full_path = self.root_path / relative_path
            # Ensure parent dir exists (can potentially use async os calls if needed)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(full_path, mode='w') as f:
                await f.write(json.dumps(data))
        # ... implement other methods asynchronously ...
    ```
4.  **Update Calling Code:** Ensure managers (`StorageManagerImpl`, `DataCollectionManagerImpl`, etc.) that call these async methods are themselves `async def` and use `await`.
    ```python
    # Example: src/collection/processor.py
    from ..storage.interfaces import IStorageManager
    # ... other imports

    class RealTimeProcessor:
        def __init__(self, storage_manager: IStorageManager):
            self.storage_manager = storage_manager

        async def process_realtime_entry(self, exchange_name: str, coin_symbol: str, timestamp: datetime, data: dict):
            try:
                await self.storage_manager.save_entry(exchange_name, coin_symbol, timestamp, data)
                # logger.debug(...)
            except StorageError as e:
                # logger.error(...)
                pass # Or re-raise / handle
    ```
5.  **Use Async Test Runner:** Use `pytest-asyncio` for testing async code. Mark test functions with `@pytest.mark.asyncio`.

## 5. Logging Implementation

**Goal:** Introduce structured and configurable logging throughout the application for better observability, monitoring, and debugging.

**Proposed Solution:**
*   Utilize Python's standard `logging` module.
*   Configure logging centrally (e.g., in `src/config.py` or a future `src/main.py`) to set levels, formats, and handlers (console, file, etc.). Configuration could potentially be part of the Pydantic settings.
*   Use module-level loggers (`logging.getLogger(__name__)`) within each file.
*   Add informative log messages at key execution points.

**Steps:**

1.  **Configure Logging:** Set up basic logging configuration early in the application startup (e.g., in `src/config.py` or a future `src/main.py`).
    ```python
    # src/config.py (example addition)
    import logging
    import sys

    # Basic configuration example
    logging.basicConfig(
        level=logging.INFO, # Or load from config settings.log_level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
            # Add FileHandler etc. based on config
        ]
    )

    # Optionally silence noisy libraries
    # logging.getLogger("some_library").setLevel(logging.WARNING)
    ```
2.  **Instantiate Loggers:** In each Python module (`.py` file), get a logger instance:
    ```python
    # e.g., src/storage/storage_manager.py
    import logging
    logger = logging.getLogger(__name__)
    ```
3.  **Add Log Statements:** Add relevant log messages using `logger.info()`, `logger.debug()`, `logger.warning()`, `logger.error()`, `logger.exception()`. Include contextual information.
    ```python
    # Example in DataCollectionManagerImpl.add_coin
    logger.info(f"Attempting to add coin '{coin_symbol}' for exchange '{exchange_name}'.")
    # ... later ...
    if available:
        logger.info(f"Coin '{coin_symbol}' available on '{exchange_name}'. Starting stream.")
        await client.start_realtime_stream(coin_symbol, self.realtime_processor.process_realtime_entry)
    else:
        logger.warning(f"Coin '{coin_symbol}' not available on '{exchange_name}'.")
        raise CoinNotAvailableError(...)

    # Example in exception handling
    except ExchangeAPIError as e:
        logger.error(f"API error checking availability for {coin_symbol} on {e.exchange_name}: {e}", exc_info=True)
        raise # Re-raise or handle
    ```

## 6. Docstrings and Type Hinting

**Goal:** Improve code clarity, readability, and maintainability through comprehensive docstrings and type hints.

**Proposed Solution:**
*   Ensure all public classes, methods, and functions have clear docstrings (e.g., Google style) explaining their purpose, arguments (`Args:`), return values (`Returns:`), and any custom exceptions raised (`Raises:`).
*   Add or complete type hints for all function/method signatures and important class attributes using Python's `typing` module.
*   Consider using `mypy` for static type checking in the CI pipeline.

**Steps:**

1.  **Review Codebase:** Systematically go through files in `src/`.
2.  **Add/Update Docstrings:** Write clear and concise documentation for all public members.
3.  **Add/Update Type Hints:** Ensure type hints are present and accurate.
4.  **(Optional) Add `mypy`:** Add `mypy` to dev dependencies and configure it (e.g., in `pyproject.toml`). Add a `mypy` check step to the CI pipeline.

## 7. Dependency Management Consolidation

**Goal:** Ensure a single source of truth for Python dependencies and maintain reproducible environments.

**Proposed Solution:**
*   Use `pyproject.toml` as the primary definition for Python package dependencies (runtime and optional/development).
*   Minimize redundancy between `pyproject.toml` and `environment.yml`. `environment.yml` can define the Python version and Conda-specific packages, then use `pip` within the Conda environment to install packages defined in `pyproject.toml`.
*   Ensure `setup.py` (if used for editable installs) is minimal and ideally reads metadata from `pyproject.toml`.

**Steps:**

1.  **Consolidate in `pyproject.toml`:** Ensure all Python packages (`pydantic-settings`, `deltalake`, `ccxt`, `pytest`, `aiofiles`, etc.) are listed correctly under `[project.dependencies]` or `[project.optional-dependencies]` (e.g., `dev`, `azure`, `test`).
2.  **Simplify `environment.yml`:** Define the Conda environment name, channels, Python version, and potentially core Conda packages. Include `pip`.
    ```yaml
    # environment.yml (example)
    name: cb-trade-ds
    channels:
      - conda-forge
      - defaults
    dependencies:
      - python=3.12 # Specify Python version
      - pip
      # Add any essential conda-only packages here
      # - package_only_on_conda
    ```
3.  **Update Setup/Install Process:** The standard process would be:
    *   `conda env create -f environment.yml`
    *   `conda activate cb-trade-ds`
    *   `pip install -e .[dev,azure]` (Install the project editable, including optional dev and azure dependencies)
4.  **Review `setup.py`:** If `setup.py` exists, ensure it's not duplicating metadata already in `pyproject.toml`. Often, a minimal `setup.py` containing just `import setuptools; setuptools.setup()` is sufficient when `pyproject.toml` is present.

## 8. Conclusion

Implementing these refactoring steps will significantly improve the structure, maintainability, and robustness of the `cb-trade-data-service`. Standardizing configuration and error handling, embracing asynchronous operations, adding comprehensive logging, and ensuring clear documentation are crucial steps towards building a production-ready service. These changes should be implemented incrementally, ideally with corresponding updates to unit and integration tests.

**Important:** After completing each major refactoring milestone (e.g., Configuration Management, Error Handling), run the full suite of unit tests to verify that no existing functionality has been broken before proceeding to the next milestone.
