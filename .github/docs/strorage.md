print(default_api.create_file(filePath = "c:\\Project\\cyberbuild\\cb-trade\\cb-trade-data-service\\.github\\docs\\storage.md", content = """<!-- filepath: c:\\Project\\cyberbuild\\cb-trade\\cb-trade-data-service\\.github\\docs\\storage.md -->
# Storage Packages Documentation

This document outlines the functionality and design of the storage-related packages within the `cb-trade-data-service`: `src/storage` and `src/target_management`.

## 1. `src/storage` Package (Raw Data Storage)

### Purpose

The `src/storage` package is responsible for the persistent storage and retrieval of raw market data collected from exchanges. This typically includes order book snapshots, trades, or other time-series data. It provides an abstraction layer over different physical storage backends (like local disk or Azure Blob Storage).

### Key Components

*   **`interfaces.py`**:
    *   `IRawDataStorage`: Defines the abstract interface for raw data storage operations (save, get range, get latest, list coins, check coin exists). Concrete storage implementations must adhere to this interface.
    *   `IStorageManager`: Defines the interface for the facade/manager class that orchestrates raw data storage.
*   **`implementations/`**: Contains concrete classes implementing `IRawDataStorage`:
    *   `LocalFileStorage`: Stores raw data as JSON files on the local filesystem, organized by exchange, coin, and timestamp-based directories (Year/Month/Day).
    *   `AzureBlobStorage`: Stores raw data as JSON blobs in Azure Blob Storage, using a similar hierarchical path structure within a specified container.
*   **`storage_manager.py`**:
    *   `StorageManagerImpl`: Implements `IStorageManager`. It acts as a facade, holding an instance of `IRawDataStorage` (determined by configuration) and delegating all storage operations to it. This decouples the rest of the application from the specific storage backend being used.
*   **`path_utility.py`**:
    *   `StoragePathUtility`: Provides static methods to generate standardized, backend-agnostic paths for raw data based on exchange, coin, and timestamp. Ensures consistent organization across different storage types.
    *   `_parse_timestamp`: Internal helper function to parse various timestamp formats into standardized `datetime` objects.
*   **Configuration (`src/config.py`)**:
    *   `StorageConfig`: A Pydantic `BaseSettings` model that reads environment variables prefixed with `STORAGE_`. It determines which `IRawDataStorage` implementation to use (`type: 'local' | 'azure'`) and provides necessary details like root paths or connection strings.

### Diagrams

#### Class Diagram (`src/storage`)

```mermaid
classDiagram
    direction LR

    class IStorageManager {
        <<Interface>>
        +save_entry(exchange, coin, timestamp, data)
        +get_range(exchange, coin, start, end, limit, offset) List~dict~
        +get_latest_entry(exchange, coin) Optional~dict~
        +list_coins(exchange) List~str~
        +check_coin_exists(exchange, coin) bool
    }

    class StorageManagerImpl {
        -raw_data_storage: IRawDataStorage
        +__init__(raw_data_storage: IRawDataStorage)
        +save_entry(...)
        +get_range(...) List~dict~
        +get_latest_entry(...) Optional~dict~
        +list_coins(...) List~str~
        +check_coin_exists(...) bool
    }

    class IRawDataStorage {
        <<Interface>>
        +save_entry(exchange, coin, timestamp, data) None
        +get_range(exchange, coin, start, end, limit, offset) List~dict~
        +get_latest_entry(exchange, coin) Optional~dict~
        +list_coins(exchange) List~str~
        +check_coin_exists(exchange, coin) bool
    }

    class LocalFileStorage {
        -storage_root: str
        +__init__(storage_root: str)
        +save_entry(...) None
        +get_range(...) List~dict~
        +get_latest_entry(...) Optional~dict~
        +list_coins(...) List~str~
        +check_coin_exists(...) bool
    }

    class AzureBlobStorage {
        -connection_string: str
        -container_name: str
        -service_client: BlobServiceClient
        -container_client: ContainerClient
        +__init__(connection_string: str, container_name: str)
        +save_entry(...) None
        +get_range(...) List~dict~
        +get_latest_entry(...) Optional~dict~
        +list_coins(...) List~str~
        +check_coin_exists(...) bool
    }

     class StoragePathUtility {
         <<Utility>>
         +get_raw_data_path(exchange, coin, timestamp) str
     }

     class StorageConfig {
         <<Pydantic Settings>>
         +type: Literal['local', 'azure']
         +local_root_path: str
         +azure_connection_string: SecretStr
         +azure_container_name: str
     }

    StorageManagerImpl ..> IStorageManager : implements
    StorageManagerImpl o-- IRawDataStorage : uses
    LocalFileStorage ..> IRawDataStorage : implements
    AzureBlobStorage ..> IRawDataStorage : implements
    LocalFileStorage ..> StoragePathUtility : uses
    AzureBlobStorage ..> StoragePathUtility : uses
    StorageManagerImpl ..> StorageConfig : configured by
    LocalFileStorage ..> StorageConfig : configured by
    AzureBlobStorage ..> StorageConfig : configured by

sequenceDiagram
    participant App as Application Code
    participant Mgr as StorageManagerImpl
    participant Prov as IRawDataStorage (e.g., LocalFileStorage)
    participant Util as StoragePathUtility

    App->>Mgr: save_entry("binance", "BTC", ts, data)
    Mgr->>Prov: save_entry("binance", "BTC", ts, data)
    Prov->>Util: get_raw_data_path("binance", "BTC", ts)
    Util-->>Prov: returns path ("raw_data/binance/BTC/...")
    Prov->>Prov: Determine full path (e.g., join with root)
    Prov->>Prov: Write data to file/blob at full path
    Prov-->>Mgr: returns (None)
    Mgr-->>App: returns (None)
```
