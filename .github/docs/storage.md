<!-- filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\.github\docs\storage.md -->
# Storage Package Documentation (Revised Design)

This document outlines the functionality and revised design of the storage package (`src/storage`) within the `cb-trade-data-service`.

## 1. `src/storage` Package (Data Storage)

### Purpose

The `src/storage` package provides a flexible system for persistent storage and retrieval of market data (e.g., OHLCV, trades, order books). It aims to abstract the underlying physical storage backend (local disk, Azure Blob/ADLS Gen2) and handle data serialization/deserialization for common formats (Parquet, JSON, Delta Lake).

### Design: Backend Abstraction and Manager Logic

The revised design simplifies the previous layered strategy pattern:

1.  **`IStorageBackend`**: Defines an interface for low-level, format-agnostic access to a specific physical storage medium (local files, Azure Blob/ADLS). It is responsible for translating logical identifiers (relative paths) into physical paths/URIs, managing necessary credentials/options, and performing basic byte-level operations (`save_bytes`, `load_bytes`, `list_items`).
2.  **`StorageManagerImpl`**: Implements the high-level `IStorageManager` interface. It contains the core logic for:
    *   Generating storage paths/identifiers based on domain context (exchange, coin, data type, timestamp).
    *   Handling data serialization/deserialization for different formats (Parquet, JSON, Delta Lake) using libraries like `pyarrow`, `pandas`, and `deltalake`.
    *   Orchestrating read/write operations by interacting with the configured `IStorageBackend`.
    *   Implementing efficient data retrieval and filtering (e.g., predicate pushdown for Delta Lake).

Configuration (e.g., backend type, connection details) is typically handled via dependency injection, often using a configuration object like Pydantic `BaseSettings`.

### Key Components

*   **`interfaces.py`**:
    *   `IStorageManager`: Defines the high-level facade interface for storage operations (`save_entry`, `get_range`, `list_coins`, etc.). Application code interacts primarily with this.
    *   `IStorageBackend`: Interface for physical storage access (byte operations, listing, URI generation, options).
        *   *Implementations:* `LocalFileBackend`, `AzureBlobBackend`.
*   **`backends/`**: Concrete `IStorageBackend` implementations (`local_file_backend.py`, `azure_blob_backend.py`).
*   **`storage_manager.py`**:
    *   `StorageManagerImpl`: Implements `IStorageManager`. It holds a configured instance of `IStorageBackend`. It contains the logic for path generation, format handling (using `pyarrow`, `pandas`, `deltalake`), and orchestrates operations with the backend.
*   **Configuration (`src/config.py`)**:
    *   `StorageConfig`: (Example) A Pydantic `BaseSettings` model. Reads environment variables or config files to determine which `IStorageBackend` implementation to instantiate and provides its necessary configuration (root paths, connection strings, container names). This configuration is used to inject the appropriate backend into `StorageManagerImpl`.

### Diagrams

#### Class Diagram (`src/storage` - Revised)

```mermaid
classDiagram
    direction LR

    class IStorageManager {
        <<Interface>>
        +save_entry(context, data, format_hint)
        +get_range(context, start_time, end_time, filters) List~dict~
        +list_coins(exchange_name, data_type) List~str~
        # ... other high-level operations
    }

    class StorageManagerImpl {
        -backend: IStorageBackend
        +__init__(backend: IStorageBackend)
        +save_entry(context, data, format_hint)
        +get_range(context, start_time, end_time, filters) List~dict~
        +list_coins(exchange_name, data_type) List~str~
        # ... internal methods for path generation, format handling
    }

    class IStorageBackend {
        <<Interface>>
        +get_uri_for_identifier(identifier) str
        +get_storage_options() dict
        +save_bytes(identifier, data: bytes)
        +load_bytes(identifier) bytes
        +list_items(prefix) List~str~
        +exists(identifier) bool
        # ... other low-level operations (delete)
    }
    class LocalFileBackend {
     -root_path: str
     +get_uri_for_identifier(identifier) str # file://...
     +get_storage_options() dict
     +save_bytes(...)
     +load_bytes(...) bytes
     +list_items(...) List~str~
     +exists(...) bool
     # ...
    }
    class AzureBlobBackend {
     -connection_string: str
     -container_name: str
     +get_uri_for_identifier(identifier) str # abfss://...
     +get_storage_options() dict # Returns Azure credentials
     +save_bytes(...) # Uses azure-storage-blob / azure-storage-dfs
     +load_bytes(...) bytes
     +list_items(...) List~str~
     +exists(...) bool
     # ...
    }

     class StorageConfig {
         <<Pydantic Settings>>
         +backend_type: Literal['local', 'azure']
         +local_root_path: str | None
         +azure_connection_string: SecretStr | None
         +azure_container_name: str | None
         # ... other config
     }

    StorageManagerImpl ..> IStorageManager : implements
    StorageManagerImpl o-- IStorageBackend : uses >

    LocalFileBackend ..> IStorageBackend : implements
    AzureBlobBackend ..> IStorageBackend : implements

    StorageManagerImpl ..> StorageConfig : configured by (indirectly via Backend)
    LocalFileBackend ..> StorageConfig : configured by
    AzureBlobBackend ..> StorageConfig : configured by

```

#### Sequence Diagram (Saving OHLCV Data with Delta/Azure)

```mermaid
sequenceDiagram
    participant App as Application Code
    participant Mgr as StorageManagerImpl
    participant Bknd as AzureBlobBackend
    participant DeltaLib as DeltaLake Library
    participant ArrowLib as PyArrow Library
    participant AzureSDK as Azure Storage SDK

    App->>Mgr: save_entry(context={"exchange": "binance", "coin": "BTC", "data_type": "ohlcv", ...}, data=df, format_hint="delta")
    Mgr->>Mgr: _generate_path(context) -> path ("ohlcv/binance/BTC/...")
    Mgr->>Bknd: get_uri_for_identifier(path)
    Bknd-->>Mgr: returns table_uri ("abfss://container/ohlcv/binance/BTC/")
    Mgr->>Bknd: get_storage_options()
    Bknd-->>Mgr: returns storage_options (creds)
    Note over Mgr: Convert df to Arrow Table if needed
    Mgr->>ArrowLib: Table.from_pandas(df) (if df is pandas)
    ArrowLib-->>Mgr: arrow_table
    Mgr->>DeltaLib: write_deltalake(table_uri, arrow_table, mode="append", storage_options=storage_options)
    Note over DeltaLib, Bknd: DeltaLib uses storage_options to authenticate
    DeltaLib->>Bknd: (Internal calls like list_items, save_bytes, etc.)
    Bknd->>AzureSDK: (Interacts with Azure Storage)
    AzureSDK-->>Bknd: returns success/data
    Bknd-->>DeltaLib: returns success/data
    DeltaLib-->>Mgr: returns success
    Mgr-->>App: returns (None)

```

## 2. `src/target_management` Package

*(This section remains unchanged unless the redesign impacts it directly)*

## Format Handling Notes

- The `StorageManagerImpl` is responsible for handling the conversion between in-memory representations (like Pandas DataFrames or PyArrow Tables) and the serialized format on disk (Parquet, JSON files, Delta Tables).
- It uses libraries like `pandas`, `pyarrow`, and `deltalake` for these conversions.
- The `save_entry` method typically accepts a DataFrame or Table and a `format_hint` (e.g., "parquet", "json", "delta").
- The `get_range` method reads data using the appropriate library based on the format (inferred or specified) and returns a list of dictionaries or a DataFrame.
- For Delta Lake, `write_deltalake` and `read_delta` (from the `deltalake` library) are used, leveraging the backend's `storage_options`.
- For Parquet, `pyarrow.parquet.write_table` and `pyarrow.parquet.read_table` (or `read_pandas`) are used, interacting with the backend's `save_bytes` and `load_bytes`.
- For JSON, `pandas.DataFrame.to_json` and `pandas.read_json` can be used with byte streams from the backend.

### Benefits of Revised Design
- Reduced complexity by removing intermediate strategy layers.
- Clear separation of concerns: Backend handles physical IO, Manager handles logic, paths, and formats.
- Easier configuration and setup using dependency injection for the backend.
- Flexibility to add new formats primarily by modifying the `StorageManagerImpl`.
