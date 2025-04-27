# Phase 1 Iteration Plan: Dedicated Data Service (cb-trade)

This document provides a step-by-step iteration plan for implementing Phase 1 of your financial model project: the Foundation Setup & Dedicated Data Service. This plan breaks down the development of the Data Service into smaller, actionable steps, building upon the architectural overview defined in the Phase 1 Implementation Plan document.

The project prefix will be cb-trade. Repositories will follow the naming convention cb-trade-[service-name]. The Data Service repository will be initialized under the cyberbuild GitHub organization.

The system will utilize a single Conda environment for the entire system for initial development and deployment. (Note: While convenient for development, a production system might benefit from per-service isolated environments).

Raw data storage will be file-based, organized by coin symbol in a folder structure, abstracting access via interfaces to support different storage backends (Local, Azure Storage). A common utility will be used for constructing storage paths. Data retrieval will be on-demand, fetching historical data, filling gaps from the exchange if necessary, and storing the gaps.

Testing Framework: All tests will be implemented using pytest.

## Iteration 1: Basic Environment & File Storage Setup

**Goal**: Set up the core development environment and establish the foundational file-based data storage abstraction.

### 1.1 Dev Environment Setup:
- Create and configure the single Conda environment for the entire cb-trade system. Install core libraries: python, pandas, numpy, pytest, azure-storage-blob, pydantic, pydantic-settings, fastapi, uvicorn, websockets, ccxt.
- Initialize the Git repository for the Data Service: github.com/cyberbuild/cb-trade-data-service.
- Set up basic project structure for the Data Service.

### 1.2 Design Raw Data File Structure & Implement Path Construction Utility:
- Define the standardized file naming convention and folder structure for storing raw 5-minute order book data. The structure is: `[storage_root]/raw_data/[exchange_name]/[coin_symbol]/[year]/[month]/[day]/[timestamp].json`.
- Implement a `StoragePathUtility` class or module with methods to generate these standardized path strings.
  - Step 1: Create a Python file for the utility at the following path: `data-service/src/storage/path_utility.py`.
  - Step 2: Implement `get_raw_data_path(exchange_name, coin_symbol, timestamp)` method.
  - Step 3: Implement `get_coin_base_path(exchange_name, coin_symbol)` method.
  - Step 4: Implement `parse_path(path_string)` method (optional, for extracting info from path).

**Test Cases**
- Test File Path: `data-service/tests/unit/storage/test_path_utility.py`
- Test `get_raw_data_path` with valid inputs (exchange, coin, timestamp) to ensure correct path format.
- Test `get_raw_data_path` with edge cases (e.g., start/end of year/month/day).
- Test `get_coin_base_path` with valid inputs.

### 1.3 Design IRawDataStorage Interface:
- Define the interface `IRawDataStorage` with methods for:
  - `save_entry(exchange_name, coin_symbol, timestamp, data)`: Saves a single raw data entry.
  - `save_bulk_entries(exchange_name, coin_symbol, data_list)`: Saves multiple entries efficiently.
  - `get_range(exchange_name, coin_symbol, start_time, end_time)`: Retrieves data entries within a time range.
  - `get_latest_timestamp(exchange_name, coin_symbol)`: Gets the timestamp of the most recent entry.
  - `list_coins(exchange_name)`: Lists available coin symbols for an exchange.
  - `check_coin_exists(exchange_name, coin_symbol)`: Checks if data exists for a coin/exchange.
- Step 1: Create a Python file for the interface at the following path: `data-service/src/storage/interfaces.py`.
- Step 2: Define the `IRawDataStorage` interface (using abc module) within this file.

### 1.4 Implement LocalFileStorage:
- Implement a concrete class `LocalFileStorage` that implements `IRawDataStorage`.
- Step 1: Create a Python file for the implementation at the following path: `data-service/src/storage/implementations/local_file_storage.py`.
- Step 2: Define the `LocalFileStorage` class within this file, inheriting from `IRawDataStorage`.
- This class will use the `StoragePathUtility` to construct local file paths based on the defined structure.
- Implement logic for reading and writing files according to these paths.
- Write unit tests for `LocalFileStorage` methods.

**Test Cases**
- Test File Path: `data-service/tests/unit/storage/implementations/test_local_file_storage.py`
- Test `save_entry` for a new coin/exchange: verify folder and file creation.
- Test `save_bulk_entries`: verify multiple files are created correctly.
- Test `get_range` with a valid time range and existing data: verify correct entries are returned and sorted.
- Test `get_range` with an empty time range or range with no data: verify empty list is returned.
- Test `get_latest_timestamp` for a coin with data and no data.
- Test `list_coins` for an exchange with and without coins.
- Test `check_coin_exists` for existing and non-existing coins.
- Test error handling for file system issues.

### 1.5 Implement AzureBlobStorage Concrete Class (Stub):
- Create a stub implementation `AzureBlobStorage` that implements `IRawDataStorage` but raises `NotImplementedError` for most methods initially. This allows the rest of the system to be built without a full Azure dependency yet.
- Step 1: Create a Python file for the implementation at the following path: `data-service/src/storage/implementations/azure_blob_storage.py`.
- Step 2: Define the `AzureBlobStorage` class within this file, inheriting from `IRawDataStorage`.
- Step 3: Implement basic constructor accepting connection details (from config).

**Test Cases**
- Test File Path: `data-service/tests/unit/storage/implementations/test_azure_blob_storage.py`
- Test that methods raise `NotImplementedError`.

### 1.6 Design and Implement IStorageManager and StorageManagerImpl:
- Define the `IStorageManager` interface (likely identical to `IRawDataStorage` initially).
- Step 1: Add `IStorageManager` definition to `data-service/src/storage/interfaces.py`.
- Step 2: Create a Python file for the manager implementation at the following path: `data-service/src/storage/manager.py`.
- Step 3: Define the `StorageManagerImpl` class within this file, which will depend on an instance of `IRawDataStorage` (injected dependency).
- Write unit tests for `StorageManagerImpl` methods, ensuring they correctly delegate to the injected `IRawDataStorage` implementation.

**Test Cases**
- Test File Path: `data-service/tests/unit/storage/test_manager.py`
- Test `StorageManagerImpl` methods with an injected `LocalFileStorage` mock/instance: verify calls are correctly passed.
- Test `StorageManagerImpl` methods with an injected `AzureBlobStorage` mock/instance: verify calls are correctly passed.

### 1.7 Configure Storage Backend:
- Implement a configuration mechanism (e.g., environment variables, `config.py` using `pydantic-settings`) to select which `IRawDataStorage` implementation (`LocalFileStorage` or `AzureBlobStorage`) to use at runtime and provide necessary connection details (base path for local, connection string/credentials for Azure).
- Step 1: Define `StorageConfig` in `data-service/src/config.py`.
- Step 2: Update `main.py` or dependency injection setup to instantiate the correct storage backend based on config.

## Iteration 2: Data Source Connector (Microkernel Core & CCXT Plugin)

**Goal**: Implement the core Microkernel logic for exchange connectors and a concrete plugin using CCXT.

### 2.1 Design and Implement IExchangeAPIClient:
- Define the core `IExchangeAPIClient` interface with methods like `get_exchange_name`, `fetch_historical_data`, `check_coin_availability`. (Removed `start_realtime_stream`).
- Step 1: Create a Python file for the interface at the following path: `data-service/src/exchange_source/interfaces.py`.
- Step 2: Define the `IExchangeAPIClient` interface (using abc module) within this file.

### 2.2 Design and Implement PluginRegistry:
- Implement a `PluginRegistry` class to store `IExchangeAPIClient` instances, mapping exchange names to clients.
- Step 1: Create a Python file for the registry at the following path: `data-service/src/exchange_source/microkernel.py`.
- Step 2: Define the `PluginRegistry` class within this file.

**Test Cases**
- Test File Path: `data-service/tests/unit/exchange_source/test_microkernel.py` (Tests for PluginRegistry and DataSourceConnectorImpl)
- Test adding a plugin to the registry.
- Test retrieving a plugin by name.
- Test attempting to retrieve a non-existent plugin.

### 2.3 Design and Implement IDataSourceConnector and DataSourceConnectorImpl (Microkernel):
- Define the `IDataSourceConnector` interface.
- Step 1: Add the `IDataSourceConnector` interface definition to the file created in 2.1: `data-service/src/exchange_source/interfaces.py`.
- Implement `DataSourceConnectorImpl`. This class will:
  - Discover and register `IExchangeAPIClient` plugins (e.g., via entry points or manual registration).
  - Delegate calls like `check_coin_availability` and `fetch_historical_data` to the appropriate registered plugin based on exchange name.
- Step 2: Add the `DataSourceConnectorImpl` class to the file created in 2.2: `data-service/src/exchange_source/microkernel.py`.
- Write unit tests for `DataSourceConnectorImpl`, ensuring correct plugin retrieval and delegation.

**Test Cases**
- Test File Path: `data-service/tests/unit/exchange_source/test_microkernel.py` (Tests for PluginRegistry and DataSourceConnectorImpl)
- Test `get_client` with a registered exchange name: verify the correct plugin instance is returned.
- Test `get_client` with a non-registered exchange name: verify appropriate error handling.
- Test delegation of `check_coin_availability` to a mock plugin.
- Test delegation of `fetch_historical_data` to a mock plugin.
- Test plugin discovery mechanism (if implemented).

### 2.4 Implement a Concrete CCXT IExchangeAPIClient Plugin:
- Implement a concrete class `CCXTExchangeClient` that implements `IExchangeAPIClient` using the CCXT library.
- Step 1: Create a Python file for the CCXT plugin at the following path: `data-service/src/exchange_source/plugins/ccxt_exchange.py`.
- Step 2: Define the `CCXTExchangeClient` class within this file, inheriting from `IExchangeAPIClient`.
- Implement `get_exchange_name`.
- Implement `check_coin_availability` using CCXT.
- Implement `fetch_historical_data` using the exchange's OHLC REST API via CCXT, handling pagination, rate limits, and translating data to the standardized internal format (e.g., list of dicts with 'timestamp', 'open', 'high', 'low', 'close', 'volume'). Focus on Crypto.com initially due to its historical data availability.
- Implement required authentication logic if needed for fetching data.
- Implement a mechanism for this plugin to be discovered and registered (e.g., using `setup.py` entry points or manual registration in `main.py`).

**Test Cases**
- Test File Path: `data-service/tests/unit/exchange_source/plugins/test_ccxt_exchange.py`
- Test `get_exchange_name`.
- Test `check_coin_availability` for known available and non-existing markets.
- Test `fetch_historical_data` for a valid range (mocking the CCXT client): verify data is fetched, translated, and returned correctly.
- Test `fetch_historical_data` error handling (invalid inputs, CCXT errors).
- Test data translation logic.

## Iteration 3: On-Demand Historical Data Handling & Gap Filling

**Goal**: Implement the on-demand historical data fetching flow, including fetching from storage, identifying gaps, fetching gaps from the exchange via the Data Source Connector, merging, storing gaps, and returning the complete dataset.

### 3.1 Design and Implement HistoricalFetcher:
- Implement `HistoricalFetcher`. This class will orchestrate fetching historical data.
- Step 1: Create a Python file for the fetcher at the following path: `data-service/src/historical/fetcher.py`.
- Step 2: Define the `HistoricalFetcher` class within this file. It will depend on `IStorageManager` and `IDataSourceConnector`.
- Step 3: Implement the core method `fetch_data(exchange_name, coin_symbol, start_time, end_time)`:
    - Call `IStorageManager.get_range` to get existing data.
    - Analyze the returned data and the requested range to identify time gaps.
    - For each gap, call `IDataSourceConnector.fetch_historical_data` to get missing data from the exchange.
    - If gap data is retrieved, call `IStorageManager.save_bulk_entries` to store the newly fetched data.
    - Merge the data from storage and the newly fetched gap data, ensuring correct sorting and no duplicates.
    - Return the complete, merged list of data entries for the requested range.

**Test Cases**
- Test File Path: `data-service/tests/unit/historical/test_fetcher.py`
- Test `fetch_data` when all data is in storage: verify `IStorageManager.get_range` is called, `IDataSourceConnector.fetch_historical_data` is NOT called, and correct data is returned.
- Test `fetch_data` when no data is in storage: verify `IStorageManager.get_range` is called, `IDataSourceConnector.fetch_historical_data` is called for the full range, `IStorageManager.save_bulk_entries` is called, and correct data is returned.
- Test `fetch_data` when there's a gap at the beginning/end/middle of the range: verify `IStorageManager.get_range` is called, `IDataSourceConnector.fetch_historical_data` is called for the gap(s), `IStorageManager.save_bulk_entries` is called for the gap data, and the merged/sorted data is returned.
- Test handling of errors from storage or data source connector during fetching.
- Test correct merging and sorting logic.

### 3.2 Design and Implement CurrentDataFetcher:
- Implement `CurrentDataFetcher`. This class will retrieve the latest timestamp from `IStorageManager`. (The actual latest data point can be retrieved via `HistoricalFetcher` with a very recent time range if needed).
- Step 1: Create a Python file for the current data fetcher at the following path: `data-service/src/historical/current_fetcher.py`.
- Step 2: Define the `CurrentDataFetcher` class within this file, depending on `IStorageManager`.
- Step 3: Implement `get_latest_timestamp(exchange_name, coin_symbol)`.

**Test Cases**
- Test File Path: `data-service/tests/unit/historical/test_current_fetcher.py`
- Test `get_latest_timestamp` calls `IStorageManager.get_latest_timestamp`.
- Test handling of None result from storage.

### 3.3 Design IHistoricalDataManager and Implement HistoricalDataManagerImpl:
- Define the `IHistoricalDataManager` interface.
- Step 1: Create a Python file for the interface at the following path: `data-service/src/historical/interfaces.py`.
- Step 2: Define the `IHistoricalDataManager` interface (using abc module) with methods like `stream_historical_data(websocket, exchange_name, coin_symbol, start_time, end_time)` and `get_most_recent_timestamp(exchange_name, coin_symbol)`.
- Implement `HistoricalDataManagerImpl`.
- Step 3: Create a Python file for the manager implementation at the following path: `data-service/src/historical/manager.py`.
- Step 4: Define the `HistoricalDataManagerImpl` class within this file, inheriting from `IHistoricalDataManager` and depending on `HistoricalFetcher`, `CurrentDataFetcher`, and `IDataSourceConnector`.
- Implement `stream_historical_data`:
    - Call `IDataSourceConnector.check_coin_availability` first. If not available, send an error message and return.
    - Call `HistoricalFetcher.fetch_data` to get the complete dataset (which handles storage, gap filling, and saving).
    - Send the data in chunks over the provided WebSocket connection.
    - Send a completion message.
- Implement `get_most_recent_timestamp` by delegating to `CurrentDataFetcher`.

**Test Cases**
- Test File Path: `data-service/tests/unit/historical/test_manager.py`
- Test `stream_historical_data` calls `IDataSourceConnector.check_coin_availability` first.
- Test `stream_historical_data` calls `HistoricalFetcher.fetch_data` if coin is available.
- Test `stream_historical_data` correctly formats and sends data chunks and completion message over the provided WebSocket (mocking the websocket and fetcher).
- Test `stream_historical_data` sends an error message if coin is not available.
- Test `stream_historical_data` handles errors during fetching or streaming.
- Test `get_most_recent_timestamp` calls `CurrentDataFetcher`.

## Iteration 4: API Endpoints (WebSocket)

**Goal**: Implement the external API for the Data Service using FastAPI and WebSockets for on-demand data requests.

### 4.1 Set up FastAPI Application:
- Create the main FastAPI application instance.
- Install `fastapi`, `uvicorn`, `websockets`.
- Step 1: Create the main application entry point file at the following path: `data-service/src/main.py`.
- Step 2: Initialize the FastAPI app instance within this file.

### 4.2 Design and Implement WebSocket Endpoint:
- Implement the main WebSocket endpoint (e.g., `/ws/data`) using FastAPI's WebSocket support.
- Step 1: Create a Python file for API models (if needed) at `data-service/src/models.py`. Define Pydantic models for WebSocket messages (e.g., `HistoricalDataRequest`, `CurrentTimestampRequest`, `DataChunkResponse`, `CompletionResponse`, `ErrorResponse`).
- Step 2: Create a Python file for the API endpoint handlers at `data-service/src/api/endpoints.py`.
- Step 3: Add the WebSocket endpoint implementation to `endpoints.py`.
- Implement logic to handle incoming WebSocket messages:
    - `request_historical`: Parse request, call `IHistoricalDataManager.stream_historical_data`.
    - `request_current_timestamp`: Parse request, call `IHistoricalDataManager.get_most_recent_timestamp`, send response.
- Implement logic to send responses and data streams (`historical_data_chunk`, `historical_data_complete`, `current_timestamp_response`, `error_response`) over the WebSocket.
- Write integration tests for the WebSocket endpoint (mocking managers and testing message flow).

**Test Cases**
- Test File Path: `data-service/tests/integration/api/test_websocket_endpoint.py`
- Test WebSocket connection handshake.
- Test `request_historical` message: verify `IHistoricalDataManager.stream_historical_data` is called and data/completion messages are sent back.
- Test `request_current_timestamp` message: verify `IHistoricalDataManager.get_most_recent_timestamp` is called and the result is sent back.
- Test sending historical data chunks and completion message over the WebSocket.
- Test sending error messages (e.g., coin not available, fetch error).
- Test handling of invalid WebSocket messages or formats.
- Test handling of WebSocket disconnection during streaming.

### 4.3 Wire up Dependencies:
- In the Data Service's main application startup (`data-service/src/main.py`), instantiate all concrete implementations (`HistoricalDataManagerImpl`, `StorageManagerImpl`, `DataSourceConnectorImpl`, `HistoricalFetcher`, `CurrentDataFetcher`, etc.) and inject them into the API endpoint handlers and each other where needed. Inject the chosen `IRawDataStorage` implementation into `StorageManagerImpl`. Register exchange plugins with the `PluginRegistry`.

## Iteration 5: Authentication & Security

**Goal**: Implement authentication for securing the Data Service WebSocket endpoint.

### 5.1 Design and Implement IAuthenticationModule and AuthenticationModuleImpl:
- Define the `IAuthenticationModule` interface.
- Step 1: Create a Python file for the interface at `data-service/src/auth/interfaces.py`.
- Step 2: Define the `IAuthenticationModule` interface (using abc module) with a method like `validate_token(token)`.
- Implement `AuthenticationModuleImpl` containing the logic for token validation (e.g., simple check, JWT validation, external provider call).
- Step 3: Create a Python file for the implementation at `data-service/src/auth/module.py`.
- Step 4: Define `AuthenticationModuleImpl` within this file.

**Test Cases**
- Test File Path: `data-service/tests/unit/auth/test_module.py`
- Test `validate_token` with valid, invalid, and potentially expired tokens.

### 5.2 Integrate Authentication into WebSocket Endpoint:
- Modify the WebSocket endpoint handler in `data-service/src/api/endpoints.py` to require authentication.
- Step 1: Accept the token during the WebSocket connection (e.g., as a query parameter or subprotocol).
- Step 2: Use the injected `IAuthenticationModule` to validate the token upon connection.
- Step 3: If validation fails, close the WebSocket connection with an appropriate code.
- Write integration tests to ensure the endpoint requires authentication.

**Test Cases**
- Test File Path: `data-service/tests/integration/api/test_websocket_endpoint.py` (Add auth tests)
- Test connecting to WebSocket with a valid token: verify connection is accepted.
- Test connecting to WebSocket with an invalid/missing token: verify connection is rejected.

## Iteration 6: Azure Storage Implementation & Multi-Exchange Plugins

**Goal**: Implement the Azure Storage backend fully and add support for more exchange plugins.

### 6.1 Implement AzureBlobStorage:
- Fully implement the concrete class `AzureBlobStorage`, completing its implementation of `IRawDataStorage` using the Azure Blob Storage SDK (`azure-storage-blob`).
- Step 1: Update the file created in 1.5: `data-service/src/storage/implementations/azure_blob_storage.py`.
- Use `StoragePathUtility` for blob paths.
- Implement logic for connecting, creating containers, reading/writing/listing blobs according to the defined structure. Handle potential Azure API errors.
- Write comprehensive unit tests for `AzureBlobStorage`.

**Test Cases**
- Test File Path: `data-service/tests/unit/storage/implementations/test_azure_blob_storage.py`
- Test `save_entry` / `save_bulk_entries` to Azure: verify blobs are created correctly.
- Test `get_range` from Azure: verify correct blobs are retrieved and data parsed/sorted.
- Test `get_latest_timestamp` from Azure.
- Test `list_coins` from Azure.
- Test `check_coin_exists` in Azure.
- Test handling of Azure API errors (mocking the Azure client).

### 6.2 Implement Additional Real IExchangeAPIClient Plugins:
- Choose one or two more real exchange APIs (e.g., Binance, Kraken, Coinbase).
- Implement concrete `IExchangeAPIClient` classes for these exchanges using CCXT or their native libraries.
- Step 1: Create a Python file for each new plugin (e.g., `data-service/src/exchange_source/plugins/binance_exchange.py`).
- Step 2: Define the concrete class, inheriting from `IExchangeAPIClient`.
- Implement `get_exchange_name`, `check_coin_availability`, `fetch_historical_data`. (No `start_realtime_stream`).
- Ensure they translate data to the standardized internal format.
- Ensure they can be discovered and registered.

**Test Cases**
- Test File Path: `data-service/tests/unit/exchange_source/plugins/test_[exchange_name]_exchange.py`
- Test implementation of `IExchangeAPIClient` methods for each new plugin.
- Test `fetch_historical_data` (mocking API calls).
- Test error handling and data translation.

### 6.3 Refine Exchange Selection Logic (Optional):
- If needed, update `DataSourceConnectorImpl` or the calling code (e.g., `HistoricalDataManagerImpl`) to handle scenarios where a coin might be available on multiple exchanges, potentially using a configuration-based preference or allowing the client to specify the exchange.

**Test Cases**
- Test File Path: `data-service/tests/unit/exchange_source/test_microkernel.py` or `data-service/tests/unit/historical/test_manager.py`
- Test exchange selection based on configuration or request parameters.

### 6.4 Comprehensive Testing:
- Write end-to-end integration tests for the Data Service, simulating client WebSocket requests and verifying data flow with different storage backends (`LocalFileStorage` and `AzureBlobStorage`) and exchange plugins.
- Test File Path: `data-service/tests/integration/test_data_service_e2e.py`
- Simulate `request_historical` for a range fully in storage.
- Simulate `request_historical` for a range partially in storage (requiring gap fill).
- Simulate `request_historical` for a range only on the exchange (initial fetch).
- Simulate `request_current_timestamp`.
- Simulate requests using different exchange plugins.
- Test error handling scenarios (invalid coin, exchange API down, storage errors).
- Test with both Local and Azure storage backends configured.

## Iteration 7: CI/CD Pipeline for Data Service

**Goal**: Automate testing and deployment of the Data Service to Azure Container Instances (ACI) or another container service.

### 7.1 Set up CI Pipeline:
- Configure a CI server (e.g., GitHub Actions, Azure DevOps Pipelines).
- Automate installing dependencies (using the Conda environment file).
- Automate running linters/formatters.
- Automate running unit tests and integration tests (potentially needing mock services or Azure credentials for integration tests).
- Automate building the Docker container image for the Data Service.
- Push the container image to a registry (e.g., Azure Container Registry, Docker Hub).

### 7.2 Set up CD Pipeline:
- Automate deploying the Data Service container image from the registry to the chosen hosting environment (e.g., Azure Container Instances, Azure Kubernetes Service, Azure Web Apps for Containers).
- Configure necessary environment variables (storage connection strings, API keys, etc.) in the deployment environment.
- Implement basic health checks for the deployed instance.

This phased iteration plan provides a structured approach to building the Data Service, focusing on an on-demand historical data retrieval model with gap-filling capabilities. Remember to prioritize writing tests at each step.