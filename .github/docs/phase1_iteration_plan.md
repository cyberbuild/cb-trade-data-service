# Phase 1 Iteration Plan: Dedicated Data Service (cb-trade)

This document provides a step-by-step iteration plan for implementing Phase 1 of your financial model project: the Foundation Setup & Dedicated Data Service. This plan breaks down the development of the Data Service into smaller, actionable steps, building upon the architectural overview defined in the Phase 1 Implementation Plan document.

- **Project prefix:** `cb-trade`
- **Repository naming:** `cb-trade-[service-name]`
- **Organization:** cyberbuild GitHub organization
- **Environment:** Single Conda environment for initial development and deployment
- **Storage:** File-based, organized by coin symbol, with interfaces for different backends (Local, Azure Storage)
- **Coin registration:** Determined by presence of coin folders in storage
- **Path utility:** Common utility for constructing storage paths

---

## Iteration 1: Basic Environment & File Storage Setup
**Goal:** Set up the core development environment and establish the foundational file-based data storage abstraction.

### 1.1 Dev Environment Setup
- Create and configure the single Conda environment for the entire cb-trade system.
- Install core libraries: python, pandas, numpy.
- Initialize the Git repository for the Data Service: github.com/cyberbuild/cb-trade-data-service.
- Set up basic project structure for the Data Service.

### 1.2 Design Raw Data File Structure & Implement Path Construction Utility
- Define the standardized file naming convention and folder structure for storing raw 5-minute order book data. The structure is:

  ```text
  [storage_root]/raw_data/[exchange_name]/[coin_symbol]/[year]/[month]/[day]/[timestamp].json
  ```

  - `[storage_root]`: Configurable base path (local dir or Azure container)
  - `raw_data/`: Static sub-directory
  - `[exchange_name]/`: Directory for the exchange (e.g., binance/)
  - `[coin_symbol]/`: Directory for the coin (e.g., BTC/)
  - `[year]/[month]/[day]/`: Nested by date
  - `[timestamp].json`: File name (timestamped)

- Implement a `StoragePathUtility` class/module with methods to generate these standardized path strings.

**Steps:**
1. Create `data-service/src/storage/path_utility.py`.
2. Define `StoragePathUtility` class/module.
3. Add `get_raw_data_path(exchange_name, coin_symbol, timestamp)` method.
4. Extract year, month, day, and format timestamp for filename.
5. Construct path: `raw_data/{exchange_name}/{coin_symbol}/{year}/{month}/{day}/{timestamp_filename}.json` (excluding `[storage_root]`).
6. Return the constructed path string.

**Test Cases:**
- Valid inputs (exchange, coin, timestamp)
- Edge cases (start/end of year/month)
- Different data types for inputs

### 1.3 Design IRawDataStorage Interface
- Define the interface `IRawDataStorage` with methods for:
  - `save_entry(exchange_name, coin_symbol, timestamp, data)`
  - `get_range(exchange_name, coin_symbol, start_time, end_time, limit, offset)`
  - `get_latest_entry(exchange_name, coin_symbol)`
  - `list_coins(exchange_name)`
  - `check_coin_exists(exchange_name, coin_symbol)`

**Steps:**
1. Create `data-service/src/storage/interfaces.py`.
2. Define `IRawDataStorage` interface (using `abc` module).

### 1.4 Implement LocalFileStorage
- Implement `LocalFileStorage` class in `data-service/src/storage/implementations/local_file_storage.py`, inheriting from `IRawDataStorage`.
- Use `StoragePathUtility` for file paths.
- Implement file read/write logic.
- Write unit tests for all methods.

**Test Cases:**
- Save entry (new/existing coin/exchange)
- Get range (valid/empty/no data, limit/offset)
- Get latest entry (with/without data)
- List coins (with/without coins)
- Check coin exists (existing/non-existing)
- Error handling (permissions, disk full)

### 1.5 Implement AzureBlobStorage (Initial Stub)
- Implement `AzureBlobStorage` in `data-service/src/storage/implementations/azure_blob_storage.py`, inheriting from `IRawDataStorage`.
- Use `StoragePathUtility` for blob paths/names.
- Methods initially raise `NotImplementedError` or return mock data.

**Test Cases:**
- Methods raise `NotImplementedError` as expected

### 1.6 Design and Implement IStorageManager and StorageManagerImpl
- Define `IStorageManager` interface (may be similar to `IRawDataStorage` or add orchestration logic).
- Implement `StorageManagerImpl` in `data-service/src/storage/manager.py`, depending on an injected `IRawDataStorage`.
- Write unit tests for correct delegation.

**Test Cases:**
- Methods with injected `LocalFileStorage` mock/instance
- Methods with injected `AzureBlobStorage` stub

### 1.7 Configure Storage Backend
- Implement a configuration mechanism (env vars, config file) to select which `IRawDataStorage` implementation to use at runtime.

---

## Iteration 2: Data Source Connector (Microkernel Core & Basic Plugin)
**Goal:** Implement the core Microkernel logic and a basic, perhaps mocked, exchange plugin.

### 2.1 Design and Implement IExchangeAPIClient
- Define `IExchangeAPIClient` interface with methods: `get_exchange_name`, `fetch_historical_data`, `start_realtime_stream`, `check_coin_availability`.
  - File: `data-service/src/data_source/interfaces.py`

### 2.2 Design and Implement PluginRegistry
- Implement `PluginRegistry` in `data-service/src/data_source/microkernel.py` to store `IExchangeAPIClient` instances.

**Test Cases:**
- Add/retrieve plugins, handle non-existent plugins, unique names, duplicates

### 2.3 Design and Implement IDataSourceConnector and DataSourceConnectorImpl (Microkernel)
- Define `IDataSourceConnector` interface in `data-service/src/data_source/interfaces.py`.
- Implement `DataSourceConnectorImpl` in `data-service/src/data_source/microkernel.py`:
  - Discover/register plugins (initially manual)
  - `get_client(exchange_name)`
  - Delegate calls to plugin
- Write unit tests for correct plugin retrieval and delegation

**Test Cases:**
- `get_client` with registered/unregistered names
- Delegation of methods to mock plugin
- Plugin discovery mechanism (if implemented)

### 2.4 Implement a Basic/Mock IExchangeAPIClient Plugin
- Create `MockExchangeClient` (or similar) in `data-service/src/data_source/plugins/mock_exchange.py`, inheriting from `IExchangeAPIClient`.
- Implement all interface methods, return/yield mock data.
- Register plugin with `PluginRegistry` at startup.

**Test Cases:**
- All methods of the plugin
- Correct name, availability, data structure, callback behavior

### 2.5 Refine IRawDataStorage and Implement Multi-Exchange Storage
- Update `IRawDataStorage` and implementations to handle `exchange_name` in storage paths.

**Test Cases:**
- Update tests to include `exchange_name` in method calls and verify exchange-specific storage

---

## Iteration 3: Real-Time Data Collection & Storage Integration
**Goal:** Implement the real-time data collection flow, integrating the Data Source Connector and Storage Manager.

### 3.1 Design and Implement CoinCollectionList
- Implement `CoinCollectionList` in `data-service/src/collection/list.py` to manage coins and assigned exchanges.

**Test Cases:**
- Add/get coins and exchanges, handle missing coins, list all coins

### 3.2 Design and Implement RealTimeProcessor
- Implement `RealTimeProcessor` in `data-service/src/collection/processor.py` to receive raw data entries, store via `IStorageManager`, and prepare for distribution.

**Test Cases:**
- Process entries, handle storage errors, queue data for distribution

### 3.3 Refine IDataCollectionManager and DataCollectionManagerImpl
- Update interface/implementation to add coins (select exchange, start stream).
- Wire up `RealTimeProcessor` to receive data from plugin callback.
- Add coin and exchange to `CoinCollectionList`.
- Write unit tests for adding coins and initiating streams.

**Test Cases:**
- Add coins (new, unavailable, already collected), exchange selection, callback wiring

---

## Iteration 4: Historical Data Handling & Storage Integration
**Goal:** Implement the historical data fetching flow, integrating the Data Source Connector and Storage Manager.

### 4.1 Design and Implement HistoricalFetcher
- Implement `HistoricalFetcher` in `data-service/src/historical/fetcher.py` to orchestrate fetching historical data, primarily from `IStorageManager`.
- (Optional) Add logic to fetch from plugin if data is not in storage.

**Test Cases:**
- Fetch data from storage, handle empty results, (optionally) fetch from plugin

### 4.2 Design and Implement CurrentDataFetcher
- Implement `CurrentDataFetcher` in `data-service/src/historical/current_fetcher.py` to retrieve the latest entry from `IStorageManager`.

**Test Cases:**
- Get latest entry, handle data/None results

### 4.3 Refine IHistoricalDataManager and HistoricalDataManagerImpl
- Update interface/implementation to stream historical data and get current data.
- Wire up to use `HistoricalFetcher` and `CurrentDataFetcher`.
- Write unit tests for retrieving historical/current data.

**Test Cases:**
- Stream data, formatting, completion, error handling, current data retrieval

---

## Iteration 5: API Endpoints (MCP & WebSocket)
**Goal:** Implement the external API for the Data Service using FastAPI, MCP, and WebSockets.

### 5.1 Set up FastAPI Application
- Create `data-service/src/main.py` and initialize FastAPI app instance.
- Install `fastapi`, `uvicorn`, `fastapi_mcp`, `websockets`.

### 5.2 Design and Implement MCP Endpoints
- Define MCP message structures (AddCoinRequest, AddCoinResponse, etc.) in `data-service/src/models.py` (Pydantic models).
- Implement FastAPI endpoints using `fastapi_mcp` in `data-service/src/api/endpoints.py`.
- Wire endpoints to call methods on `IDataCollectionManager`.
- Write integration tests (mocking the manager).

**Test Cases:**
- Endpoints with valid/invalid tokens, manager failures, correct responses

### 5.3 Design and Implement WebSocket Endpoint
- Implement `/ws/data` endpoint in `data-service/src/api/endpoints.py` using FastAPI's WebSocket support.
- Handle messages: `subscribe_realtime`, `request_historical`, `request_current`.
- Wire handlers to call methods on `IDataCollectionManager` and `IHistoricalDataManager`.
- Send data streams and responses over the WebSocket.
- Write integration tests for message flow.

**Test Cases:**
- Handshake/authentication, message handling, data streaming, error handling

### 5.4 Wire up Dependencies
- In `data-service/src/main.py`, instantiate all concrete implementations and inject them into API handlers and each other as needed.
- Inject the chosen `IRawDataStorage` implementation into `StorageManagerImpl`.

---

## Iteration 6: Authentication & Security
**Goal:** Implement OAuth authentication for securing the Data Service.

### 6.1 Design and Implement IAuthenticationModule and AuthenticationModuleImpl
- Define `IAuthenticationModule` interface in `data-service/src/auth/interfaces.py`.
- Implement `AuthenticationModuleImpl` and `TokenValidator` in `data-service/src/auth/module.py`.

**Test Cases:**
- Token validation with valid/invalid/expired tokens

### 6.2 Integrate OAuth2 into FastAPI Endpoints
- Use FastAPI's `OAuth2PasswordBearer` and `Depends` to secure endpoints.
- Implement `get_current_user` dependency in `data-service/src/auth/module.py`.
- Update API endpoint handlers to use `Depends(get_current_user)`.
- Write integration tests to ensure endpoints require authentication.

**Test Cases:**
- Access with/without/invalid tokens, unsecured endpoints

---

## Iteration 7: Azure Storage Implementation & Multi-Exchange Plugins
**Goal:** Implement the Azure Storage backend and add support for real exchange plugins.

### 7.1 Implement AzureBlobStorage
- Complete `AzureBlobStorage` in `data-service/src/storage/implementations/azure_blob_storage.py` using Azure Blob Storage SDK.
- Use `StoragePathUtility` for blob paths/names.
- Implement logic for connecting to Azure, creating containers/folders, and reading/writing blobs.
- Write unit tests for all methods.

**Test Cases:**
- Save, retrieve, list, check coins in Azure Blob Storage, error handling

### 7.2 Implement Real IExchangeAPIClient Plugins
- Choose one or two real exchange APIs (e.g., Binance, Coinbase).
- Implement concrete `IExchangeAPIClient` classes in `data-service/src/data_source/plugins/`.
- Ensure translation to standardized internal format.
- Implement plugin discovery mechanism.

**Test Cases:**
- All plugin methods, data translation, rate limits, error handling

### 7.3 Refine Exchange Selection Logic
- Update `DataCollectionManagerImpl` to handle coins available on multiple exchanges, with more sophisticated selection logic.

**Test Cases:**
- Exchange selection with config mapping, fallback scenarios

### 7.4 Refine Historical Data Fetching (from Source)
- Implement logic in `IHistoricalDataManagerImpl` and `HistoricalFetcher` to fetch historical data from plugin if not found in storage.

**Test Cases:**
- Stream data from storage, plugin, handle overlaps/errors

### 7.5 Comprehensive Testing
- Write end-to-end integration tests for the Data Service, simulating client requests and verifying data flow with different storage backends and plugins.

**Test Cases:**
- Add coins, request data, multiple clients, error scenarios

---

## Iteration 8: CI/CD Pipeline for Data Service
**Goal:** Automate testing and deployment of the Data Service to Azure Container Instances (ACI).

### 8.1 Set up CI Pipeline
- Configure a CI server (e.g., Jenkins, GitHub Actions, Azure DevOps Pipelines).
- Automate building the Data Service code, running unit/integration tests on every code push.
- Automate building the Docker container image for the Data Service.

### 8.2 Set up CD Pipeline
- Automate deploying the Data Service container image to Azure Container Instances (ACI) upon successful CI build.
- Implement basic health checks for the deployed ACI instance.

---

This phased iteration plan provides a structured approach to building the Data Service, starting with foundational components and gradually adding complexity and features, incorporating the specific requirements for file-based storage, multiple storage backends, the Microkernel pattern for exchange connectors, deployment to Azure Container Instances (ACI), and detailed test cases for each implementation step. Remember to prioritize writing tests at each step to ensure the reliability of your service.
