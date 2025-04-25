# Phase 1 Iteration Plan: Dedicated Data Service (cb-trade)

This document provides a step-by-step iteration plan for implementing Phase 1 of the financial model project: the Foundation Setup & Dedicated Data Service. Each iteration breaks down development into smaller, actionable steps, building upon the architectural overview defined in the Phase 1 Implementation Plan.

**Project Prefix**: `cb-trade`  
**Repository**: `github.com/cyberbuild/cb-trade-data-service`  
**Environment**: Single Conda environment (Python, pandas, NumPy)  
**Storage**: File-based raw data storage by coin symbol, abstracted via interfaces (Local, Azure)  

---

## Iteration 1: Basic Environment & File Storage Setup
**Goal:** Set up the core development environment and establish the file-based data storage abstraction.

1. **Dev Environment Setup**  
   - Create/configure Conda environment for the `cb-trade` system.  
   - Install core libraries: `python`, `pandas`, `numpy`.  
   - Initialize Git repo: `cyberbuild/cb-trade-data-service`.  
   - Scaffold basic project structure.

2. **Design Raw Data File Structure**  
   - Define folder & file naming convention for raw 5‑min order book data:  
     ```text
     [storage_root]/raw_data/[coin_symbol]/[year]/[month]/[day]/[timestamp].json
     ```

3. **Design `IRawDataStorage` Interface**  
   - `save_entry(coin_symbol, timestamp, data)`  
   - `get_range(coin_symbol, start_time, end_time, limit, offset)`  
   - `get_latest_entry(coin_symbol)`  
   - `list_coins()`  
   - `check_coin_exists(coin_symbol)`

4. **Implement `LocalFileStorage`**  
   - Concrete class using local filesystem & defined structure.  
   - Read/write logic per spec.  
   - Unit tests: save, retrieve, list coins.

5. **Implement `AzureBlobStorage` (Stub)**  
   - Stub class implementing `IRawDataStorage`.  
   - Methods raise `NotImplementedError` or return mock data.

6. **Design & Implement `IStorageManager` / `StorageManagerImpl`**  
   - Define `IStorageManager` (orchestrator).  
   - `StorageManagerImpl` depends on an injected `IRawDataStorage`.  
   - Unit tests for correct delegation.

7. **Configure Storage Backend**  
   - Config mechanism (env var or config file) to select `LocalFileStorage` vs. `AzureBlobStorage` at runtime.

---

## Iteration 2: Data Source Connector (Microkernel Core & Basic Plugin)
**Goal:** Implement core Microkernel logic for exchange plugins and a basic/mock exchange client.

2.1 **Design `IExchangeAPIClient`**  
   - Methods: `get_exchange_name()`, `fetch_historical_data()`, `start_realtime_stream()`, `check_coin_availability()`.

2.2 **Implement `PluginRegistry`**  
   - Store `IExchangeAPIClient` instances by exchange name.

2.3 **Design & Implement `IDataSourceConnector` / `DataSourceConnectorImpl`**  
   - Discover/register `IExchangeAPIClient` plugins.  
   - `get_client(exchange_name)` retrieves correct plugin.  
   - Delegate calls (e.g., `fetch_historical_data()`).  
   - Unit tests for plugin retrieval & delegation.

2.4 **Basic/Mock Exchange Plugin**  
   - Implement `MockExchangeClient` or `BasicExchangeClient` with mock/hardcoded data.  
   - Register with `PluginRegistry` at startup.

2.5 **Refine Storage Interfaces for Multi-Exchange**  
   - Update `IRawDataStorage` and implementations to handle `[exchange_name]/[coin_symbol]` in path.

---

## Iteration 3: Real‑Time Data Collection & Storage Integration
**Goal:** Implement real-time data flow, integrating the Data Source Connector and Storage Manager.

3.1 **CoinCollectionList** – manage coins & assigned exchanges.  
3.2 **RealTimeProcessor** – callback handler that stores incoming data via `IStorageManager`.  
3.3 **IDataCollectionManager** / `DataCollectionManagerImpl`  
   - Methods: add coin (select exchange, start stream).  
   - Wire up `RealTimeProcessor` & `IDataSourceConnector`.  
   - Unit tests for coin addition & stream initiation.

---

## Iteration 4: Historical Data Handling & Storage Integration
**Goal:** Implement historical data fetching, integrating the Data Source Connector and Storage Manager.

4.1 **HistoricalFetcher** – orchestrates historical retrieval from `IStorageManager`.  
4.2 **CurrentDataFetcher** – retrieves latest entry.  
4.3 **IHistoricalDataManager** / `HistoricalDataManagerImpl`  
   - Methods: stream historical data, get current data.  
   - Wire up fetchers.  
   - Unit tests for historical & current data retrieval.

---

## Iteration 5: API Endpoints (MCP & WebSocket)
**Goal:** Expose the Data Service via FastAPI, MCP, and WebSockets.

5.1 **FastAPI Setup**  
   - Install `fastapi`, `uvicorn`, `fastapi_mcp`, `websockets`.  
   - Create `app` instance.

5.2 **MCP Endpoints**  
   - Define MCP messages (e.g., AddCoinRequest/Response).  
   - Implement endpoints for Add Coin, Check Availability.  
   - Integration tests (mock managers).

5.3 **WebSocket Endpoint** (`/ws/data`)  
   - Handle messages: `subscribe_realtime`, `request_historical`, `request_current`.  
   - Delegate to managers, send streams/responses.  
   - Integration tests for message flow.

5.4 **Dependency Injection**  
   - Instantiate & inject: `DataCollectionManagerImpl`, `HistoricalDataManagerImpl`, `StorageManagerImpl`, `DataSourceConnectorImpl`, `AuthenticationModuleImpl`.

---

## Iteration 6: Authentication & Security
**Goal:** Secure endpoints with OAuth2.

6.1 **`IAuthenticationModule` / `AuthenticationModuleImpl`** – token validation logic.  
6.2 **FastAPI OAuth2 Integration**  
   - Use `OAuth2PasswordBearer` & `Depends`.  
   - `get_current_user` dependency.  
   - Tests for endpoint authentication.

---

## Iteration 7: Azure Storage & Real Exchange Plugins
**Goal:** Implement Azure backend and real exchange clients.

7.1 **`AzureBlobStorage`** – complete SDK-based implementation, unit tests.  
7.2 **Real `IExchangeAPIClient` Plugins** (e.g., Binance, Coinbase).  
7.3 **Refine Exchange Selection Logic**  
7.4 **Historical Data Fallback** – fetch from exchange if not in storage.  
7.5 **Comprehensive E2E Tests** – simulate client requests across backends & plugins.

---

## Iteration 8: CI/CD Pipeline
**Goal:** Automate build, test, and deployment.

8.1 **CI Pipeline** – GitHub Actions/Azure DevOps: build, unit/integration tests on push.  
8.2 **CD Pipeline** – deploy to staging, health checks.

---

*End of Phase 1 Iteration Plan*