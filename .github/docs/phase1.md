# Phase 1 Implementation Plan: Dedicated Data Service (UML)

This document provides a concrete implementation plan for the Data Service, the foundational component of your real-time adaptive crypto trading system. This service is responsible for all raw 5-minute order book data ingestion, storage, and distribution to other services (Build, Stage, Live).

The Data Service will be implemented using Python with FastAPI for the web framework, fastapi_mcp for Model Context Protocol (MCP) communication, WebSockets for streaming and real-time data, OAuth for authentication, and a centralized database for storage. All key components will be designed and implemented against interfaces or abstraction classes to ensure testability and maintainability. The Data Source Connector module will utilize a Microkernel plugin pattern to support multiple cryptocurrency exchanges.

---

## High-Level Module Dependency

```mermaid
graph TD
    A["API Endpoints\n(FastAPI/MCP/WebSocket)"] --> B["Data Collection Manager"]
    A --> C["Historical Data Manager"]
    A --> D["Storage Manager"]
    A --> E["Authentication Module"]
    B --> F["Data Source Connector"]
    B --> D
    C --> F
    C --> D
    F --> G["External Exchange API Plugins"]
    D --> H["Centralized Raw Data Storage"]
```

**Module Descriptions:**
- **API Endpoints:** Handles incoming requests and WebSocket connections.
- **Data Collection Manager:** Manages the list of coins being collected and processes real-time data streams.
- **Historical Data Manager:** Handles requests for historical data.
- **Storage Manager:** Abstracts interactions with the raw data storage.
- **Authentication Module:** Validates incoming requests using OAuth.
- **Data Source Connector:** Implements the Microkernel, discovering and utilizing exchange plugins to handle low-level communication with external exchange APIs.
- **External Exchange API Plugins:** Concrete implementations for specific exchanges, adhering to a defined interface.
- **Centralized Raw Data Storage:** The persistent storage for raw data.

## Principle: Implementing Against Interfaces

To ensure testability and flexibility, concrete classes will implement interfaces or inherit from abstract base classes. Code that depends on a component will depend on its interface/abstraction, not the concrete implementation. This allows for easy substitution with mock objects during testing or alternative implementations in the future.

## Module Details and Implementation (UML Focus)

### API Endpoints Module

This module is the external interface of the Data Service, built using FastAPI. It defines the endpoints for MCP communication and manages WebSocket connections.

**Responsibilities:**
- Receive and route incoming MCP requests (Add Coin, Check Availability, Historical Data Download)
- Manage WebSocket connections for real-time data streams, historical data streaming, and "Most Current Data" requests
- Validate requests using the Authentication Module dependency (via its interface)
- Interact with the Data Collection Manager and Historical Data Manager dependencies (via their interfaces)

**Key Classes and Relationships:**
```mermaid
classDiagram
    class FastAPIApp
    class MCPRouter
    class WebSocketEndpoint
    class HTTP_Endpoint

    class IAuthenticationModule {
        +validate_token(token)
    }
    class IDataCollectionManager {
        +add_coin(coin)
        +check_availability(coin)
        +stream_realtime_data(coin, ws)
    }
    class IHistoricalDataManager {
        +stream_historical_data(coin, start, end, ws)
        +get_most_current_data(coin)
    }

    FastAPIApp "1" -- "*" MCPRouter : associates
    FastAPIApp "1" -- "*" WebSocketEndpoint : associates
    FastAPIApp "1" -- "*" HTTP_Endpoint : associates

    MCPRouter ..> IAuthenticationModule : depends on
    MCPRouter ..> IDataCollectionManager : depends on

    WebSocketEndpoint ..> IAuthenticationModule : depends on
    WebSocketEndpoint ..> IDataCollectionManager : depends on
    WebSocketEndpoint ..> IHistoricalDataManager : depends on

    HTTP_Endpoint ..> IAuthenticationModule : depends on
    HTTP_Endpoint ..> IDataCollectionManager : depends on
```

### Data Source Connector Module (Microkernel)

This module acts as the Microkernel, responsible for discovering and utilizing exchange-specific plugins to interact with external APIs.

**Plugin Interface (IExchangeAPIClient):**
```mermaid
classDiagram
    class IExchangeAPIClient {
        +get_exchange_name() string
        +fetch_historical_data(coin, start, end) List<Dict>
        +start_realtime_stream(coin, callback) void
        +check_coin_availability(coin) boolean
        # Internal methods for auth, rate limiting specific to exchange
    }
```

**Key Classes and Relationships:**
```mermaid
classDiagram
    class IDataSourceConnector {
        +get_client(exchange_name) IExchangeAPIClient
        +check_coin_availability(exchange_name, coin) boolean
        +start_realtime_stream(exchange_name, coin, callback) void
        +fetch_historical_data(exchange_name, coin, start, end) List<Dict>
    }
    class DataSourceConnectorImpl
    class IExchangeAPIClient {
        +get_exchange_name() string
        +fetch_historical_data(coin, start, end) List<Dict>
        +start_realtime_stream(coin, callback) void
        +check_coin_availability(coin) boolean
    }
    class ExchangeAPIClientPlugin1
    class ExchangeAPIClientPlugin2
    class PluginRegistry
    class ExternalExchangeAPI

    IDataSourceConnector <|-- DataSourceConnectorImpl
    IExchangeAPIClient <|-- ExchangeAPIClientPlugin1
    IExchangeAPIClient <|-- ExchangeAPIClientPlugin2

    DataSourceConnectorImpl "1" -- "1" PluginRegistry : associates with
    PluginRegistry "1" -- "*" IExchangeAPIClient : manages plugins

    ExchangeAPIClientPlugin1 "1" -- "1" ExternalExchangeAPI : associates with
    ExchangeAPIClientPlugin2 "1" -- "1" ExternalExchangeAPI : associates with

    DataSourceConnectorImpl ..> IExchangeAPIClient : depends on
```

### Data Collection Manager Module

This module manages the list of coins for which real-time data is being collected and processes the incoming real-time data stream.

```mermaid
classDiagram
    class IDataCollectionManager {
        +add_coin(coin)
        +stream_realtime_data(coin, ws)
        +check_availability(coin)
    }
    class DataCollectionManagerImpl
    class CoinCollectionList {
        +add_coin(coin, exchange)
        +get_exchange(coin)
    }
    class RealTimeProcessor {
        -process_realtime_entry(coin, data)
    }
    class AvailabilityChecker
    class IStorageManager {
        +store_raw_data(coin, data)
    }
    class IDataSourceConnector {
        +get_client(exchange_name) IExchangeAPIClient
        +check_coin_availability(exchange_name, coin) boolean
    }
    class IExchangeAPIClient {
        +start_realtime_stream(coin, callback) void
        +check_coin_availability(coin) boolean
    }

    IDataCollectionManager <|-- DataCollectionManagerImpl

    DataCollectionManagerImpl "1" -- "1" CoinCollectionList : associates with
    DataCollectionManagerImpl "1" -- "1" RealTimeProcessor : associates with
    DataCollectionManagerImpl "1" -- "1" AvailabilityChecker : associates with

    DataCollectionManagerImpl ..> IDataSourceConnector : depends on
    DataCollectionManagerImpl ..> IStorageManager : depends on

    RealTimeProcessor ..> IStorageManager : depends on
    AvailabilityChecker ..> IDataSourceConnector : depends on
    DataCollectionManagerImpl ..> IExchangeAPIClient : depends on
```

### Historical Data Manager Module

This module handles requests for historical data and manages streaming it over WebSockets.

```mermaid
classDiagram
    class IHistoricalDataManager {
        +stream_historical_data(coin, start, end, ws)
        +get_most_current_data(coin)
    }
    class HistoricalDataManagerImpl
    class HistoricalFetcher
    class StreamingHandler
    class CurrentDataFetcher
    class IStorageManager {
        +get_raw_data_range(coin, start, end, limit, offset)
        +get_latest_raw_entry(coin)
    }
    class IDataSourceConnector {
        +get_client(exchange_name) IExchangeAPIClient
    }
    class IExchangeAPIClient {
        +fetch_historical_data(coin, start, end) List<Dict>
    }

    IHistoricalDataManager <|-- HistoricalDataManagerImpl

    HistoricalDataManagerImpl "1" -- "1" HistoricalFetcher : associates with
    HistoricalDataManagerImpl "1" -- "1" StreamingHandler : associates with
    HistoricalDataManagerImpl "1" -- "1" CurrentDataFetcher : associates with

    HistoricalDataManagerImpl ..> IStorageManager : depends on
    HistoricalFetcher ..> IStorageManager : depends on
    CurrentDataFetcher ..> IStorageManager : depends on
    HistoricalDataManagerImpl ..> IDataSourceConnector : depends on
    HistoricalDataManagerImpl ..> IExchangeAPIClient : depends on
```

### Storage Manager Module

This module abstracts the interaction with the centralized raw data storage.

```mermaid
classDiagram
    class IStorageManager {
        +store_raw_data(coin, data)
        +get_raw_data_range(coin, start, end, limit, offset)
        +get_latest_raw_entry(coin)
    }
    class StorageManagerImpl
    class IDatabaseConnection
    class IRawDataRepository {
        +save(entry)
        +find_by_range(coin, start, end, limit, offset)
        +find_latest(coin)
    }
    class RawDataRepositoryImpl
    class RawOrderBookEntry {
        coin_symbol
        timestamp
        data
    }

    IStorageManager <|-- StorageManagerImpl
    IDatabaseConnection <|-- DatabaseConnectionImpl
    IRawDataRepository <|-- RawDataRepositoryImpl

    StorageManagerImpl "1" -- "1" IDatabaseConnection : associates with
    StorageManagerImpl "1" -- "1" IRawDataRepository : associates with

    RawDataRepositoryImpl ..> IDatabaseConnection : depends on
    IRawDataRepository "1" -- "*" RawOrderBookEntry : manages
```

### Authentication Module

This module handles OAuth authentication for securing the Data Service's endpoints.

```mermaid
classDiagram
    class IAuthenticationModule {
        +validate_token(token)
    }
    class AuthenticationModuleImpl
    class OAuth2PasswordBearer
    class TokenValidator {
        +validate_token(token)
    }
    class get_current_user {
        # FastAPI Dependency
    }

    IAuthenticationModule <|-- AuthenticationModuleImpl
    AuthenticationModuleImpl "1" -- "1" TokenValidator : associates with
    get_current_user ..> IAuthenticationModule : depends on
    get_current_user ..> OAuth2PasswordBearer : depends on
```

## Data Collection Workflow (Sequence Diagram)

```mermaid
sequenceDiagram
    Client->>API Endpoints: MCP Request: Add Coin (CoinX, ExchangeA)
    API Endpoints->>Authentication Module: Validate Token
    Authentication Module-->>API Endpoints: Validation Result
    API Endpoints->>Data Collection Manager: add_coin(CoinX, ExchangeA)
    Data Collection Manager->>Data Source Connector: get_client(ExchangeA)
    Data Source Connector-->>Data Collection Manager: IExchangeAPIClient (for ExchangeA)
    Data Collection Manager->>IExchangeAPIClient: check_coin_availability(CoinX)
    IExchangeAPIClient-->>Data Collection Manager: Availability Result
    alt If Available
        Data Collection Manager->>IExchangeAPIClient: start_realtime_stream(CoinX, callback)
        IExchangeAPIClient->>External Exchange API: Subscribe to Real-Time Data (CoinX)
        External Exchange API-->>IExchangeAPIClient: Real-Time Data Entry (CoinX)
        IExchangeAPIClient->>Data Collection Manager: callback(CoinX, data_entry)
        Data Collection Manager->>Storage Manager: store_raw_data(CoinX, data_entry)
        Storage Manager-->>Data Collection Manager: Storage Result
        alt If WebSocket Subscribers Exist
            Data Collection Manager->>API Endpoints: Push data_entry to WebSocket
            API Endpoints-->>Client: WebSocket Message: Real-Time Data (CoinX, data_entry)
        end
        Data Collection Manager-->>API Endpoints: add_coin Result (Success)
        API Endpoints-->>Client: MCP Response: Add Coin (Success)
    else If Not Available
        Data Collection Manager-->>API Endpoints: add_coin Result (Failure)
        API Endpoints-->>Client: MCP Response: Add Coin (Failure)
    end
```

## Historical Data Workflow (Sequence Diagram)

```mermaid
sequenceDiagram
    Client->>API Endpoints: WebSocket Message: request_historical (CoinY, ExchangeB, StartDate, EndDate)
    API Endpoints->>Authentication Module: Validate WebSocket Connection/Token
    Authentication Module-->>API Endpoints: Validation Result
    API Endpoints->>Data Collection Manager: add_coin(CoinY, ExchangeB)
    Data Collection Manager-->>API Endpoints: add_coin Result (Ensures data is collected)
    API Endpoints->>Historical Data Manager: stream_historical_data(CoinY, ExchangeB, StartDate, EndDate, websocket)
    Historical Data Manager->>Storage Manager: get_raw_data_range(CoinY, StartTime, EndTime, Chunk)
    Storage Manager-->>Historical Data Manager: Historical Data Chunk
    alt While Data Chunks Exist in Storage
        Historical Data Manager->>API Endpoints: Send data_chunk over WebSocket
        API Endpoints-->>Client: WebSocket Message: historical_data_chunk (CoinY, data_chunk)
        Historical Data Manager->>Storage Manager: get_raw_data_range(Next Chunk)
        Storage Manager-->>Historical Data Manager: Next Historical Data Chunk
    end
    alt If More Data Needed from Source (Optional Fallback)
        Historical Data Manager->>Data Source Connector: get_client(ExchangeB)
        Data Source Connector-->>Historical Data Manager: IExchangeAPIClient (for ExchangeB)
        Historical Data Manager->>IExchangeAPIClient: fetch_historical_data(CoinY, RemainingRange)
        IExchangeAPIClient-->>Historical Data Manager: Historical Data from Exchange
        %% Process and potentially store this data before streaming
        Historical Data Manager->>API Endpoints: Send fetched data over WebSocket
        API Endpoints-->>Client: WebSocket Message: historical_data_chunk (CoinY, data_chunk)
    end
    Historical Data Manager->>API Endpoints: Send Completion Message over WebSocket
    API Endpoints-->>Client: WebSocket Message: historical_data_complete (CoinY)
```

---

This detailed breakdown provides a concrete plan for implementing the Data Service using UML class diagrams and descriptions of relationships, emphasizing the use of interfaces for testability and maintainability and incorporating the Microkernel pattern for exchange connectors.
