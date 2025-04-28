# Historical Data Package (`src/historical`)

This package is responsible for retrieving historical market data from the configured storage backend.

## Components

### Interfaces (`interfaces.py`)

*   **`IHistoricalDataManager`**: Defines the contract for managing historical data retrieval.
    *   `stream_historical_data(coin, exchange, start, end, ws)`: Streams historical data for a given coin and exchange within a time range to a websocket connection.
    *   `get_most_current_data(coin, exchange)`: Retrieves the most recent data entry for a given coin and exchange.

### Implementation (`manager.py`)

*   **`HistoricalDataManagerImpl`**: The concrete implementation of `IHistoricalDataManager`.
    *   It utilizes `HistoricalFetcher` and `CurrentDataFetcher` to interact with the storage layer.
    *   Provides both synchronous and asynchronous wrappers for its methods to support different execution contexts (e.g., sync tests, async application).
    *   Handles chunking of data when streaming to avoid overwhelming the websocket connection.

### Fetchers

*   **`HistoricalFetcher` (`fetcher.py`)**: Responsible for fetching historical data within a specific time range from the storage backend. It interacts with the `IStorageManager` interface.
*   **`CurrentDataFetcher` (`current_fetcher.py`)**: Responsible for fetching the single most recent data entry for a coin/exchange pair from the storage backend via the `IStorageManager` interface.

## Dependencies

*   **`storage` package**: Depends on the `IStorageManager` interface defined in `src/storage/interfaces.py`. An instance implementing this interface must be provided to `HistoricalDataManagerImpl` upon initialization.
*   **`asyncio`**: Used for handling asynchronous operations, particularly for streaming data and interacting with potentially async storage backends.
*   **Websockets**: The `stream_historical_data` method requires a websocket object (presumably from a library like `fastapi.websockets` or `websockets`) to send data back to the client.

## Usage

Typically, an instance of `HistoricalDataManagerImpl` would be created, injecting a configured `IStorageManager` instance. This manager instance can then be used by other parts of the application (e.g., API endpoints) to serve historical data requests.
