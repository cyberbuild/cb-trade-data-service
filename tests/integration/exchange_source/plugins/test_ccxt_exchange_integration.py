# tests/integration/exchange_source/plugins/test_ccxt_exchange_integration.py
import pytest
import datetime
import logging
from exchange_source.plugins.ccxt_exchange import CCXTExchangeClient
from config import get_settings

logger = logging.getLogger(__name__)

# Use yield to allow for cleanup after tests in the module run
@pytest.fixture(scope="function")  # Use function scope for async client
async def client(): # Make the fixture async
    settings = get_settings()
    # Ensure you have cryptocom credentials in your .env or environment
    _client = CCXTExchangeClient(exchange_id='cryptocom', config=settings.ccxt)
    try:
        yield _client # Provide the client to the tests
    finally:
        # Cleanup: Close the client connection after tests are done
        logger.info("Closing CCXT client connection...")
        await _client.aclose() # Use aclose() for async cleanup
        logger.info("CCXT client connection closed.")

@pytest.mark.integration
@pytest.mark.asyncio # Mark test as async
async def test_integration_check_coin_availability_success(client):
    logger.info("Running test_integration_check_coin_availability_success...")
    result = await client.check_coin_availability("BTC/USD") # Await the coroutine
    logger.info(f"Result for BTC/USD availability: {result}")
    assert result is True

@pytest.mark.integration
@pytest.mark.asyncio # Mark test as async
async def test_integration_check_coin_availability_fail(client):
    logger.info("Running test_integration_check_coin_availability_fail...")
    result = await client.check_coin_availability("INVALID/SYMBOL") # Await the coroutine
    logger.info(f"Result for INVALID/SYMBOL availability: {result}")
    assert result is False

@pytest.mark.integration
@pytest.mark.asyncio # Mark test as async
async def test_integration_fetch_historical_data_short_period(client):
    logger.info("Running test_integration_fetch_historical_data_short_period...")
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=1)
    interval = "5m"
    logger.info(f"Fetching historical data for BTC/USD from {start_time} to {end_time} with interval {interval}")
    result = await client.fetch_historical_data("BTC/USD", start_time, end_time, interval=interval) # Await the coroutine
    logger.info(f"Fetched {len(result)} candles.")
    assert isinstance(result, list)
    # Check if data looks reasonable (e.g., timestamps are within range)
    if result:
        assert isinstance(result[0], dict)
        assert 'timestamp' in result[0]
        # Convert timestamp ms to datetime object for comparison
        first_timestamp_dt = datetime.datetime.fromtimestamp(result[0]['timestamp'] / 1000, tz=datetime.timezone.utc)
        last_timestamp_dt = datetime.datetime.fromtimestamp(result[-1]['timestamp'] / 1000, tz=datetime.timezone.utc)
        logger.info(f"First candle timestamp: {first_timestamp_dt}, Last candle timestamp: {last_timestamp_dt}")
        # Allow for some buffer as exchange might return slightly outside the exact start/end
        assert start_time - datetime.timedelta(minutes=10) <= first_timestamp_dt <= end_time + datetime.timedelta(minutes=10)
        assert start_time - datetime.timedelta(minutes=10) <= last_timestamp_dt <= end_time + datetime.timedelta(minutes=10)
    else:
        logger.warning("No candles fetched, skipping detailed checks.")

@pytest.mark.integration
@pytest.mark.asyncio # Mark test as async
async def test_integration_fetch_historical_data_invalid_symbol(client):
    logger.info("Running test_integration_fetch_historical_data_invalid_symbol...")
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=1)
    logger.info(f"Fetching historical data for INVALID/SYMBOL from {start_time} to {end_time}")
    result = await client.fetch_historical_data("INVALID/SYMBOL", start_time, end_time) # Await the coroutine
    logger.info(f"Result: {result}")
    assert result == []

@pytest.mark.integration
@pytest.mark.asyncio # Mark test as async
async def test_integration_fetch_two_weeks_five_minute_data(client):
    """Test fetching two weeks of 5m interval data from Crypto.com (ccxt ID 'cryptocom')."""
    import time
    end_dt = datetime.datetime.now(datetime.timezone.utc)
    start_dt = end_dt - datetime.timedelta(weeks=2)
    interval = "5m"
    start_time_perf = time.time()
    logger.info(f"Fetching two weeks of 5m data for BTC/USD from {start_dt} to {end_dt}")
    result = await client.fetch_historical_data("BTC/USD", start_dt, end_dt, interval=interval) # Await the coroutine
    elapsed = time.time() - start_time_perf
    row_count = len(result)
    logger.info(f"Fetched {row_count} candles in {elapsed:.2f} seconds.")
    assert isinstance(result, list)
    assert row_count > 0 # Should fetch a significant number of candles
    # Expected candles: 2 weeks * 7 days/week * 24 hours/day * 12 candles/hour (for 5m interval)
    expected_min_candles = 2 * 7 * 24 * 12 * 0.9 # Allow for some downtime/missing data (10% buffer)
    expected_max_candles = 2 * 7 * 24 * 12 * 1.1 # Allow for slight overfetch
    logger.info(f"Expected candle count between {expected_min_candles:.0f} and {expected_max_candles:.0f}")
    assert expected_min_candles <= row_count <= expected_max_candles
    # Check first and last timestamps
    first_timestamp_dt = datetime.datetime.fromtimestamp(result[0]['timestamp'] / 1000, tz=datetime.timezone.utc)
    last_timestamp_dt = datetime.datetime.fromtimestamp(result[-1]['timestamp'] / 1000, tz=datetime.timezone.utc)
    logger.info(f"First candle: {first_timestamp_dt}, Last candle: {last_timestamp_dt}")
    # Check if timestamps are roughly within the requested range (allow buffer)
    assert start_dt - datetime.timedelta(minutes=10) <= first_timestamp_dt <= start_dt + datetime.timedelta(minutes=10)
    assert end_dt - datetime.timedelta(minutes=10) <= last_timestamp_dt <= end_dt + datetime.timedelta(minutes=10)
