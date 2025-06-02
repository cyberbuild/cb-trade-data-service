# tests/integration/exchange_source/plugins/test_ccxt_exchange_integration.py

import pytest
import datetime
import logging
import os
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from config import get_settings, Settings
from exchange_source.models import ExchangeData, OHLCVRecord

logger = logging.getLogger(__name__)

# Use yield to allow for cleanup after tests in the module run
@pytest.fixture(scope="function")
async def client(monkeypatch):
    # Patch the environment to ensure Azure storage is used
    monkeypatch.setenv("STORAGE_TYPE", "azure")
    settings = get_settings()
    _client = CCXTExchangeClient(config=settings.ccxt, exchange_id=settings.exchange_id)
    try:
        yield _client
    finally:
        logger.info("Closing CCXT client connection...")
        await _client.close()
        logger.info("CCXT client connection closed.")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_check_coin_availability(client):
    available = await client.check_coin_availability("BTC/USD")
    assert isinstance(available, bool)
    assert available is True
    not_available = await client.check_coin_availability("INVALID/SYMBOL")
    assert not_available is False

@pytest.mark.integration
@pytest.mark.asyncio # Mark test as async
async def test_integration_check_coin_availability_fail(client):
    logger.info("Running test_integration_check_coin_availability_fail...")
    result = await client.check_coin_availability("INVALID/SYMBOL") # Await the coroutine
    logger.info(f"Result for INVALID/SYMBOL availability: {result}")
    assert result is False

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_ohlcv_data_basic(client):
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(minutes=30)
    interval = "5m"
    result = await client.fetch_ohlcv_data("BTC/USD", start_time, end_time, interval=interval)
    assert isinstance(result, ExchangeData)
    data = result.data
    assert isinstance(data, list)
    assert all(isinstance(rec, OHLCVRecord) for rec in data)
    meta = result.metadata
    assert meta.data_type == "ohlcv"
    assert meta.exchange == client.get_exchange_name()
    assert meta.coin_symbol == "BTC/USD"
    assert meta.interval == interval
    assert len(data) > 0
    first = data[0]
    assert isinstance(first.timestamp, int)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_ohlcv_data_invalid_symbol(client):
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(minutes=30)
    with pytest.raises(ValueError):
        await client.fetch_ohlcv_data("INVALID/SYMBOL", start_time, end_time)

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
    result = await client.fetch_ohlcv_data("BTC/USD", start_dt, end_dt, interval=interval)
    elapsed = time.time() - start_time_perf
    assert hasattr(result, 'data')
    data = result.data
    row_count = len(data)
    logger.info(f"Fetched {row_count} candles in {elapsed:.2f} seconds.")
    assert isinstance(result, ExchangeData)
    assert row_count > 0
    # Expected candles: 2 weeks * 7 days/week * 24 hours/day * 12 candles/hour (for 5m interval)
    expected_min_candles = 2 * 7 * 24 * 12 * 0.9 # Allow for some downtime/missing data (10% buffer)
    expected_max_candles = 2 * 7 * 24 * 12 * 1.1 # Allow for slight overfetch
    logger.info(f"Expected candle count between {expected_min_candles:.0f} and {expected_max_candles:.0f}")
    assert expected_min_candles <= row_count <= expected_max_candles
    # Check first and last timestamps
    first_timestamp_dt = datetime.datetime.fromtimestamp(data[0].timestamp / 1000, tz=datetime.timezone.utc)
    last_timestamp_dt = datetime.datetime.fromtimestamp(data[-1].timestamp / 1000, tz=datetime.timezone.utc)
    logger.info(f"First candle: {first_timestamp_dt}, Last candle: {last_timestamp_dt}")
    # Check if timestamps are roughly within the requested range (allow buffer)
    assert start_dt - datetime.timedelta(minutes=10) <= first_timestamp_dt <= start_dt + datetime.timedelta(minutes=10)
    assert end_dt - datetime.timedelta(minutes=10) <= last_timestamp_dt <= end_dt + datetime.timedelta(minutes=10)
