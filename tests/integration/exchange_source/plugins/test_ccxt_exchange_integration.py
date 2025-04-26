# tests/integration/exchange_source/plugins/test_kraken_exchange_integration.py
import pytest
import datetime
from exchange_source.plugins.ccxt_exchange import CCXTExchangeClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ccxt_integration_test")

@pytest.fixture(scope="module")
def client():
    # Use Crypto.com (public, no API key required)
    return CCXTExchangeClient('cryptocom')

@pytest.mark.integration
def test_integration_check_coin_availability_success(client):
    logger.info("Running test_integration_check_coin_availability_success...")
    result = client.check_coin_availability("BTC/USD")
    logger.info(f"Result for BTC/USD availability: {result}")
    assert result is True

@pytest.mark.integration
def test_integration_check_coin_availability_fail(client):
    logger.info("Running test_integration_check_coin_availability_fail...")
    result = client.check_coin_availability("INVALID/SYMBOL")
    logger.info(f"Result for INVALID/SYMBOL availability: {result}")
    assert result is False

@pytest.mark.integration
def test_integration_fetch_historical_data_short_period(client):
    logger.info("Running test_integration_fetch_historical_data_short_period...")
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=1)
    interval = "5m"
    logger.info(f"Fetching historical data for BTC/USD from {start_time} to {end_time} with interval {interval}")
    result = client.fetch_historical_data("BTC/USD", start_time, end_time, interval=interval)
    logger.info(f"Fetched {len(result)} candles.")
    assert isinstance(result, list)
    if result:
        logger.info(f"First candle: {result[0]}")
        assert "timestamp" in result[0]
        assert "open" in result[0]
        assert "high" in result[0]
        assert "low" in result[0]
        assert "close" in result[0]
        assert "volume" in result[0]
        assert result[0]["exchange"] == "cryptocom"
        interval_ms = 5 * 60 * 1000
        assert result[0]["timestamp"] >= int(start_time.timestamp() * 1000) - interval_ms
        assert result[-1]["timestamp"] <= int(end_time.timestamp() * 1000)

@pytest.mark.integration
def test_integration_fetch_historical_data_invalid_symbol(client):
    logger.info("Running test_integration_fetch_historical_data_invalid_symbol...")
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(hours=1)
    logger.info(f"Fetching historical data for INVALID/SYMBOL from {start_time} to {end_time}")
    result = client.fetch_historical_data("INVALID/SYMBOL", start_time, end_time)
    logger.info(f"Result: {result}")
    assert result == []

@pytest.mark.integration
def test_integration_fetch_two_weeks_five_minute_data(client):
    """Test fetching two weeks of 5m interval data from Crypto.com (ccxt ID 'cryptocom')."""
    import time
    end_dt = datetime.datetime.now(datetime.timezone.utc)
    start_dt = end_dt - datetime.timedelta(weeks=2)
    interval = "5m"
    start_time = time.time()
    logger.info(f"Fetching two weeks of 5m data for BTC/USD from {start_dt} to {end_dt}")
    result = client.fetch_historical_data("BTC/USD", start_dt, end_dt, interval=interval)
    elapsed = time.time() - start_time
    row_count = len(result)
    logger.info(f"Fetched {row_count} rows in {elapsed:.2f} seconds.")
    print(f"Fetched {row_count} rows in {elapsed:.2f} seconds.")
    if result:
        first_ts = result[0]["timestamp"]
        last_ts = result[-1]["timestamp"]
        print(f"First row timestamp: {first_ts}")
        print(f"Last row timestamp: {last_ts}")
        print(f"Requested start: {int(start_dt.timestamp() * 1000)}")
        print(f"Requested end: {int(end_dt.timestamp() * 1000)}")
    assert elapsed < 60, f"Fetching took too long: {elapsed:.2f} seconds"
    assert isinstance(result, list)
    assert row_count > 0
    expected_rows = 14 * 24 * 12  # 4032
    assert row_count == expected_rows, f"Expected {expected_rows} rows, got {row_count}"
