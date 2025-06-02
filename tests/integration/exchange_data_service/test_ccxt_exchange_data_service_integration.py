import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone
import logging
import os
from exchange_data_service.ccxt_exchange_data_service import CCXTExchangeDataService
from exchange_data_service.interface import Interval
from historical.manager import HistoricalDataManagerImpl
from storage.storage_manager import IStorageManager
from config import get_settings

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
def settings():
    return get_settings()


@pytest_asyncio.fixture
def ccxt_exchange_data_service(settings, storage_manager):
    historical_manager = HistoricalDataManagerImpl(storage_manager)
    return CCXTExchangeDataService(settings, historical_manager)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_get_data_integration(ccxt_exchange_data_service, setup_historical_data):
    """Test the get_data ABC method with real data from setup fixture."""
    context = setup_historical_data
    symbol = context['coin']  # BTC/USD from setup fixture
    interval_str = context['interval']  # 1m from setup fixture
    interval = Interval(interval_str)  # Convert to enum
      # Get data for the last 15 minutes (shortened for faster tests)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=15)
    
    logger.info(f"Testing get_data for {symbol} with interval {interval_str}")
    
    result = await ccxt_exchange_data_service.get_ohlcv_data(symbol, interval, start_time, end_time)
    
    assert result is not None
    assert hasattr(result, 'data')
    assert hasattr(result, 'metadata')
    logger.info(f"Retrieved {len(result.data) if result.data else 0} records")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data", 
    [("local", "local"), ("azure", "azure")], 
    indirect=True
)
async def test_get_data_with_enum_interval(ccxt_exchange_data_service, setup_historical_data):
    """Test the internal _get_data_with_enum method with Interval enum."""
    context = setup_historical_data
    symbol = context['coin']
    interval_enum = Interval.MINUTE
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=10)
    
    logger.info(f"Testing _get_data_with_enum for {symbol} with interval {interval_enum.value}")
    
    result = await ccxt_exchange_data_service.get_ohlcv_data(symbol, interval_enum, start_time, end_time)
    
    assert result is not None
    assert hasattr(result, 'data')
    assert hasattr(result, 'metadata')
    logger.info(f"Retrieved {len(result.data) if result.data else 0} records")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("storage_manager", ["local", "azure"], indirect=True)
async def test_sync_with_exchange_async_method(ccxt_exchange_data_service):
    """Test the internal async sync method - this is the core ABC functionality."""
    symbol = 'BTC/USDT'
    interval = Interval.HOUR
    
    logger.info(f"Testing sync_with_exchange for {symbol} with interval {interval.value}")
    
    # This should complete without error
    result = await ccxt_exchange_data_service.sync_with_exchange(symbol, interval)
    
    # Should return self (IExchangeDataService)
    assert result is ccxt_exchange_data_service
    
    logger.info("sync_with_exchange completed successfully")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("storage_manager", ["local", "azure"], indirect=True)
async def test_end_to_end_sync_and_retrieve(ccxt_exchange_data_service):
    """Test end-to-end: sync data then retrieve it using async methods."""
    symbol = 'BTC/USDT'
    interval = Interval.HOUR
    
    logger.info(f"Testing end-to-end sync and retrieve for {symbol}")
    
    # First sync data from exchange using async method
    result = await ccxt_exchange_data_service.sync_with_exchange(symbol, interval)
    
    # Should return self (IExchangeDataService)
    assert result is ccxt_exchange_data_service
      # Then retrieve the data (shortened time range for faster tests)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=30)
    
    result = await ccxt_exchange_data_service.get_ohlcv_data(symbol, interval, start_time, end_time)
    
    assert result is not None
    logger.info(f"End-to-end test completed. Retrieved {len(result.data) if result.data else 0} records")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("storage_manager", ["local", "azure"], indirect=True)
async def test_multiple_intervals(ccxt_exchange_data_service):
    """Test the service with different interval types using async methods."""
    symbol = 'BTC/USDT'    
    intervals = [Interval.MINUTE, Interval.FIVEMINUTES, Interval.HOUR]
    
    for interval in intervals:
        logger.info(f"Testing with interval: {interval.value}")
        
        # Test async sync
        await ccxt_exchange_data_service.sync_with_exchange(symbol, interval)
          # Test data retrieval (much shorter time range for faster tests)
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=10)  # Fixed 10 minutes instead of interval-based
        
        data_result = await ccxt_exchange_data_service.get_ohlcv_data(symbol, interval, start_time, end_time)
        assert data_result is not None
        
        logger.info(f"Interval {interval.value} test completed successfully")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("storage_manager", ["local", "azure"], indirect=True)
async def test_error_handling_invalid_symbol(ccxt_exchange_data_service):
    """Test error handling with invalid symbol."""
    invalid_symbol = 'INVALID/SYMBOL'
    interval = Interval.HOUR
    
    logger.info(f"Testing error handling with invalid symbol: {invalid_symbol}")
      # This should not crash but may return empty data or handle gracefully
    try:
        await ccxt_exchange_data_service.sync_with_exchange(invalid_symbol, interval)
        logger.info("Invalid symbol test completed (no exception raised)")
    except Exception as e:
        logger.info(f"Invalid symbol test completed with expected error: {e}")
        # Expected behavior - error should be handled gracefully


