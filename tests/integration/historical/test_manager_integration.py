import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone
import asyncio
import logging
import pandas as pd
from historical.manager import HistoricalDataManagerImpl
from storage.storage_manager import IStorageManager
from storage.paging import Paging
from exchange_source.models import Metadata

logger = logging.getLogger(__name__)


# Dummy WebSocket class for testing streaming (if needed for future streaming functionality)
class DummyWS:
    def __init__(self):
        self.sent = []
        self.closed = False

    async def send_json(self, data):
        if self.closed:
            raise ConnectionError("WebSocket is closed")
        self.sent.append(data)
        logger.debug(f"DummyWS received: {data}")

    async def close(self):
        self.closed = True


# Test getting the most current data (uses HistoricalFetcher internally)
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_get_most_current_data_integration(
    storage_manager, setup_historical_data
):
    """Test getting the most current data point using the manager."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    result = await mgr.get_most_current_data(metadata)

    assert result is not None
    assert isinstance(result, dict)
    assert "timestamp" in result
    # Check timestamp is recent (within the last hour + buffer)
    now = datetime.now(timezone.utc)
    result_time = pd.to_datetime(result["timestamp"], unit="ms", utc=True)
    assert (
        now - timedelta(hours=1, minutes=10)
        <= result_time
        <= now + timedelta(minutes=10)
    )


# Test paging functionality
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_get_historical_data_with_paging_limit(
    storage_manager, setup_historical_data
):
    """Test getting historical data with limit pagination."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Test with limit
    paging = Paging.create(limit=10)
    result = await mgr.get_historical_data(metadata, start_time, end_time, paging)

    assert result is not None
    assert len(result.data) <= 10
    assert len(result.data) > 0


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_get_historical_data_with_paging_offset(
    storage_manager, setup_historical_data
):
    """Test getting historical data with offset pagination."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Get first page
    paging_first = Paging.create(limit=5, offset=0)
    result_first = await mgr.get_historical_data(
        metadata, start_time, end_time, paging_first
    )

    # Get second page
    paging_second = Paging.create(limit=5, offset=5)
    result_second = await mgr.get_historical_data(
        metadata, start_time, end_time, paging_second
    )

    assert result_first is not None
    assert result_second is not None
    assert len(result_first.data) <= 5
    assert len(result_second.data) <= 5

    # Ensure different data (no overlap)
    if len(result_first.data) > 0 and len(result_second.data) > 0:
        first_timestamps = {row["timestamp"] for row in result_first.data}
        second_timestamps = {row["timestamp"] for row in result_second.data}
        assert first_timestamps.isdisjoint(second_timestamps)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_get_historical_data_all_records(storage_manager, setup_historical_data):
    """Test getting all historical data without pagination."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Test with all_records paging
    paging = Paging.all_records()
    result = await mgr.get_historical_data(metadata, start_time, end_time, paging)

    assert result is not None
    assert len(result.data) > 0
    # Should get all available records (around 60)
    assert 50 <= len(result.data) <= 70


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_get_historical_data_no_paging_parameter(
    storage_manager, setup_historical_data
):
    """Test getting historical data without specifying paging parameter (should default to all records)."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Test without paging parameter
    result = await mgr.get_historical_data(metadata, start_time, end_time)

    assert result is not None
    assert len(result.data) > 0
    # Should get all available records (around 60)
    assert 50 <= len(result.data) <= 70


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_get_historical_data_large_offset(storage_manager, setup_historical_data):
    """Test getting historical data with offset larger than available data."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Test with large offset
    paging = Paging.create(limit=10, offset=1000)
    result = await mgr.get_historical_data(metadata, start_time, end_time, paging)

    assert result is not None
    assert len(result.data) == 0  # Should return empty list


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_manager,setup_historical_data",
    [("local", "local"), ("azure", "azure")],
    indirect=True,
)
async def test_paging_consistency_across_calls(storage_manager, setup_historical_data):
    """Test that paging returns consistent results across multiple calls."""
    context = setup_historical_data
    metadata = Metadata(context)
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    paging = Paging.create(limit=10, offset=0)

    # Make multiple calls with same paging
    result1 = await mgr.get_historical_data(metadata, start_time, end_time, paging)
    result2 = await mgr.get_historical_data(metadata, start_time, end_time, paging)

    assert result1 is not None
    assert result2 is not None
    assert len(result1.data) == len(result2.data)

    # Results should be identical
    if len(result1.data) > 0:
        timestamps1 = [row["timestamp"] for row in result1.data]
        timestamps2 = [row["timestamp"] for row in result2.data]
        assert timestamps1 == timestamps2
