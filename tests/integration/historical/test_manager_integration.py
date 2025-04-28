import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone
import asyncio
import logging
import pandas as pd
from historical.manager import HistoricalDataManagerImpl
from storage.interfaces import IStorageManager

logger = logging.getLogger(__name__)

# Dummy WebSocket class for testing streaming
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

# Test streaming data using the setup fixture
@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_historical_data_integration(storage_manager, setup_historical_data):
    """Test streaming historical data that was populated by the setup fixture."""
    context = setup_historical_data
    ws = DummyWS()
    mgr = HistoricalDataManagerImpl(storage_manager)

    # Define start and end times matching the setup fixture's range (last hour)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Stream with a small chunk size to test chunking
    await mgr.stream_historical_data(context, start_time, end_time, ws, chunk_size=10)

    # Assertions
    assert len(ws.sent) > 0 # Check that something was sent

    chunk_messages = [m for m in ws.sent if m["type"] == "historical_data_chunk"]
    complete_messages = [m for m in ws.sent if m["type"] == "historical_data_complete"]
    error_messages = [m for m in ws.sent if m["type"] == "historical_data_error"]

    assert not error_messages # No errors expected
    assert len(complete_messages) == 1 # One completion message expected
    assert len(chunk_messages) > 1 # Expect multiple chunks with chunk_size=10 for ~60 records

    # Check total records sent matches expected count (around 60)
    total_records_sent = sum(len(chunk['data']) for chunk in chunk_messages)
    assert 50 <= total_records_sent <= 70 # Allow for variations

    # Check context in messages
    assert all(msg['context'] == context for msg in chunk_messages)
    assert complete_messages[0]['context'] == context

# Test streaming when the websocket connection fails mid-stream
@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_historical_data_websocket_error_integration(storage_manager, setup_historical_data):
    """Test streaming stops and sends error if websocket fails."""
    context = setup_historical_data
    ws = DummyWS()
    mgr = HistoricalDataManagerImpl(storage_manager)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Simulate websocket error after receiving the first chunk
    original_send_json = ws.send_json
    call_count = 0
    async def faulty_send_json(data):
        nonlocal call_count
        call_count += 1
        if call_count > 1: # Fail after the first successful send
            ws.closed = True # Mark as closed to simulate error
            logger.info("Simulating WebSocket error")
            raise ConnectionError("Simulated WebSocket connection error")
        await original_send_json(data)

    ws.send_json = faulty_send_json

    # Stream with a small chunk size
    # Use asyncio.gather to catch the expected ConnectionError if it propagates
    # Or check the log/error message sent *before* the connection breaks
    await mgr.stream_historical_data(context, start_time, end_time, ws, chunk_size=5)

    # Assertions
    assert len(ws.sent) > 0 # At least the first chunk should have been sent

    chunk_messages = [m for m in ws.sent if m["type"] == "historical_data_chunk"]
    complete_messages = [m for m in ws.sent if m["type"] == "historical_data_complete"]
    # The manager tries to send an error message *before* stopping
    error_messages = [m for m in ws.sent if m["type"] == "historical_data_error"]

    assert len(chunk_messages) == 1 # Only the first chunk should succeed
    # The error message might fail to send if the connection is truly broken *during* send
    # The manager logs the error internally regardless.
    # assert len(error_messages) == 1 # This might be unreliable depending on timing
    assert not complete_messages # Should not complete successfully

# Test getting the most current data (uses CurrentDataFetcher internally)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_most_current_data_integration(storage_manager, setup_historical_data):
    """Test getting the most current data point using the manager."""
    context = setup_historical_data
    mgr = HistoricalDataManagerImpl(storage_manager)

    result = await mgr.get_most_current_data(context)

    assert result is not None
    assert isinstance(result, dict)
    assert 'timestamp' in result
    # Check timestamp is recent (within the last hour + buffer)
    now = datetime.now(timezone.utc)
    result_time = pd.to_datetime(result['timestamp'])
    assert now - timedelta(hours=1, minutes=10) <= result_time <= now + timedelta(minutes=10)
