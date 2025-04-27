import sys
import pytest
import datetime
from exchange_source.plugins.ccxt_exchange import CCXTExchangeClient
from exchange_source.config import CCXTConfig  # <-- Update import

class TestCCXTExchangeClient:
    @pytest.fixture
    async def client(self):
        # Provide a default CCXTConfig and exchange_id as required by the new constructor
        config = CCXTConfig()  # Use the imported config
        client = CCXTExchangeClient(config, 'cryptocom')
        yield client
        await client.close()

    @pytest.mark.asyncio
    async def test_get_exchange_name(self, client):
        assert client.get_exchange_name() == 'cryptocom'

    @pytest.mark.asyncio
    async def test_check_coin_availability(self, client):
        assert await client.check_coin_availability('BTC/USD') is True
        assert await client.check_coin_availability('INVALID/SYMBOL') is False

    @pytest.mark.asyncio
    async def test_fetch_historical_data_short_period(self, client):
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(hours=1)
        interval = '5m'
        result = await client.fetch_historical_data('BTC/USD', start_time, end_time, interval=interval)
        assert isinstance(result, list)
        if result:
            first = result[0]
            assert 'timestamp' in first
            assert 'open' in first
            assert 'high' in first
            assert 'low' in first
            assert 'close' in first
            assert 'volume' in first
            assert first['exchange'] == 'cryptocom'
            interval_ms = 5 * 60 * 1000
            assert first['timestamp'] >= int(start_time.timestamp() * 1000) - interval_ms
            assert result[-1]['timestamp'] <= int(end_time.timestamp() * 1000)

    @pytest.mark.asyncio
    async def test_fetch_historical_data_invalid_symbol(self, client):
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(hours=1)
        result = await client.fetch_historical_data('INVALID/SYMBOL', start_time, end_time)
        assert result == []
