import pytest
import datetime
from src.exchange_source.plugins.ccxt_exchange import CCXTExchangeClient

class TestCCXTExchangeClient:
    @pytest.fixture
    def client(self):
        # Use Gemini (public, no API key required)
        return CCXTExchangeClient('gemini')

    def test_get_exchange_name(self, client):
        assert client.get_exchange_name() == 'gemini'

    def test_check_coin_availability(self, client):
        assert client.check_coin_availability('BTC/USD') is True
        assert client.check_coin_availability('INVALID/SYMBOL') is False

    def test_fetch_historical_data_short_period(self, client):
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(hours=1)
        interval = '5m'
        result = client.fetch_historical_data('BTC/USD', start_time, end_time, interval=interval)
        assert isinstance(result, list)
        if result:
            first = result[0]
            assert 'timestamp' in first
            assert 'open' in first
            assert 'high' in first
            assert 'low' in first
            assert 'close' in first
            assert 'volume' in first
            assert first['exchange'] == 'gemini'
            interval_ms = 5 * 60 * 1000
            assert first['timestamp'] >= int(start_time.timestamp() * 1000) - interval_ms
            assert result[-1]['timestamp'] <= int(end_time.timestamp() * 1000)

    def test_fetch_historical_data_invalid_symbol(self, client):
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(hours=1)
        result = client.fetch_historical_data('INVALID/SYMBOL', start_time, end_time)
        assert result == []
