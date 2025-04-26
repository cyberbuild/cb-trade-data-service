import ccxt
import logging
import time
from typing import Any, Dict, List, Optional, Callable, Union
import datetime

logger = logging.getLogger(__name__)

class CCXTExchangeClient:
    def __init__(self, exchange_id: str, api_key: Optional[str] = None, secret: Optional[str] = None, **kwargs):
        exchange_class = getattr(ccxt, exchange_id)
        params = {'enableRateLimit': True}
        if api_key:
            params['apiKey'] = api_key
        if secret:
            params['secret'] = secret
        params.update(kwargs)
        self._exchange = exchange_class(params)
        self.exchange_id = exchange_id
        self._ws_connections = {}

    def get_exchange_name(self) -> str:
        return self.exchange_id

    def check_coin_availability(self, coin_symbol: str) -> bool:
        try:
            self._exchange.load_markets()
            return coin_symbol in self._exchange.markets
        except Exception as e:
            logger.error(f"Error checking coin availability for {coin_symbol}: {e}")
            return False

    def fetch_historical_data(
        self,
        coin_symbol: str,
        start_time: Union[int, datetime.datetime],
        end_time: Union[int, datetime.datetime],
        interval: str = '5m',
        max_limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        try:
            if isinstance(start_time, datetime.datetime):
                start_time = int(start_time.timestamp() * 1000)
            if isinstance(end_time, datetime.datetime):
                end_time = int(end_time.timestamp() * 1000)
            if start_time < 1000000000000:
                start_time *= 1000
            if end_time < 1000000000000:
                end_time *= 1000
            self._exchange.load_markets()
            if coin_symbol not in self._exchange.markets:
                logger.error(f"Symbol {coin_symbol} not available on {self.exchange_id}")
                return []
            all_candles = []
            since = start_time
            interval_duration_ms = self._parse_interval_ms(interval)
            # Use exchange's default limit if not provided
            limit = max_limit or getattr(self._exchange, 'maxOHLCVLimit', 1000)
            prev_since = None
            while since < end_time:
                logger.info(f"Fetching data for {coin_symbol} from {since} to {end_time}")
                candles = self._exchange.fetch_ohlcv(
                    symbol=coin_symbol,
                    timeframe=interval,
                    since=since,
                    limit=limit
                )
                if not candles:
                    break
                filtered_candles = [candle for candle in candles if start_time <= candle[0] <= end_time]
                all_candles.extend(filtered_candles)
                last_timestamp = candles[-1][0]
                if prev_since == last_timestamp or last_timestamp >= end_time:
                    break
                prev_since = last_timestamp
                since = last_timestamp + interval_duration_ms
                time.sleep(self._exchange.rateLimit / 1000)
            return self._standardize_ohlcv_data(all_candles)
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return []

    def _parse_interval_ms(self, interval: str) -> int:
        # Use exchange's timeframes if available, else fallback
        if hasattr(self._exchange, 'timeframes') and interval in self._exchange.timeframes:
            # Try to parse interval string like '5m', '1h', etc.
            unit = interval[-1]
            value = int(interval[:-1])
            if unit == 'm':
                return value * 60 * 1000
            elif unit == 'h':
                return value * 60 * 60 * 1000
            elif unit == 'd':
                return value * 24 * 60 * 60 * 1000
            elif unit == 'w':
                return value * 7 * 24 * 60 * 60 * 1000
        # Fallback to 5m
        return 5 * 60 * 1000

    def _standardize_ohlcv_data(self, candles: List) -> List[Dict[str, Any]]:
        standardized_candles = []
        for candle in candles:
            timestamp, open_price, high, low, close, volume = candle
            standardized_candle = {
                "timestamp": timestamp,
                "open": float(open_price),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume),
                "exchange": self.get_exchange_name()
            }
            standardized_candles.append(standardized_candle)
        return standardized_candles
