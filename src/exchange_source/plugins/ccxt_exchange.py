import ccxt.async_support as ccxt  # Use async version
import logging
import time
from typing import Any, Dict, List, Optional, Callable, Union
import datetime
import asyncio  # Import asyncio

# Use absolute import from package root
from config import CCXTConfig

logger = logging.getLogger(__name__)


class CCXTExchangeClient:
    # Accept CCXTConfig
    def __init__(self, config: CCXTConfig, exchange_id: str, api_key: Optional[str] = None, secret: Optional[str] = None, **kwargs):
        self.config = config  # Store config
        self.exchange_id = exchange_id
        exchange_class = getattr(ccxt, self.exchange_id)
        params = {'enableRateLimit': True}
        # Potentially load API keys from config if not provided directly
        # Example: api_key = api_key or getattr(config, f'{exchange_id}_api_key', None)
        # secret = secret or getattr(config, f'{exchange_id}_secret', None)
        if api_key:
            params['apiKey'] = api_key
        if secret:
            params['secret'] = secret
        params.update(kwargs)
        # Instantiate async exchange
        self._exchange = exchange_class(params)
        # self._ws_connections = {} # WebSocket handling needs separate async implementation

    async def close(self):  # Add async close method
        """Closes the underlying ccxt exchange connection."""
        try:
            await self._exchange.close()
            logger.info(f"Closed connection for exchange: {self.exchange_id}")
        except Exception as e:
            logger.error(f"Error closing connection for exchange {self.exchange_id}: {e}")

    async def aclose(self):
        """Alias for async close, for compatibility with async test fixtures."""
        await self.close()

    def get_exchange_name(self) -> str:
        return self.exchange_id

    # Make methods async
    async def check_coin_availability(self, coin_symbol: str) -> bool:
        try:
            await self._exchange.load_markets()
            return coin_symbol in self._exchange.markets
        except ccxt.NetworkError as e:
            logger.error(f"Network error checking coin availability for {coin_symbol} on {self.exchange_id}: {e}")
            return False
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error checking coin availability for {coin_symbol} on {self.exchange_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking coin availability for {coin_symbol} on {self.exchange_id}: {e}")
            return False

    async def fetch_historical_data(
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
            # Ensure milliseconds
            if start_time < 1000000000000:
                start_time *= 1000
            if end_time < 1000000000000:
                end_time *= 1000

            await self._exchange.load_markets()
            if coin_symbol not in self._exchange.markets:
                logger.error(f"Symbol {coin_symbol} not available on {self.exchange_id}")
                return []

            all_candles = []
            since = start_time
            interval_duration_ms = self._parse_interval_ms(interval)
            limit = max_limit or getattr(self._exchange, 'maxOHLCVLimit', 1000)
            prev_since = None

            while since < end_time:
                logger.info(f"Fetching data for {coin_symbol} from {datetime.datetime.fromtimestamp(since/1000, tz=datetime.timezone.utc)} (ts: {since}) on {self.exchange_id}")
                try:
                    # Use await for async fetch_ohlcv
                    candles = await self._exchange.fetch_ohlcv(
                        symbol=coin_symbol,
                        timeframe=interval,
                        since=since,
                        limit=limit
                    )
                except ccxt.RateLimitExceeded as e:
                    logger.warning(f"Rate limit exceeded on {self.exchange_id}. Retrying after delay... Error: {e}")
                    await asyncio.sleep(self._exchange.rateLimit / 1000 * 1.1)  # Add slight buffer
                    continue  # Retry the same fetch
                except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as e:
                    logger.error(f"Network/Exchange error fetching {coin_symbol} from {self.exchange_id}: {e}. Stopping fetch for this range.")
                    break  # Stop fetching for this request on network issues
                except ccxt.ExchangeError as e:
                    logger.error(f"Exchange specific error fetching {coin_symbol} from {self.exchange_id}: {e}. Stopping fetch for this range.")
                    break  # Stop fetching on other exchange errors

                if not candles:
                    logger.info(f"No more candles returned for {coin_symbol} starting from {since} on {self.exchange_id}.")
                    break

                # Filter candles strictly within the requested range *after* fetching
                # Note: Some exchanges might return candles starting *before* `since`
                filtered_candles = [c for c in candles if start_time <= c[0] < end_time]
                all_candles.extend(filtered_candles)

                last_timestamp = candles[-1][0]

                # Check if progress is being made or if we are past the end time
                if prev_since == last_timestamp or last_timestamp >= end_time:
                    logger.info(f"Stopping fetch for {coin_symbol}: No new data or end_time reached.")
                    break

                prev_since = last_timestamp
                # Calculate next `since` based on the last timestamp received + interval
                since = last_timestamp + interval_duration_ms

                # Respect rate limit (already handled by ccxt async, but explicit sleep can be added if needed)
                # await asyncio.sleep(self._exchange.rateLimit / 1000)

            return self._standardize_ohlcv_data(all_candles)
        except Exception as e:
            logger.exception(f"Unexpected error fetching historical data for {coin_symbol} on {self.exchange_id}: {e}")
            return []
        finally:
            # Ensure exchange connection is closed if this client instance is short-lived
            # If the client is long-lived, closing might be handled elsewhere.
            # await self.close() # Consider if closing here is appropriate
            pass

    # ... _parse_interval_ms remains the same ...
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
        logger.warning(f"Interval '{interval}' not found in exchange timeframes, falling back to 5 minutes.")
        return 5 * 60 * 1000

    # ... _standardize_ohlcv_data remains the same ...
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

    # Note: Real-time stream implementation needs significant changes for async
    # async def start_realtime_stream(self, coin_symbol: str, callback: Callable):
    #     raise NotImplementedError("Async WebSocket streaming not implemented yet.")
