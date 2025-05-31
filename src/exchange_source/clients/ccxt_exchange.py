import ccxt.async_support as ccxt
import logging
import time
from typing import Any, Dict, List, Optional, Callable, Union
import datetime
import asyncio

from exchange_source.config import CCXTConfig
from exchange_source.models import ExchangeData, OHLCVRecord
from exchange_source.clients.iexchange_api_client import IExchangeAPIClient


logger = logging.getLogger(__name__)

class CCXTExchangeClient(IExchangeAPIClient):
    def __init__(self, config: CCXTConfig, api_key: Optional[str] = None, **kwargs):
        self.config = config
        self.exchange_id = config.exchange_id
        exchange_class = getattr(ccxt, self.exchange_id)
        params = {'enableRateLimit': True}
        
        if api_key:
            params['apiKey'] = api_key
        params.update(kwargs)
        
        self._exchange = exchange_class(params)

    async def close(self):
        """Closes the underlying ccxt exchange connection."""
        try:
            await self._exchange.close()
            logger.info(f"Closed connection for exchange: {self.exchange_id}")
        except Exception as e:
            logger.error(f"Error closing connection for exchange {self.exchange_id}: {e}")

    def get_exchange_name(self) -> str:
        return self.exchange_id

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

    async def fetch_ohlcv_data(
        self,
        coin_symbol: str,
        start_time: Union[int, datetime.datetime],
        end_time: Union[int, datetime.datetime],
        interval: str = '5m',
        max_limit: Optional[int] = None
    ) -> ExchangeData:
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
                raise ValueError(f"Symbol {coin_symbol} not available on {self.exchange_id}")

            all_candles = []
            since = start_time
            interval_duration_ms = self._parse_interval_ms(interval)
            limit = max_limit or getattr(self._exchange, 'maxOHLCVLimit', 1000)
            prev_since = None

            while since < end_time:
                logger.info(f"Fetching data for {coin_symbol} from {datetime.datetime.fromtimestamp(since/1000, tz=datetime.timezone.utc)} (ts: {since}) on {self.exchange_id}")
                try:
                    candles = await self._exchange.fetch_ohlcv(
                        symbol=coin_symbol,
                        timeframe=interval,
                        since=since,
                        limit=limit
                    )
                except ccxt.RateLimitExceeded as e:
                    logger.warning(f"Rate limit exceeded on {self.exchange_id}. Retrying after delay... Error: {e}")
                    await asyncio.sleep(self._exchange.rateLimit / 1000 * 1.1)
                    continue
                except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as e:
                    logger.error(f"Network/Exchange error fetching {coin_symbol} from {self.exchange_id}: {e}. Stopping fetch for this range.")
                    break
                except ccxt.ExchangeError as e:
                    logger.error(f"Exchange specific error fetching {coin_symbol} from {self.exchange_id}: {e}. Stopping fetch for this range.")
                    break

                if not candles:
                    logger.info(f"No more candles returned for {coin_symbol} starting from {since} on {self.exchange_id}.")
                    break

                # Filter candles strictly within the requested range
                filtered_candles = [c for c in candles if start_time <= c[0] < end_time]
                all_candles.extend(filtered_candles)

                last_timestamp = candles[-1][0]

                # Check if progress is being made
                if prev_since == last_timestamp or last_timestamp >= end_time:
                    logger.info(f"Stopping fetch for {coin_symbol}: No new data or end_time reached.")
                    break

                prev_since = last_timestamp
                since = last_timestamp + interval_duration_ms

            # Create standardized data
            standardized_data = self._standardize_ohlcv_data(all_candles)
            
            # Create metadata separately from the data
            metadata = {
                'data_type': 'ohlcv',
                'exchange': self.get_exchange_name(),
                'coin': coin_symbol,
                'interval': interval
            }
            
            # Return container with data and metadata properly separated
            return ExchangeData(data=standardized_data, metadata=metadata)
        except ValueError:
            raise
        except Exception as e:
            logger.exception(f"Unexpected error fetching historical data for {coin_symbol} on {self.exchange_id}: {e}")
            metadata = {
                'data_type': 'ohlcv',
                'exchange': self.exchange_id,
                'coin': coin_symbol,
                'interval': interval
            }
            return ExchangeData(data=[], metadata=metadata)

    def _parse_interval_ms(self, interval: str) -> int:
        # Use exchange's timeframes if available, else fallback
        if hasattr(self._exchange, 'timeframes') and interval in self._exchange.timeframes:
            # Parse interval string like '5m', '1h', etc.
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

    def _standardize_ohlcv_data(self, candles: List) -> List[OHLCVRecord]:
        standardized_candles = []
        for candle in candles:
            timestamp, open_price, high, low, close, volume = candle
            standardized_candle = OHLCVRecord({
                "timestamp": timestamp,
                "open": float(open_price),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume)
            })
            standardized_candles.append(standardized_candle)
        return standardized_candles

    async def start_realtime_stream(self, coin_symbol: str, callback: Callable):
        raise NotImplementedError("Async WebSocket streaming not implemented yet.")
