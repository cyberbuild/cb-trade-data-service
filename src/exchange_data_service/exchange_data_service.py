from typing import Optional
from datetime import datetime, timedelta

from exchange_source.models import ExchangeData
from .interface import IExchangeDataService, Interval
from storage.paging import Paging
from historical.manager import IHistoricalDataManager
from exchange_source.clients.iexchange_api_client import IExchangeAPIClient


class ExchangeDataService(IExchangeDataService):
    def __init__(
        self,
        exchange_client: IExchangeAPIClient,
        historical_manager: IHistoricalDataManager,
    ):
        self.historical_manager = historical_manager
        self.exchange_client = exchange_client

    async def get_ohlcv_data(
        self,
        symbol: str,
        interval: Interval,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        paging: Optional[Paging] = None,
    ) -> ExchangeData:
        """
        Get OHLCV data with optional pagination support
        """
        await self.sync_with_exchange(symbol, interval)

        from exchange_source.models import Metadata

        metadata = Metadata(
            {
                "data_type": "ohlcv",
                "exchange": self.exchange_client.get_exchange_name(),
                "coin": symbol,
                "interval": interval.value,
            }
        )

        if paging is None:
            paging = Paging.all_records()

        return await self.historical_manager.get_historical_data(
            metadata, start, end, paging
        )

    async def sync_with_exchange(
        self, symbol: str, interval: Interval
    ) -> "IExchangeDataService":
        try:
            from exchange_source.models import Metadata

            metadata = Metadata(
                {
                    "data_type": "ohlcv",
                    "exchange": self.exchange_client.get_exchange_name(),
                    "coin": symbol,
                    "interval": interval.value,
                }
            )

            latest_entry = await self.historical_manager.get_most_current_data(metadata)

            now = datetime.now()
            interval_delta = interval.to_timedelta()
            if latest_entry is None:
                start_time = now - timedelta(days=1)
                need_sync = True
            else:
                latest_timestamp = latest_entry.get("timestamp")
                if isinstance(latest_timestamp, int):
                    latest_datetime = datetime.fromtimestamp(latest_timestamp / 1000)
                else:
                    latest_datetime = latest_timestamp

                time_since_last = now - latest_datetime
                need_sync = time_since_last > interval_delta
                start_time = latest_datetime

            if need_sync:
                context = {
                    "exchange": self.exchange_client.get_exchange_name(),
                    "symbol": symbol,
                    "interval": interval.value,
                }

                exchange_data = await self.exchange_client.fetch_ohlcv_data(
                    coin_symbol=symbol,
                    start_time=start_time,
                    end_time=now,
                    interval=interval.value,
                )

                if exchange_data and exchange_data.data:
                    await self.historical_manager.save_data(context, exchange_data)

        except Exception as e:
            print(f"Error syncing data for {symbol} {interval.value}: {e}")

        finally:
            await self.exchange_client.close()

        return self
