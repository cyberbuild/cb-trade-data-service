from datetime import timedelta

from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from .exchange_data_service import ExchangeDataService
from config import Settings
from historical.manager import IHistoricalDataManager

INTERVALS = {
    '1m': timedelta(minutes=1),
    '5m': timedelta(minutes=5),
    '1h': timedelta(hours=1),
    '1d': timedelta(days=1),
    '1mo': timedelta(days=30),
    '1y': timedelta(days=365),
}


class CCXTExchangeDataService(ExchangeDataService):
    def __init__(self, settings: Settings,
                 historical_data_manager: IHistoricalDataManager):
        exchange_client = CCXTExchangeClient(config=settings.ccxt)
        super().__init__(exchange_client, historical_data_manager)
