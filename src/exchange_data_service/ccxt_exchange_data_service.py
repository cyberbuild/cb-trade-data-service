
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from exchange_source.models import IExchangeRecord, ExchangeData
from exchange_source.clients.ccxt_exchange import CCXTExchangeClient
from .exchange_data_service import ExchangeDataService

INTERVALS = {
    '1m': timedelta(minutes=1),
    '5m': timedelta(minutes=5),
    '1h': timedelta(hours=1),
    '1d': timedelta(days=1),
    '1mo': timedelta(days=30),
    '1y': timedelta(days=365),
}


from config import Settings

from historical.manager import IHistoricalDataManager
from exchange_source.clients.iexchange_api_client import IExchangeAPIClient



class CCXTExchangeDataService(ExchangeDataService):
    def __init__(self, settings: Settings, historical_data_manager: IHistoricalDataManager):
        exchange_client = CCXTExchangeClient(config=settings.ccxt)
        super().__init__(exchange_client, historical_data_manager)
        
