from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import logging


logger = logging.getLogger(__name__)

class IStoragePathStrategy(ABC):
    @abstractmethod
    def generate_base_path(self, context: Dict[str, Any]) -> str:
        """Generate the base path for storage based on context."""
        pass


class OHLCVPathStrategy(IStoragePathStrategy):
    def generate_base_path(self, context: Dict[str, Any]) -> str:
        required_keys = ['exchange', 'coin', 'interval']
        if not all(key in context for key in required_keys):
            raise ValueError(f"Context must contain keys: {required_keys}")
            
        record_type = 'ohlcv'
        
        exchange = str(context['exchange']).lower().replace(' ', '_').strip()
        coin = str(context['coin']).upper().replace('/', '_').strip()
        interval = str(context['interval']).lower().strip()
        
        if not all([exchange, coin, interval]):
            raise ValueError("Context values (exchange, coin, interval) cannot be empty.")
            
        return f"{record_type}/{exchange}/{coin}/{interval}"
        

