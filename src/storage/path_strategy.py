from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import logging
from exchange_source.models import Metadata


logger = logging.getLogger(__name__)

class IStoragePathStrategy(ABC):
    @abstractmethod
    def generate_base_path(self, context: Dict[str, Any]) -> str:
        """Generate the base path for storage based on context."""
        pass
    
    @abstractmethod
    def generate_path_prefix(self, context: Dict[str, Any]) -> str:
        """Generate a partial path prefix (e.g., for listing subdirectories)."""
        pass
    
    @abstractmethod
    def get_metadata(self, path: str) -> Metadata:
        """Rehydrate metadata from a storage path."""
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
    
    def generate_path_prefix(self, context: Dict[str, Any]) -> str:
        """
        Generate a path prefix for listing directories.
        This allows for partial contexts (e.g., just exchange and data_type).
        """
        record_type = 'ohlcv'
        
        # If exchange is provided, include it in the path
        if 'exchange' in context:
            exchange = str(context['exchange']).lower().replace(' ', '_').strip()
            prefix = f"{record_type}/{exchange}"
            
            # If coin is also provided, include it too
            if 'coin' in context:
                coin = str(context['coin']).upper().replace('/', '_').strip()
                prefix = f"{prefix}/{coin}"
                
                # If interval is also provided, include it as well
                if 'interval' in context:
                    interval = str(context['interval']).lower().strip()
                    prefix = f"{prefix}/{interval}"
            
            return prefix
        
        # If no exchange provided, just return the record type
        return record_type
    
    def get_metadata(self, path: str) -> Metadata:
        parts = path.strip("/").split("/")
        if len(parts) < 4:
            raise ValueError(f"Invalid path: {path}")
        record_type, exchange, coin, interval = parts[:4]
        if record_type != 'ohlcv':
            raise ValueError(f"Invalid data_type in path: {record_type}")
        return Metadata({
            'data_type': record_type,
            'exchange': exchange,
            'coin': coin,
            'interval': interval
        })


