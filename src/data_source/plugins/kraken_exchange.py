"""
Kraken Exchange API client implementation using ccxt.
"""
import datetime
import logging
import time
from typing import Any, Dict, List, Optional, Callable, Union
import threading

import ccxt
from src.data_source.interfaces import IExchangeAPIClient
from .ccxt_exchange import CCXTExchangeClient

# Configure logging
logger = logging.getLogger(__name__)

# Time interval mapping (in minutes) for Kraken API
# Kraken supports: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600
INTERVAL_MAP = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
    "2w": 21600
}

# Default interval (5 minutes)
DEFAULT_INTERVAL = "5m"

# For now, just use the generic CCXTExchangeClient for Kraken
KrakenExchangeClient = lambda api_key=None, secret=None: CCXTExchangeClient('kraken', api_key=api_key, secret=secret)

def register_plugin(registry):
    """
    Register this plugin with the PluginRegistry.
    This function will be discovered and called by the plugin discovery mechanism.
    Args:
        registry: The plugin registry to register with
    """
    kraken_client = KrakenExchangeClient()
    registry.add_plugin(kraken_client)
