"""
Exchange Adapters module.
Provides abstract adapter interfaces and dynamic factory lookups for multiple execution venues.
"""

from shared.adapters.base import BaseExchangeAdapter
from shared.adapters.hyperliquid import HyperliquidAdapter
from shared.adapters.tradfi import TradfiAdapter
from shared.database import get_exchange_name_for_symbol

# Instantiated singletons for efficient reuse
_adapters = {
    "hyperliquid": HyperliquidAdapter(),
    "tradfi": TradfiAdapter()
}

def get_adapter(exchange_name: str) -> BaseExchangeAdapter:
    """
    Get the concrete adapter instance for a specific exchange name.
    Defaults to HyperliquidAdapter if unknown.
    """
    name = exchange_name.lower().strip()
    return _adapters.get(name, _adapters["hyperliquid"])

def get_adapter_for_symbol(symbol: str) -> BaseExchangeAdapter:
    """
    Get the concrete adapter instance for a given trading symbol by checking the DB.
    """
    exchange_name = get_exchange_name_for_symbol(symbol)
    return get_adapter(exchange_name)
