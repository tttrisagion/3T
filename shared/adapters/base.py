"""
Base class defining the unified interface for all execution venues.
Allows the reconciliation engine and tasks to interact with any exchange agnostically.
"""

from abc import ABC, abstractmethod

class BaseExchangeAdapter(ABC):
    @abstractmethod
    def fetch_ohlcv(self, symbol: str, timeframe: str, lookback: int) -> list[tuple]:
        """
        Fetch historical interval OHLCV data for a symbol.
        Returns a list of (timestamp_ms, open, high, low, close, volume) tuples.
        """
        pass

    @abstractmethod
    def fetch_and_store_balance(self, cursor, exchange_id: int) -> tuple[float, float]:
        """
        Fetch account balances and positions, delete stale DB positions,
        insert fresh positions, and return (account_value, cross_maintenance_margin_used).
        """
        pass

    @abstractmethod
    def is_market_open(self, symbol: str) -> bool:
        """
        Check if the market for this asset class is open for safe operations.
        """
        pass

    @abstractmethod
    def get_observer_state(self, symbol: str, wallet_id: str = None) -> tuple[float | None, str | None, float]:
        """
        Get position from observer nodes and perform safety validation.
        Returns (position_size, error_message, margin_used).
        """
        pass

    @abstractmethod
    def apply_position_clamping_and_rounding(self, symbol: str, desired_position: float, symbol_cap: float) -> float:
        """
        Apply custom margin/USD clamping and size rounding rules to a desired position.
        """
        pass

    @abstractmethod
    def cancel_all_open_orders(self, symbols: list[str]) -> int:
        """
        Cancel active open orders for the given list of symbols.
        """
        pass
