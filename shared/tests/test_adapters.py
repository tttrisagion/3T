import unittest
from unittest.mock import patch, MagicMock

from shared.adapters import get_adapter, get_adapter_for_symbol
from shared.adapters.base import BaseExchangeAdapter
from shared.adapters.hyperliquid import HyperliquidAdapter
from shared.adapters.tradfi import TradfiAdapter


class TestExchangeAdapters(unittest.TestCase):
    """Test cases for the polymorphic Exchange Adapters and Factory/Registry."""

    def test_get_adapter_instances(self):
        """Test that get_adapter returns the correct concrete class instances."""
        hl_adapter = get_adapter("hyperliquid")
        self.assertIsInstance(hl_adapter, HyperliquidAdapter)
        self.assertIsInstance(hl_adapter, BaseExchangeAdapter)

        tf_adapter = get_adapter("tradfi")
        self.assertIsInstance(tf_adapter, TradfiAdapter)
        self.assertIsInstance(tf_adapter, BaseExchangeAdapter)

        # Test case-insensitivity and whitespace resilience
        hl_adapter_case = get_adapter("  HyperLiquid  ")
        self.assertIsInstance(hl_adapter_case, HyperliquidAdapter)

        # Test default fallback for unknown exchange name
        default_adapter = get_adapter("unknown_venue")
        self.assertIsInstance(default_adapter, HyperliquidAdapter)

    @patch("shared.adapters.get_exchange_name_for_symbol")
    def test_get_adapter_for_symbol(self, mock_get_exchange):
        """Test that get_adapter_for_symbol maps symbols to the correct adapters via the database."""
        mock_get_exchange.return_value = "tradfi"
        tf_adapter = get_adapter_for_symbol("AAPL")
        self.assertIsInstance(tf_adapter, TradfiAdapter)
        mock_get_exchange.assert_called_once_with("AAPL")

        mock_get_exchange.reset_mock()
        mock_get_exchange.return_value = "hyperliquid"
        hl_adapter = get_adapter_for_symbol("BTC/USDC:USDC")
        self.assertIsInstance(hl_adapter, HyperliquidAdapter)
        mock_get_exchange.assert_called_once_with("BTC/USDC:USDC")

    def test_hyperliquid_market_always_open(self):
        """Test that HyperLiquid perps market is always open (24/7)."""
        hl_adapter = get_adapter("hyperliquid")
        self.assertTrue(hl_adapter.is_market_open("BTC/USDC:USDC"))

    @patch("shared.adapters.tradfi.config")
    def test_tradfi_test_mode_market_open(self, mock_config):
        """Test that TradfiAdapter.is_market_open returns True when test_mode is enabled."""
        mock_config.get.side_effect = lambda key, default=None: True if key == "trading_hours.test_mode" else default
        tf_adapter = get_adapter("tradfi")
        self.assertTrue(tf_adapter.is_market_open("AAPL"))

    def test_hyperliquid_position_clamping_and_rounding(self):
        """Test clamping of desired positions for HyperLiquid based on margin usage."""
        hl_adapter = get_adapter("hyperliquid")
        
        # Scenario 1: Margin usage (8.0) does not exceed budget cap (10.0) -> No clamping
        clamped_1 = hl_adapter.apply_position_clamping_and_rounding(
            symbol="BTC/USDC:USDC",
            desired_position=1.5,
            symbol_cap=10.0,
            symbol_margin_used=8.0
        )
        self.assertAlmostEqual(clamped_1, 1.5)

        # Scenario 2: Margin usage (15.0) exceeds budget cap (10.0) -> Clamped proportionally (10/15 = 2/3)
        clamped_2 = hl_adapter.apply_position_clamping_and_rounding(
            symbol="BTC/USDC:USDC",
            desired_position=1.5,
            symbol_cap=10.0,
            symbol_margin_used=15.0
        )
        self.assertAlmostEqual(clamped_2, 1.0)

    @patch.object(TradfiAdapter, "_get_current_price")
    def test_tradfi_position_clamping_and_rounding(self, mock_get_price):
        """Test clamping of desired positions for TradFi stocks based on estimated USD value, and whole-share rounding."""
        tf_adapter = get_adapter("tradfi")
        mock_get_price.return_value = 100.0 # Each share is $100
        
        # Scenario 1: Target value ($500) is well below budget cap ($1000) -> No clamping, but rounds fractional share to whole share
        clamped_1 = tf_adapter.apply_position_clamping_and_rounding(
            symbol="AAPL",
            desired_position=5.4, # 5.4 shares = $540
            symbol_cap=1000.0,
            symbol_margin_used=0.0
        )
        self.assertEqual(clamped_1, 5.0) # Rounds 5.4 to nearest whole share (5.0)

        # Scenario 2: Target value ($1500) exceeds budget cap ($1000) -> Clamped proportionally ($1000 / $1500 = 2/3 of 15 shares = 10 shares)
        clamped_2 = tf_adapter.apply_position_clamping_and_rounding(
            symbol="AAPL",
            desired_position=15.0, # 15 shares = $1500
            symbol_cap=1000.0,
            symbol_margin_used=0.0
        )
        self.assertEqual(clamped_2, 10.0) # Clamps to 10.0 shares exactly

        # Scenario 3: Desired position is flat (0.0) -> Stays 0.0
        clamped_3 = tf_adapter.apply_position_clamping_and_rounding(
            symbol="AAPL",
            desired_position=0.0,
            symbol_cap=1000.0,
            symbol_margin_used=0.0
        )
        self.assertEqual(clamped_3, 0.0)


if __name__ == "__main__":
    unittest.main()
