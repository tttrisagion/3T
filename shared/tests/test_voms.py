import unittest

from shared.voms import VOMS


class TestVOMS(unittest.TestCase):
    """Test cases for the Virtual Order Management System (VOMS)."""

    def setUp(self):
        """Set up a fresh VOMS instance for each test."""
        self.voms = VOMS(starting_balance=10000, leverage=10, fee_rate=0.0004)

    def _run_scenarios_1_to_5(self):
        """Helper to run the first 5 scenarios to set up state for later tests."""
        self.voms.update_price(100)
        self.voms.add_trade(size=10)
        self.voms.update_price(110)
        self.voms.add_trade(size=5)
        self.voms.update_price(95)
        self.voms.add_trade(size=-8)

    def test_initialization(self):
        """Test that VOMS initializes with correct default values."""
        self.assertEqual(self.voms.starting_balance, 10000)
        self.assertEqual(self.voms.leverage, 10)
        self.assertIsNone(self.voms.get_metrics())

    def test_add_trade_before_price_update(self):
        """Test that adding a trade before a price update raises an error."""
        with self.assertRaises(RuntimeError):
            self.voms.add_trade(size=10)

    def test_scenario_1_open_long_position(self):
        """Test opening a single long position."""
        self.voms.update_price(100)
        self.voms.add_trade(size=10)
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["position_size"], 10)
        self.assertAlmostEqual(metrics["unrealized_pnl"], -0.8)
        self.assertAlmostEqual(metrics["account_balance"], 9999.2)

    def test_scenario_2_price_increases(self):
        """Test P&L calculation when the price increases."""
        self.voms.update_price(100)
        self.voms.add_trade(size=10)
        self.voms.update_price(110)
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["unrealized_pnl"], 99.2)
        self.assertAlmostEqual(metrics["account_balance"], 10099.2)

    def test_scenario_3_increase_long_position(self):
        """Test metrics after adding to an existing long position."""
        self.voms.update_price(100)
        self.voms.add_trade(size=10)
        self.voms.update_price(110)
        self.voms.add_trade(size=5)
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["position_size"], 15)
        self.assertAlmostEqual(metrics["unrealized_pnl"], 98.76)

    def test_scenario_4_price_decreases(self):
        """Test P&L calculation when the price decreases."""
        self.voms.update_price(100)
        self.voms.add_trade(size=10)
        self.voms.update_price(110)
        self.voms.add_trade(size=5)
        self.voms.update_price(95)
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["unrealized_pnl"], -126.24)

    def test_scenario_5_partially_close_position(self):
        """Test metrics after partially closing a position."""
        self._run_scenarios_1_to_5()
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["position_size"], 7)
        self.assertAlmostEqual(metrics["unrealized_pnl"], -126.848)

    def test_scenario_6_smaller_position_price_up(self):
        """Test P&L when the remaining smaller position gains value."""
        self._run_scenarios_1_to_5()
        self.voms.update_price(110)  # Price recovers
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["position_size"], 7)
        self.assertAlmostEqual(metrics["unrealized_pnl"], -21.848)

    def test_scenario_7_go_fully_short(self):
        """Test metrics after flipping from a long to a short position."""
        self._run_scenarios_1_to_5()
        self.voms.update_price(110)
        self.voms.add_trade(size=-17)  # From +7 to -10
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["position_size"], -10)
        self.assertAlmostEqual(metrics["unrealized_pnl"], -23.344)

    def test_scenario_8_short_position_drawdown(self):
        """Test P&L when the short position has a drawdown."""
        self._run_scenarios_1_to_5()
        self.voms.update_price(110)
        self.voms.add_trade(size=-17)
        self.voms.update_price(115)  # Price moves against short
        metrics = self.voms.get_metrics()
        self.assertAlmostEqual(metrics["position_size"], -10)
        self.assertAlmostEqual(metrics["unrealized_pnl"], -73.344)


if __name__ == "__main__":
    unittest.main()
