"""
Unit tests for the reconciliation engine.

Tests all reconciliation logic cases from the specification and consensus mechanisms.
"""

import os

# Import the functions to test
import sys
import unittest
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import requests_mock

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "worker"))

from reconciliation_engine import (
    calculate_reconciliation_action,
    get_actual_state,
    get_base_symbol,
    get_current_price,
    get_desired_state,
    get_local_position,
    get_observer_state,
    reconcile_positions,
    send_order_to_gateway,
)


class TestReconciliationEngine(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.test_symbols = ["BTC/USDC:USDC", "BCH/USDC:USDC", "ENA/USDC:USDC"]
        self.test_wallet_address = "0x6F87795cF1B94f1572c161E0633751C9e226f955"

    def test_get_base_symbol(self):
        """Test base symbol extraction"""
        self.assertEqual(get_base_symbol("BTC/USDC:USDC"), "BTC")
        self.assertEqual(get_base_symbol("BCH/USDC:USDC"), "BCH")
        self.assertEqual(get_base_symbol("ENA/USDC:USDC"), "ENA")
        self.assertEqual(get_base_symbol("BTC"), "BTC")  # Already base symbol

    @patch("reconciliation_engine.is_market_open", return_value=True)
    @patch("reconciliation_engine.calculate_kelly_position_size")
    @patch("reconciliation_engine.get_db_connection")
    @patch("reconciliation_engine.get_latest_balance")
    @patch("reconciliation_engine.config")
    @patch("reconciliation_engine.get_current_price")
    def test_get_desired_state_with_active_runs(
        self,
        mock_price,
        mock_config,
        mock_balance,
        mock_db,
        mock_kelly_calc,
        mock_market_open,
    ):
        """Test desired state calculation with active runs"""
        # Mock configuration
        mock_config.get.return_value = 0.0025  # risk_pos_percentage

        # Mock latest balance
        mock_balance.return_value = 100000.0  # $100,000 balance

        # Mock current price
        mock_price.return_value = 118000.0  # BTC price

        # FORCES the Kelly Size to 250.0
        # Base Size = 100,000 * 0.0025 = 250.0
        # We force the calculator to return this base size, bypassing probation logic
        mock_kelly_calc.return_value = 250.0

        # Mock database response
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (
            "BTC",
            2.0,  # position
            100.0,  # total_pnl
            5,  # runs
        )
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db.return_value = mock_conn

        result = get_desired_state("BTC/USDC:USDC")

        # Expected Math:
        # Risk Size = 250.0
        # Position = 2.0
        # Price = 118,000
        # Result = (2.0 * 250.0) / 118,000 = 0.004237...
        expected = 0.004237
        self.assertAlmostEqual(result, expected, places=6)

    @patch("reconciliation_engine.get_db_connection")
    def test_get_desired_state_no_active_runs(self, mock_db):
        """Test desired state calculation with no active runs"""
        # Mock database response with no results
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db.return_value = mock_conn

        result = get_desired_state("BTC/USDC:USDC")
        self.assertEqual(result, 0.0)

    @patch("reconciliation_engine.get_db_connection")
    def test_get_current_price_from_market_data(self, mock_db):
        """Test price retrieval from market data table"""
        # Mock database response
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (118000.0,)
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db.return_value = mock_conn

        result = get_current_price("BTC/USDC:USDC")
        self.assertEqual(result, 118000.0)

    @patch("reconciliation_engine.get_db_connection")
    def test_get_local_position(self, mock_db):
        """Test local position retrieval"""
        # Mock database responses
        mock_cursor = Mock()
        # First call returns product_id
        # Second call returns position_size
        mock_cursor.fetchone.side_effect = [(1,), (0.00012,)]
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db.return_value = mock_conn

        result = get_local_position("BTC/USDC:USDC")
        self.assertEqual(result, 0.00012)

    @patch("reconciliation_engine.config")
    def test_get_observer_state(self, mock_config):
        """Test observer position retrieval"""
        # Mock configuration
        mock_config.get.return_value = [
            "http://localhost:8001/3T-observer.json"
        ]  # observer_nodes
        mock_config.get_secret.return_value = self.test_wallet_address  # wallet_address

        # Mock observer response
        observer_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "positions": {
                self.test_wallet_address: {
                    "assetPositions": [{"position": {"coin": "BTC", "szi": "0.00012"}}]
                }
            },
        }

        with requests_mock.Mocker() as m:
            m.get("http://localhost:8001/3T-observer.json", json=observer_data)

            result, error = get_observer_state("BTC/USDC:USDC")
            self.assertEqual(result, 0.00012)
            self.assertIsNone(error)

    @patch("reconciliation_engine.get_local_position")
    @patch("reconciliation_engine.get_observer_state")
    def test_get_actual_state_consensus(self, mock_observer, mock_local):
        """Test consensus mechanism when positions agree"""
        mock_local.return_value = 0.00012
        mock_observer.return_value = (0.00012, None)

        position, has_consensus = get_actual_state("BTC/USDC:USDC")

        self.assertTrue(has_consensus)
        self.assertEqual(position, 0.00012)

    @patch("reconciliation_engine.get_local_position")
    @patch("reconciliation_engine.get_observer_state")
    def test_get_actual_state_no_consensus(self, mock_observer, mock_local):
        """Test consensus mechanism when positions disagree"""
        mock_local.return_value = 0.00012
        mock_observer.return_value = (0.00015, None)

        position, has_consensus = get_actual_state("BTC/USDC:USDC")

        self.assertFalse(has_consensus)
        self.assertIsNone(position)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_both_short_reduce_position(self, mock_config, mock_price):
        """Test reconciliation logic: both short, need to reduce position"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        # Current: -0.0003, Target: -0.0002 (reduce short position)
        actual_position = -0.0003
        desired_position = -0.0002

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should buy to reduce short position
        # position_delta = (0.0002 - 0.0003) * -1 = 0.0001
        self.assertTrue(execute_trade)
        self.assertEqual(side, "buy")
        self.assertAlmostEqual(position_delta, 0.0001, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_both_short_add_position(self, mock_config, mock_price):
        """Test reconciliation logic: both short, need to add position"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        # Current: -0.0001, Target: -0.0003 (increase short position)
        actual_position = -0.0001
        desired_position = -0.0003

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should sell to increase short position
        # position_delta = abs(0.0001 - 0.0003) = 0.0002
        self.assertTrue(execute_trade)
        self.assertEqual(side, "sell")
        self.assertAlmostEqual(position_delta, 0.0002, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_short_to_long(self, mock_config, mock_price):
        """Test reconciliation logic: currently short, needs to go long"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        actual_position = -0.0002
        desired_position = 0.0001

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should buy: abs(-0.0002) + 0.0001 = 0.0003
        self.assertTrue(execute_trade)
        self.assertEqual(side, "buy")
        self.assertAlmostEqual(position_delta, 0.0003, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_long_to_short(self, mock_config, mock_price):
        """Test reconciliation logic: currently long, needs to go short"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        actual_position = 0.0002
        desired_position = -0.0001

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should sell: (0.0002 + abs(-0.0001)) * -1 = -0.0003
        self.assertTrue(execute_trade)
        self.assertEqual(side, "sell")
        self.assertAlmostEqual(position_delta, -0.0003, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_both_long_add_position(self, mock_config, mock_price):
        """Test reconciliation logic: both long, need to add position"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        # Current: 0.0001, Target: 0.0003 (increase long position)
        actual_position = 0.0001
        desired_position = 0.0003

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should buy: 0.0003 - 0.0001 = 0.0002
        self.assertTrue(execute_trade)
        self.assertEqual(side, "buy")
        self.assertAlmostEqual(position_delta, 0.0002, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_both_long_reduce_position(self, mock_config, mock_price):
        """Test reconciliation logic: both long, need to reduce position"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        # Current: 0.0003, Target: 0.0001 (reduce long position)
        actual_position = 0.0003
        desired_position = 0.0001

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should sell: 0.0003 - 0.0001 = 0.0002
        self.assertTrue(execute_trade)
        self.assertEqual(side, "sell")
        self.assertAlmostEqual(position_delta, 0.0002, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_create_new_position_long(self, mock_config, mock_price):
        """Test reconciliation logic: create new long position"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        actual_position = 0.0  # Flat
        desired_position = 0.0005  # Want long

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should buy: position_delta = 0.0005
        self.assertTrue(execute_trade)
        self.assertEqual(side, "buy")
        self.assertAlmostEqual(position_delta, 0.0005, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_create_new_position_short(self, mock_config, mock_price):
        """Test reconciliation logic: create new short position"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        actual_position = 0.0  # Flat
        desired_position = -0.0005  # Want short

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should sell: position_delta = -0.0005
        self.assertTrue(execute_trade)
        self.assertEqual(side, "sell")
        self.assertAlmostEqual(position_delta, -0.0005, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_liquidate_long_position(self, mock_config, mock_price):
        """Test reconciliation logic: liquidate long position (no risk)"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        actual_position = 0.0003  # Long position
        desired_position = 0.0  # No risk

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should sell: position_delta = 0.0003 * -1 = -0.0003
        self.assertTrue(execute_trade)
        self.assertEqual(side, "sell")
        self.assertAlmostEqual(position_delta, -0.0003, places=6)

    @patch("reconciliation_engine.get_current_price")
    @patch("reconciliation_engine.config")
    def test_reconciliation_liquidate_short_position(self, mock_config, mock_price):
        """Test reconciliation logic: liquidate short position (no risk)"""
        mock_config.get.return_value = 11.0  # minimum_trade_threshold
        mock_price.return_value = 118000.0

        actual_position = -0.0003  # Short position
        desired_position = 0.0  # No risk

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should buy: position_delta = -0.0003 * -1 = 0.0003
        self.assertTrue(execute_trade)
        self.assertEqual(side, "buy")
        self.assertAlmostEqual(position_delta, 0.0003, places=6)

    @patch("reconciliation_engine.get_current_price")
    def test_reconciliation_no_action_needed(self, mock_price):
        """Test reconciliation logic: no action needed when positions are aligned"""
        mock_price.return_value = 118000.0

        actual_position = 0.0002
        desired_position = 0.0002  # Same position

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # No action needed
        self.assertFalse(execute_trade)
        self.assertIsNone(side)
        self.assertEqual(position_delta, 0.0)

    @patch("reconciliation_engine.get_current_price")
    def test_reconciliation_threshold_check(self, mock_price):
        """Test that small differences below $11 threshold don't trigger trades"""
        mock_price.return_value = 118000.0

        # Small difference: 0.00009 * 118000 = $10.62 (below $11.0 threshold)
        actual_position = 0.001
        desired_position = 0.00109

        execute_trade, side, position_delta = calculate_reconciliation_action(
            actual_position, desired_position, "BTC/USDC:USDC"
        )

        # Should not execute trade due to threshold
        self.assertFalse(execute_trade)

    @patch("reconciliation_engine.config")
    def test_send_order_to_gateway(self, mock_config):
        """Test sending order to execution gateway"""
        mock_config.get.return_value = "http://localhost:8002"

        with requests_mock.Mocker() as m:
            m.post("http://localhost:8002/execute_order", json={"status": "success"})

            result = send_order_to_gateway("BTC/USDC:USDC", "buy", 0.001)

            self.assertTrue(result)

            # Check request was made correctly
            self.assertEqual(len(m.request_history), 1)
            req = m.request_history[0]
            self.assertEqual(req.method, "POST")

            # Check request body
            expected_data = {
                "symbol": "BTC/USDC:USDC",
                "side": "buy",
                "size": 0.001,
                "type": "market",
            }
            self.assertEqual(req.json(), expected_data)

    @patch("reconciliation_engine.get_latest_margin_usage")
    @patch("reconciliation_engine.get_latest_balance")
    @patch("reconciliation_engine.config")
    @patch("reconciliation_engine.get_desired_state")
    @patch("reconciliation_engine.get_actual_state")
    @patch("reconciliation_engine.calculate_reconciliation_action")
    @patch("reconciliation_engine.send_order_to_gateway")
    def test_reconcile_positions_full_cycle(
        self,
        mock_gateway,
        mock_calc,
        mock_actual,
        mock_desired,
        mock_config,
        mock_balance,
        mock_margin,
    ):
        """Test full reconciliation cycle"""

        # Mock config to handle multiple return values
        def config_get_side_effect(key, default=None):
            if key == "reconciliation_engine.symbols":
                return ["BTC/USDC:USDC"]
            if key == "reconciliation_engine.max_margin_usage_percentage":
                return 0.01
            return default

        mock_config.get.side_effect = config_get_side_effect

        # Mock desired state
        mock_desired.return_value = 0.001

        # Mock actual state with consensus
        mock_actual.return_value = (0.0005, True)

        # Mock reconciliation calculation
        mock_calc.return_value = (True, "buy", 0.0005)

        # Mock balance and margin
        mock_balance.return_value = 100000.0
        mock_margin.return_value = 100.0

        # Mock gateway success
        mock_gateway.return_value = True

        # Run reconciliation
        reconcile_positions()

        # Verify all functions were called
        mock_desired.assert_called_once_with("BTC/USDC:USDC")
        mock_actual.assert_called_once_with("BTC/USDC:USDC")
        mock_calc.assert_called_once_with(0.0005, 0.001, "BTC/USDC:USDC")
        mock_gateway.assert_called_once_with("BTC/USDC:USDC", "buy", 0.0005)

    @patch("reconciliation_engine.config")
    @patch("reconciliation_engine.get_desired_state")
    @patch("reconciliation_engine.get_actual_state")
    def test_reconcile_positions_no_consensus(
        self, mock_actual, mock_desired, mock_config
    ):
        """Test reconciliation skips when no consensus"""
        # Mock configuration
        mock_config.get.return_value = ["BTC/USDC:USDC"]

        # Mock desired state
        mock_desired.return_value = 0.001

        # Mock actual state without consensus
        mock_actual.return_value = (None, False)

        # Run reconciliation
        reconcile_positions()

        # Should skip trading due to no consensus
        mock_actual.assert_called_once_with("BTC/USDC:USDC")


if __name__ == "__main__":
    unittest.main()
