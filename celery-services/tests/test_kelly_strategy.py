"""
Tests for Kelly Strategy functionality
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from worker.reconciliation_engine import (
    _calculate_kelly_metrics,
    calculate_kelly_position_size,
)


class TestKellyStrategy:
    """Test Kelly Strategy calculations"""

    @patch("worker.reconciliation_engine.get_db_connection")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_metrics_insufficient_data(self, mock_tracer, mock_db):
        """Test Kelly metrics with insufficient historical data"""
        # Mock database response with insufficient data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1.5,), (2.0,)]  # Only 2 records
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value.__enter__.return_value = mock_conn

        # Mock tracer
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        win_rate, reward_ratio, kelly_percentage = _calculate_kelly_metrics(
            "height IS NOT NULL", "TEST/USDC:USDC"
        )

        # Should return None values due to insufficient data
        assert win_rate is None
        assert reward_ratio is None
        assert kelly_percentage is None

    @patch("worker.reconciliation_engine.get_db_connection")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_metrics_success(self, mock_tracer, mock_db):
        """Test Kelly metrics calculation with valid data"""
        # Mock database response with sufficient data (10+ records)
        pnl_data = [
            (5.0,),  # win
            (-2.0,),  # loss
            (3.0,),  # win
            (-1.5,),  # loss
            (4.0,),  # win
            (-2.5,),  # loss
            (6.0,),  # win
            (-1.0,),  # loss
            (2.5,),  # win
            (-3.0,),  # loss
            (1.5,),  # win
            (-2.2,),  # loss
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = pnl_data
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value.__enter__.return_value = mock_conn

        # Mock tracer
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        win_rate, reward_ratio, kelly_percentage = _calculate_kelly_metrics(
            "height IS NOT NULL", "TEST/USDC:USDC"
        )

        # Should return calculated values
        assert win_rate is not None
        assert reward_ratio is not None
        assert kelly_percentage is not None

        # Check basic calculations
        assert win_rate == 6 / 12  # 6 wins out of 12 total
        assert reward_ratio > 0  # Should be positive
        assert isinstance(kelly_percentage, float)

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_no_data(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing when there is no Kelly data."""
        mock_kelly_metrics.return_value = (None, None, None)
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size, "TEST/USDC:USDC")
        
        # FIX: Expect 10% probation size, not full base size
        # Old: assert adjusted_size == base_size
        assert adjusted_size == base_size * 0.1

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_historical_zero(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing when historical Kelly is zero."""
        mock_kelly_metrics.side_effect = [
            (0.6, 2.0, 0.3),  # Current
            (0.0, 0.0, 0.0),  # Historical
        ]
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size, "TEST/USDC:USDC")
        assert adjusted_size == base_size

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_positive_performance(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing with positive relative performance."""
        mock_kelly_metrics.side_effect = [
            (0.8, 3.0, -0.04),  # Current
            (0.7, 2.0, -0.08),  # Historical
        ]
        mock_config.get.return_value = {"kelly_threshold": 0.5}
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        # kelly_performance = 1 - (-0.04 / -0.08) = 1 - 0.5 = 0.5
        # expected_size = 100 + (100 * 0.5) = 150
        adjusted_size = calculate_kelly_position_size(base_size, "TEST/USDC:USDC")
        assert abs(adjusted_size - 150.0) < 1e-9

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_negative_performance(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing with negative relative performance."""
        mock_kelly_metrics.side_effect = [
            (0.6, 1.5, -0.12),  # Current
            (0.7, 2.0, -0.08),  # Historical
        ]
        mock_config.get.return_value = {"kelly_threshold": 0.5}
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        # kelly_performance = 1 - (-0.12 / -0.08) = 1 - 1.5 = -0.5
        # expected_size = 100 + (100 * -0.5) = 50
        adjusted_size = calculate_kelly_position_size(base_size, "TEST/USDC:USDC")
        assert abs(adjusted_size - 50.0) < 1e-9

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_threshold_cap(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing is capped by the threshold."""
        mock_kelly_metrics.side_effect = [
            (0.9, 4.0, -0.02),  # Current
            (0.7, 2.0, -0.08),  # Historical
        ]
        mock_config.get.return_value = {"kelly_threshold": 0.5}
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        # kelly_performance = 1 - (-0.02 / -0.08) = 1 - 0.25 = 0.75
        # Capped at 0.5
        # expected_size = 100 + (100 * 0.5) = 150
        adjusted_size = calculate_kelly_position_size(base_size, "TEST/USDC:USDC")
        assert abs(adjusted_size - 150.0) < 1e-9

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_negative_floor(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test that position size is floored at zero."""
        mock_kelly_metrics.side_effect = [
            (0.5, 1.0, -0.24),  # Current
            (0.7, 2.0, -0.08),  # Historical
        ]
        mock_config.get.return_value = {"kelly_threshold": 0.5}
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        # kelly_performance = 1 - (-0.24 / -0.08) = 1 - 3.0 = -2.0
        # Floored at -0.98 to prevent flip
        # expected_size = 100 + (100 * -0.98) = 2.0
        adjusted_size = calculate_kelly_position_size(base_size, "TEST/USDC:USDC")
        assert abs(adjusted_size - 2.0) < 1e-9
