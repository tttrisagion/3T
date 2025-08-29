"""
Tests for Kelly Strategy functionality
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from worker.reconciliation_engine import (
    calculate_kelly_metrics,
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

        win_rate, reward_ratio, kelly_percentage = calculate_kelly_metrics()

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

        win_rate, reward_ratio, kelly_percentage = calculate_kelly_metrics()

        # Should return calculated values
        assert win_rate is not None
        assert reward_ratio is not None
        assert kelly_percentage is not None

        # Check basic calculations
        assert win_rate == 6 / 12  # 6 wins out of 12 total
        assert reward_ratio > 0  # Should be positive
        assert isinstance(kelly_percentage, float)

    @patch("worker.reconciliation_engine.calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_no_data(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test Kelly position sizing with no historical data"""
        # Mock no Kelly data available
        mock_kelly_metrics.return_value = (None, None, None)

        # Mock tracer
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Should return base size when no Kelly data
        assert adjusted_size == base_size

    @patch("worker.reconciliation_engine.calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_with_kelly_data(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test Kelly position sizing with valid Kelly data"""
        # Mock Kelly data
        mock_kelly_metrics.return_value = (
            0.6,
            2.0,
            0.3,
        )  # 60% win rate, 2:1 reward ratio, 30% Kelly
        mock_config.get.return_value = 0.5  # 50% threshold

        # Mock tracer
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Should increase position size by Kelly percentage
        expected = base_size + (base_size * 0.3)  # 100 + (100 * 0.3) = 130
        assert adjusted_size == expected

    @patch("worker.reconciliation_engine.calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_threshold_cap(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test Kelly position sizing with threshold capping"""
        # Mock Kelly data with high Kelly percentage
        mock_kelly_metrics.return_value = (
            0.8,
            3.0,
            0.7,
        )  # 70% Kelly (above 50% threshold)
        mock_config.get.return_value = 0.5  # 50% threshold

        # Mock tracer
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Should cap at threshold (50%)
        expected = base_size + (base_size * 0.5)  # 100 + (100 * 0.5) = 150
        assert adjusted_size == expected

    @patch("worker.reconciliation_engine.calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_negative_kelly(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test Kelly position sizing with negative Kelly percentage"""
        # Mock Kelly data with negative Kelly percentage
        mock_kelly_metrics.return_value = (0.3, 1.5, -0.2)  # Negative Kelly
        mock_config.get.return_value = 0.5  # 50% threshold

        # Mock tracer
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Should not reduce position size (clamped to 0)
        assert adjusted_size == base_size  # No reduction, stays at base size
