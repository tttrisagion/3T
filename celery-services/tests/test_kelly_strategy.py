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
            "height IS NOT NULL"
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
            "height IS NOT NULL"
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
    def test_calculate_kelly_position_size_no_historical_data(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing when there is no historical Kelly data."""
        # Mock metrics: current is positive, historical is None
        mock_kelly_metrics.side_effect = [
            (0.6, 2.0, 0.3),  # Current
            (None, None, None),  # Historical
        ]
        mock_config.get.return_value = {}  # Default config

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Should use current Kelly score as fallback
        expected = base_size + (base_size * 0.3)  # 130
        assert adjusted_size == expected

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_no_current_data(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing when there is no current Kelly data."""
        # Mock metrics: current is None, historical is positive
        mock_kelly_metrics.side_effect = [
            (None, None, None),  # Current
            (0.5, 2.0, 0.25),  # Historical
        ]
        mock_config.get.return_value = {}

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Should return base size as it cannot compare
        assert adjusted_size == base_size

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_positive_performance(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing when current performance is better than historical."""
        # Mock metrics: current > historical
        mock_kelly_metrics.side_effect = [
            (0.7, 2.5, 0.5),  # Current (Kelly 50%)
            (0.6, 2.0, 0.3),  # Historical (Kelly 30%)
        ]
        mock_config.get.return_value = {
            "kelly_max_increase": 1.0,
            "kelly_max_decrease": -0.5,
        }

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # performance_ratio = 0.5 / 0.3 = 1.666...
        # kelly_performance = 1.666... - 1 = 0.666...
        # multiplier = 1 + 0.666... = 1.666...
        expected = base_size * (1 + (0.5 / 0.3 - 1))
        assert abs(adjusted_size - expected) < 1e-9

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_negative_performance(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test sizing when current performance is worse than historical."""
        # Mock metrics: current < historical
        mock_kelly_metrics.side_effect = [
            (0.5, 1.5, 0.1),  # Current (Kelly 10%)
            (0.6, 2.0, 0.3),  # Historical (Kelly 30%)
        ]
        mock_config.get.return_value = {
            "kelly_max_increase": 1.0,
            "kelly_max_decrease": -0.5,
        }

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # performance_ratio = 0.1 / 0.3 = 0.333...
        # kelly_performance = 0.333... - 1 = -0.666...
        # Capped at -0.5
        # multiplier = 1 - 0.5 = 0.5
        expected = base_size * 0.5
        assert abs(adjusted_size - expected) < 1e-9

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_max_increase_cap(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test that position size increase is capped."""
        # Mock metrics: current >> historical
        mock_kelly_metrics.side_effect = [
            (0.8, 4.0, 0.7),  # Current (Kelly 70%)
            (0.5, 2.0, 0.2),  # Historical (Kelly 20%)
        ]
        # Ratio = 3.5, performance = 2.5, capped at 1.0
        mock_config.get.return_value = {
            "kelly_max_increase": 1.0,
            "kelly_max_decrease": -0.5,
        }

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Capped at 100% increase (multiplier of 2)
        expected = base_size * 2.0
        assert abs(adjusted_size - expected) < 1e-9

    @patch("worker.reconciliation_engine._calculate_kelly_metrics")
    @patch("worker.reconciliation_engine.config")
    @patch("worker.reconciliation_engine.tracer")
    def test_calculate_kelly_position_size_max_decrease_floor(
        self, mock_tracer, mock_config, mock_kelly_metrics
    ):
        """Test that position size decrease is floored."""
        # Mock metrics: current << historical
        mock_kelly_metrics.side_effect = [
            (0.4, 1.2, 0.05),  # Current (Kelly 5%)
            (0.7, 3.0, 0.5),  # Historical (Kelly 50%)
        ]
        # Ratio = 0.1, performance = -0.9, floored at -0.8
        mock_config.get.return_value = {
            "kelly_max_increase": 1.0,
            "kelly_max_decrease": -0.8,
        }

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = (
            mock_span
        )

        base_size = 100.0
        adjusted_size = calculate_kelly_position_size(base_size)

        # Floored at 80% decrease (multiplier of 0.2)
        expected = base_size * 0.2
        assert abs(adjusted_size - expected) < 1e-9
