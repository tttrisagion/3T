"""
Tests for the resilient exchange manager.
"""

import time
from unittest.mock import Mock, patch

import pytest

from shared.exchange_manager import ExchangeManager


class TestExchangeManager:
    def setup_method(self):
        # Reset singleton for each test
        ExchangeManager._instance = None

    @patch("shared.exchange_manager.config")
    @patch("shared.exchange_manager.ccxt")
    def test_get_exchange_success(self, mock_ccxt, mock_config):
        # Setup mocks
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.apiKey": "test_api_key",
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
            "exchanges.hyperliquid.privateKey": "test_private_key",
        }.get(key)

        mock_exchange = Mock()
        mock_exchange.fetch_time.return_value = int(time.time() * 1000)
        mock_ccxt.hyperliquid.return_value = mock_exchange

        manager = ExchangeManager()
        exchange = manager.get_exchange()

        assert exchange == mock_exchange
        mock_ccxt.hyperliquid.assert_called_once()

    @patch("shared.exchange_manager.config")
    def test_missing_secrets_raises_error(self, mock_config):
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.apiKey": None,
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
            "exchanges.hyperliquid.privateKey": "test_private_key",
        }.get(key)

        manager = ExchangeManager()
        with pytest.raises(ValueError, match="Missing HyperLiquid secrets"):
            manager.get_exchange()

    @patch("shared.exchange_manager.config")
    @patch("shared.exchange_manager.ccxt")
    def test_health_check_failure_recreates_exchange(self, mock_ccxt, mock_config):
        # Setup mocks
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.apiKey": "test_api_key",
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
            "exchanges.hyperliquid.privateKey": "test_private_key",
        }.get(key)

        # First exchange fails health check
        failing_exchange = Mock()
        failing_exchange.fetch_time.side_effect = Exception("Network error")

        # Second exchange succeeds
        healthy_exchange = Mock()
        healthy_exchange.fetch_time.return_value = int(time.time() * 1000)

        mock_ccxt.hyperliquid.side_effect = [failing_exchange, healthy_exchange]

        manager = ExchangeManager()
        manager.health_check_interval = 0  # Force health check

        exchange = manager.get_exchange()

        # Should have created two exchanges due to health check failure
        assert mock_ccxt.hyperliquid.call_count == 2
        assert exchange == healthy_exchange

    @patch("shared.exchange_manager.config")
    @patch("shared.exchange_manager.ccxt")
    def test_execute_with_retry_success_after_failure(self, mock_ccxt, mock_config):
        # Setup mocks
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.apiKey": "test_api_key",
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
            "exchanges.hyperliquid.privateKey": "test_private_key",
        }.get(key)

        mock_exchange = Mock()
        mock_exchange.fetch_time.return_value = int(time.time() * 1000)
        mock_ccxt.hyperliquid.return_value = mock_exchange

        # Mock operation that fails once then succeeds
        mock_operation = Mock()
        mock_operation.side_effect = [Exception("Network error"), "success"]

        manager = ExchangeManager()
        result = manager.execute_with_retry(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 2

    @patch("shared.exchange_manager.config")
    @patch("shared.exchange_manager.ccxt")
    def test_circuit_breaker_opens_after_failures(self, mock_ccxt, mock_config):
        # Setup mocks
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.apiKey": "test_api_key",
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
            "exchanges.hyperliquid.privateKey": "test_private_key",
        }.get(key)

        failing_exchange = Mock()
        failing_exchange.fetch_time.side_effect = Exception("Network error")
        mock_ccxt.hyperliquid.return_value = failing_exchange

        manager = ExchangeManager()
        manager.circuit_breaker_threshold = 2  # Lower threshold for testing
        manager.health_check_interval = 0  # Force health checks

        # Multiple failures should eventually open circuit breaker
        for i in range(manager.circuit_breaker_threshold + 1):
            try:
                manager.get_exchange()
            except Exception:
                pass  # Expected to fail

        # Now circuit breaker should be open
        with pytest.raises(Exception, match="Circuit breaker open"):
            manager.get_exchange()

    def test_singleton_pattern(self):
        manager1 = ExchangeManager()
        manager2 = ExchangeManager()

        assert manager1 is manager2
