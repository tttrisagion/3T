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
        mock_exchange.load_markets.return_value = {}
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
        failing_exchange.load_markets.side_effect = Exception("Network error")

        # Second exchange succeeds
        healthy_exchange = Mock()
        healthy_exchange.load_markets.return_value = {}

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

        manager = ExchangeManager()
        manager.circuit_breaker_threshold = 3  # Lower threshold for testing
        manager.circuit_breaker_reset_time = 3600  # 1 hour - prevent reset during test

        # Initially circuit breaker should be closed
        assert not manager._is_circuit_breaker_open("hyperliquid")

        # Record failures directly to test the circuit breaker logic
        for i in range(manager.circuit_breaker_threshold):
            manager._record_failure("hyperliquid")
            # Circuit breaker should open when threshold is reached
            if i < manager.circuit_breaker_threshold - 1:
                assert not manager._is_circuit_breaker_open("hyperliquid")
            else:
                assert manager._is_circuit_breaker_open("hyperliquid")

        # Verify failure count
        assert (
            manager._failure_count["hyperliquid"] == manager.circuit_breaker_threshold
        )

        # Reset should close circuit breaker
        manager._reset_failure_count("hyperliquid")
        assert not manager._is_circuit_breaker_open("hyperliquid")
        assert manager._failure_count["hyperliquid"] == 0

    def test_singleton_pattern(self):
        manager1 = ExchangeManager()
        manager2 = ExchangeManager()

        assert manager1 is manager2
