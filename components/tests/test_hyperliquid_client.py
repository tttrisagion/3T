"""
Tests for the HyperLiquid client utility library.
"""

from unittest.mock import Mock, patch

import pytest

from shared.hyperliquid_client import get_exchange_instruments, setup_hyperliquid_client


class TestHyperLiquidClient:
    @patch("shared.hyperliquid_client.config")
    @patch("shared.hyperliquid_client.eth_account.Account.from_key")
    @patch("shared.hyperliquid_client.Info")
    @patch("shared.hyperliquid_client.Exchange")
    def test_setup_hyperliquid_client_success(
        self, mock_exchange, mock_info, mock_from_key, mock_config
    ):
        # Setup mocks
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.privateKey": "test_private_key",
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
        }.get(key)

        mock_account = Mock()
        mock_account.address = "0x1234567890123456789012345678901234567890"
        mock_from_key.return_value = mock_account

        mock_info_instance = Mock()
        mock_info_instance.user_state.return_value = {
            "marginSummary": {"accountValue": "1000.0"}
        }
        mock_info_instance.spot_user_state.return_value = {
            "balances": [{"coin": "USDC", "total": "1000.0"}]
        }
        mock_info.return_value = mock_info_instance

        mock_exchange_instance = Mock()
        mock_exchange.return_value = mock_exchange_instance

        # Test successful setup
        address, info, exchange = setup_hyperliquid_client()

        assert address == "0x1234567890123456789012345678901234567890"
        assert info == mock_info_instance
        assert exchange == mock_exchange_instance

        mock_config.get_secret.assert_any_call("exchanges.hyperliquid.privateKey")
        mock_config.get_secret.assert_any_call("exchanges.hyperliquid.walletAddress")

    @patch("shared.hyperliquid_client.config")
    def test_setup_hyperliquid_client_missing_private_key(self, mock_config):
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.privateKey": None,
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
        }.get(key)

        with pytest.raises(
            ValueError,
            match="Missing required secret: exchanges.hyperliquid.privateKey",
        ):
            setup_hyperliquid_client()

    @patch("shared.hyperliquid_client.config")
    def test_setup_hyperliquid_client_missing_wallet_address(self, mock_config):
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.privateKey": "test_private_key",
            "exchanges.hyperliquid.walletAddress": None,
        }.get(key)

        with pytest.raises(
            ValueError,
            match="Missing required secret: exchanges.hyperliquid.walletAddress",
        ):
            setup_hyperliquid_client()

    @patch("shared.hyperliquid_client.config")
    @patch("shared.hyperliquid_client.eth_account.Account.from_key")
    @patch("shared.hyperliquid_client.Info")
    def test_setup_hyperliquid_client_no_equity(
        self, mock_info, mock_from_key, mock_config
    ):
        # Setup mocks for no equity scenario
        mock_config.get_secret.side_effect = lambda key: {
            "exchanges.hyperliquid.privateKey": "test_private_key",
            "exchanges.hyperliquid.walletAddress": "0x1234567890123456789012345678901234567890",
        }.get(key)

        mock_account = Mock()
        mock_account.address = "0x1234567890123456789012345678901234567890"
        mock_from_key.return_value = mock_account

        mock_info_instance = Mock()
        mock_info_instance.user_state.return_value = {
            "marginSummary": {"accountValue": "0.0"}
        }
        mock_info_instance.spot_user_state.return_value = {"balances": []}
        mock_info_instance.base_url = "https://api.hyperliquid.xyz"
        mock_info.return_value = mock_info_instance

        with pytest.raises(Exception, match="No accountValue"):
            setup_hyperliquid_client()

    @patch("mysql.connector.connect")
    @patch("shared.hyperliquid_client.config")
    def test_get_exchange_instruments(self, mock_config, mock_connect):
        # Setup mocks
        mock_config.get.side_effect = lambda key: {
            "database.host": "localhost",
            "database.user": "root",
            "database.database": "3t",
        }.get(key)
        mock_config.get_secret.return_value = "password"

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            {"name": "BTC"},
            {"name": "ETH"},
            {"name": "SOL"},
        ]

        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.is_connected.return_value = True
        mock_connect.return_value = mock_connection

        # Test getting instruments
        instruments = get_exchange_instruments()

        assert instruments == ["BTC", "ETH", "SOL"]
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
