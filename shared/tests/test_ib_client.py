import unittest
from unittest.mock import patch, MagicMock, AsyncMock

from shared.ib_client import IBClient


class TestIBClient(unittest.TestCase):
    """Test cases for the Interactive Brokers client wrapper."""

    def setUp(self):
        self.client = IBClient()

    @patch("shared.ib_client.IB")
    def test_get_balance_and_positions_success(self, mock_ib_class):
        """Test retrieving and normalizing account balance and portfolio positions."""
        mock_ib = mock_ib_class.return_value
        mock_ib.isConnected.return_value = True
        
        # Mock connectAsync (asynchronous)
        mock_ib.connectAsync = AsyncMock()
        
        # Mock accountSummaryAsync (asynchronous)
        item_net_liq = MagicMock()
        item_net_liq.tag = "NetLiquidation"
        item_net_liq.value = "150000.50"
        
        item_maint_margin = MagicMock()
        item_maint_margin.tag = "MaintMarginReq"
        item_maint_margin.value = "35000.25"
        
        mock_ib.accountSummaryAsync = AsyncMock(return_value=[item_net_liq, item_maint_margin])
        
        # Mock portfolio (synchronous)
        mock_contract = MagicMock()
        mock_contract.secType = "STK"
        mock_contract.symbol = "AAPL"
        
        mock_item = MagicMock()
        mock_item.contract = mock_contract
        mock_item.position = 100
        mock_item.marketValue = 18500.0
        mock_item.unrealizedPNL = 450.0
        
        mock_ib.portfolio.return_value = [mock_item]
        
        # Run method
        balance, positions = self.client.get_balance_and_positions()
        
        # Asserts
        mock_ib.connectAsync.assert_called_once_with(self.client.host, self.client.port, clientId=unittest.mock.ANY)
        self.assertEqual(balance["account_value"], 150000.50)
        self.assertEqual(balance["cross_maintenance_margin_used"], 35000.25)
        
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0], {
            "symbol": "AAPL",
            "position_size": 100.0,
            "position_value": 18500.0,
            "unrealized_pnl": 450.0
        })

    @patch("shared.ib_client.IB")
    @patch("shared.ib_client.Stock")
    @patch("shared.ib_client.MarketOrder")
    def test_place_market_order(self, mock_market_order, mock_stock, mock_ib_class):
        """Test placement of a market order."""
        mock_ib = mock_ib_class.return_value
        mock_ib.isConnected.return_value = True
        mock_ib.connectAsync = AsyncMock()
        mock_ib.qualifyContractsAsync = AsyncMock()
        
        # Mock trade details returned by placeOrder
        mock_trade = MagicMock()
        mock_trade.order.orderId = 1234
        mock_trade.orderStatus.status = "Submitted"
        mock_trade.isDone.return_value = True
        mock_ib.placeOrder.return_value = mock_trade
        
        result = self.client.place_order("AAPL", "buy", 50, "market")
        
        mock_stock.assert_called_once_with("AAPL", "SMART", "USD")
        mock_market_order.assert_called_once_with("BUY", 50)
        mock_ib.placeOrder.assert_called_once()
        
        self.assertEqual(result["id"], "1234")
        self.assertEqual(result["status"], "Submitted")

    @patch("shared.ib_client.IB")
    def test_cancel_order_success(self, mock_ib_class):
        """Test successful order cancellation."""
        mock_ib = mock_ib_class.return_value
        mock_ib.isConnected.return_value = True
        mock_ib.connectAsync = AsyncMock()
        
        # Mock active trade
        mock_trade = MagicMock()
        mock_trade.order.orderId = 5555
        mock_ib.trades.return_value = [mock_trade]
        
        success = self.client.cancel_order("5555")
        self.assertTrue(success)
        mock_ib.cancelOrder.assert_called_once_with(mock_trade.order)

    @patch("shared.ib_client.IB")
    def test_cancel_order_not_found(self, mock_ib_class):
        """Test cancel returns False when order cannot be found."""
        mock_ib = mock_ib_class.return_value
        mock_ib.isConnected.return_value = True
        mock_ib.connectAsync = AsyncMock()
        mock_ib.trades.return_value = []
        
        success = self.client.cancel_order("9999")
        self.assertFalse(success)
        mock_ib.cancelOrder.assert_not_called()
