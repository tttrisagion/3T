import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, UTC

from shared.iqfeed_client import IQFeedClient


class TestIQFeedClient(unittest.TestCase):
    """Test cases for the DTN IQFeed historical lookup client."""

    def setUp(self):
        self.client = IQFeedClient()

    def test_timeframe_to_seconds(self):
        """Test conversion of standard 3T timeframes to seconds."""
        self.assertEqual(self.client._timeframe_to_seconds("1m"), 60)
        self.assertEqual(self.client._timeframe_to_seconds("4h"), 14400)
        self.assertEqual(self.client._timeframe_to_seconds("1d"), 86400)
        
        with self.assertRaises(ValueError):
            self.client._timeframe_to_seconds("1x")

    def test_parse_response_success(self):
        """Test parsing valid historical responses from IQFeed."""
        raw_response = (
            "2026-06-01 12:00:00,105.5,104.2,105.0,105.2,100000,10000,50\r\n"
            "2026-06-01 12:01:00,106.0,105.0,105.2,105.8,110000,11000,55\r\n"
            "!ENDMSG!\r\n"
        )
        
        parsed = self.client._parse_response(raw_response)
        
        self.assertEqual(len(parsed), 2)
        
        from zoneinfo import ZoneInfo
        # Row 1 check (parsed as Eastern Time and converted to UTC)
        dt1 = datetime.strptime("2026-06-01 12:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/New_York"))
        ts1 = int(dt1.astimezone(ZoneInfo("UTC")).timestamp() * 1000)
        self.assertEqual(parsed[0], (ts1, 105.0, 105.5, 104.2, 105.2, 10000.0))
        
        # Row 2 check
        dt2 = datetime.strptime("2026-06-01 12:01:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/New_York"))
        ts2 = int(dt2.astimezone(ZoneInfo("UTC")).timestamp() * 1000)
        self.assertEqual(parsed[1], (ts2, 105.2, 106.0, 105.0, 105.8, 11000.0))

    def test_parse_response_error(self):
        """Test that server errors raised by IQFeed are caught and thrown."""
        raw_response = "E,!ERROR!,No historical data found for symbol\r\n"
        
        with self.assertRaises(ValueError) as context:
            self.client._parse_response(raw_response)
            
        self.assertIn("No historical data found for symbol", str(context.exception))

    @patch("socket.socket")
    def test_fetch_ohlcv_integration(self, mock_socket_class):
        """Test full fetch_ohlcv stream parsing flow with socket mocks."""
        mock_socket = MagicMock()
        mock_socket_class.return_return_value = mock_socket
        
        # Mock receiving the handshake/data response in chunks
        mock_socket.recv.side_effect = [
            b"2026-06-01 12:00:00,105.5,104.2,105.0,105.2,100000,10000,50\r\n",
            b"!ENDMSG!\r\n",
            b""  # empty to break loop
        ]
        
        # Mock class instantiation to return our mocked socket
        with patch("socket.socket") as mock_sock_init:
            mock_sock_init.return_value = mock_socket
            parsed = self.client.fetch_ohlcv("AAPL", "1m", 1)
            
            # Verify handshake and query were sent
            mock_socket.sendall.assert_any_call(b"S,SET PROTOCOL,6.2\r\n")
            mock_socket.sendall.assert_any_call(unittest.mock.ANY)
            
            # Verify parsing
            self.assertEqual(len(parsed), 1)
            self.assertEqual(parsed[0][1], 105.0)  # Open
            self.assertEqual(parsed[0][4], 105.2)  # Close
