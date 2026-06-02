"""
Resilient DTN IQFeed historical lookup client.
Connects directly to IQFeed's Lookup/Historical port (configured as 9100) via raw TCP,
sends historical queries, and parses the CSV response into standard 3T OHLCV format.
"""

import logging
import socket
import time
from datetime import datetime, UTC

from shared.config import config

logger = logging.getLogger(__name__)


class IQFeedClient:
    """
    Client for DTN IQFeed historical data lookup over port 9100.
    Direct socket protocol integration.
    """

    def __init__(self):
        self.host = config.get("exchanges.tradfi.iqfeed.host", "127.0.0.1")
        self.port = int(config.get("exchanges.tradfi.iqfeed.port", 9100))
        self.timeout = 15  # seconds
        self.buffer_size = 4096

    def fetch_ohlcv(self, symbol: str, timeframe: str, lookback: int) -> list[tuple]:
        """
        Fetch historical interval OHLCV data for a given symbol.
        
        Args:
            symbol: Ticker symbol (e.g., AAPL)
            timeframe: Standard 3T timeframe (e.g., '1m', '4h')
            lookback: Number of historical bars to fetch
            
        Returns:
            list[tuple]: List of (timestamp_ms, open, high, low, close, volume)
        """
        # Convert standard 3T timeframe string to seconds
        interval_seconds = self._timeframe_to_seconds(timeframe)
        
        # Connect to socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        
        try:
            logger.info(f"Connecting to DTN IQFeed historical server at {self.host}:{self.port}...")
            s.connect((self.host, self.port))
            
            # 1. Handshake - set protocol to 6.2 (standard, highly compatible)
            s.sendall(b"S,SET PROTOCOL,6.2\r\n")
            
            # 2. Construct historical command using standard 'HIT' (Historical Interval Triangulation)
            # Command format: HIT,[Symbol],[Interval],[StartDateTime],[EndDateTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend]
            # IQFeed's HIT command requires either BeginDateTime or EndDateTime to be specified.
            # Formatting current UTC time as YYYYMMDD HHMMSS ensures we fetch the latest valid data.
            now_str = datetime.now(UTC).strftime("%Y%m%d %H%M%S")
            command = f"HIT,{symbol},{interval_seconds},,{now_str},{lookback},,,0,,\r\n"
            logger.info(f"Sending IQFeed query: {command.strip()}")
            s.sendall(command.encode("ascii"))
            
            # 3. Read raw stream
            response_data = ""
            while True:
                chunk = s.recv(self.buffer_size).decode("ascii", errors="ignore")
                if not chunk:
                    break
                response_data += chunk
                
                # Check for end of message sentinel
                if "!ENDMSG!" in chunk:
                    break
            
            # 4. Parse response
            return self._parse_response(response_data)
            
        except Exception as e:
            logger.error(f"IQFeed fetch failed for {symbol} ({timeframe}): {e}")
            raise
        finally:
            s.close()

    def _timeframe_to_seconds(self, timeframe: str) -> int:
        """Convert standard timeframes (e.g. '1m', '4h') to seconds."""
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        if unit == "m":
            return value * 60
        elif unit == "h":
            return value * 3600
        elif unit == "d":
            return value * 86400
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

    def _parse_response(self, raw_data: str) -> list[tuple]:
        """
        Parse DTN IQFeed lookup response.
        Response contains lines of comma-separated values.
        
        Format for HIT interval data:
            [RequestID],DateTime,High,Low,Open,Close,TotVolume,PeriodVolume,NumberTrades
        """
        ohlcv_list = []
        lines = raw_data.split("\r\n")
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Sentinel for end of stream
            if "!ENDMSG!" in line:
                break
                
            # Handle server-side errors
            if line.startswith("E,"):
                parts = line.split(",")
                error_msg = parts[2] if len(parts) > 2 else line
                raise ValueError(f"IQFeed returned error: {error_msg}")
                
            parts = line.split(",")
            # In HIT format, DTN protocol 6.x returns:
            # [RequestID], DateTime, High, Low, Open, Close, TotVolume, PeriodVolume, NumberTrades
            # Let's dynamically detect if the date is at index 0 or index 1 (meaning Request ID like 'LH' is present)
            if len(parts) < 7:
                continue
                
            try:
                # Detect shifted fields due to RequestID (contains '-' in date like '2026-06-01')
                if len(parts) >= 8 and "-" in parts[1]:
                    date_str = parts[1].strip()
                    high = float(parts[2])
                    low = float(parts[3])
                    open_px = float(parts[4])
                    close = float(parts[5])
                    volume = float(parts[7])  # PeriodVolume is accurate for the bar volume
                else:
                    date_str = parts[0].strip()
                    high = float(parts[1])
                    low = float(parts[2])
                    open_px = float(parts[3])
                    close = float(parts[4])
                    volume = float(parts[6])
                
                # Parse date string: 'YYYY-MM-DD HH:MM:SS'
                # DTN IQFeed historical timestamps are returned in Exchange Local Time (Eastern Time).
                # We must localize to America/New_York and convert to UTC before getting the timestamp.
                from zoneinfo import ZoneInfo
                naive_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                localized_dt = naive_dt.replace(tzinfo=ZoneInfo("America/New_York"))
                utc_dt = localized_dt.astimezone(ZoneInfo("UTC"))
                timestamp_ms = int(utc_dt.timestamp() * 1000)
                
                ohlcv_list.append((timestamp_ms, open_px, high, low, close, volume))
            except Exception as e:
                # Handle potential parsing issues with individual rows gracefully
                logger.warning(f"Failed to parse IQFeed row '{line}': {e}")
                continue
                
        # Re-sort to guarantee oldest to newest
        ohlcv_list.sort(key=lambda x: x[0])
        logger.info(f"Successfully parsed {len(ohlcv_list)} bars from IQFeed.")
        return ohlcv_list


# Global client instance
iqfeed_client = IQFeedClient()
