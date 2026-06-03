"""
Data Gate for Providence - Redis Implementation
"""

import logging

from shared.database import get_redis_connection

logger = logging.getLogger(__name__)


def get_price_from_redis(symbol: str) -> float | None:
    """Gets the latest price for a symbol from Redis stream with dynamic 10-minute staleness checks."""
    try:
        with get_redis_connection(decode_responses=True) as redis_conn:
            # First try the simple key (if feed task is running)
            price = redis_conn.get(f"price:{symbol}")
            if price is not None:
                return float(price)

            # Fallback: Read directly from the price stream
            stream_name = "prices:updated"
            messages = redis_conn.xrevrange(stream_name, count=100)

            for _, msg_data in messages:
                if msg_data.get("symbol") == symbol:
                    price_str = msg_data.get("price")
                    if price_str:
                        # Extract and validate timestamp for staleness
                        ts_str = msg_data.get("timestamp")
                        if ts_str:
                            try:
                                import time
                                ts = float(ts_str)
                                # Dynamically convert millisecond timestamps to seconds
                                if ts > 5000000000:
                                    ts /= 1000.0
                                
                                age = time.time() - ts
                                if age > 600:  # 10 minutes max age
                                    logger.warning(
                                        f"Price update for {symbol} is too stale ({age:.1f}s old, limit 600s). "
                                        f"Pausing Providence iterations."
                                    )
                                    return None
                            except Exception as ts_err:
                                logger.warning(f"Failed to parse timestamp '{ts_str}' for {symbol}: {ts_err}")
                        
                        return float(price_str)

            return None

    except Exception as e:
        logger.error(f"Error reading price from Redis for {symbol}: {e}")
        return None


def get_volatility_from_redis(symbol: str) -> float | None:
    """Gets the latest volatility for a symbol from Redis stream."""
    try:
        with get_redis_connection(decode_responses=True) as redis_conn:
            # First try the simple key (if volatility task is running)
            volatility = redis_conn.get(f"volatility:{symbol}")
            if volatility is not None:
                return float(volatility)

            # Fallback: Read directly from the volatility stream
            stream_name = "volatility:updated"
            messages = redis_conn.xrevrange(stream_name, count=100)

            for _, msg_data in messages:
                if msg_data.get("symbol") == symbol:
                    vol_str = msg_data.get("volatility")
                    if vol_str:
                        return float(vol_str)

            return None

    except Exception as e:
        logger.error(f"Error reading volatility from Redis for {symbol}: {e}")
        return None
