"""
Price streaming service that subscribes to real-time price updates for all supported instruments
and publishes them to Redis streams for consumption by other services.
"""

import os
import sys
import time

import redis
from hyperliquid.utils import constants

# Import shared modules
from shared.config import Config
from shared.hyperliquid_client import get_exchange_instruments, setup_hyperliquid_client
from shared.opentelemetry_config import get_tracer

# Force stdout to be unbuffered for Docker logs
sys.stdout.reconfigure(line_buffering=True)

# Get tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "price_stream_producer"))


def get_redis_connection():
    """Creates and returns a new Redis connection."""
    config = Config()
    return redis.Redis(
        host=config.get("redis.host"),
        port=config.get("redis.port"),
        db=config.get("redis.db"),
        decode_responses=True,
    )


def publish_price_update(symbol: str, price: float, timestamp: float):
    """
    Publish a price update to the Redis stream.

    Args:
        symbol: Trading symbol (e.g., 'BTC/USDC:USDC')
        price: Current price
        timestamp: Unix timestamp of the price update
    """
    with tracer.start_as_current_span("publish_price_update") as span:
        try:
            config = Config()
            r = get_redis_connection()
            stream_name = config.get("redis.streams.price_updates")

            event_data = {
                "symbol": symbol,
                "price": str(price),  # Convert to string to avoid precision issues
                "timestamp": str(timestamp),
            }

            r.xadd(stream_name, event_data)

            span.set_attribute("redis.stream.name", stream_name)
            span.set_attribute("price.symbol", symbol)
            span.set_attribute("price.value", price)

            print(f"{timestamp},{symbol},{price}", flush=True)

        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error publishing price update for {symbol}: {e}", flush=True)
            raise


def process_trade(data):
    """
    Process incoming trade data and publish to Redis stream.

    Args:
        data: Trade data from HyperLiquid WebSocket
    """
    try:
        if not data.get("data") or len(data["data"]) == 0:
            return

        trade = data["data"][0]
        coin = trade.get("coin")
        price = float(trade.get("px", 0))

        if not coin or price <= 0:
            return

        # Format symbol to match database convention
        symbol = f"{coin}/USDC:USDC"
        timestamp = time.time()

        publish_price_update(symbol, price, timestamp)

    except Exception as e:
        print(f"Error processing trade data: {e}", flush=True)


def start_price_streaming():
    """
    Start the price streaming service by subscribing to all supported instruments.
    Includes a heartbeat mechanism to detect stale connections.
    """
    with tracer.start_as_current_span("start_price_streaming") as span:
        print("Starting price streaming service...", flush=True)

        last_message_time = time.time()
        STALE_CONNECTION_THRESHOLD = 60  # seconds

        def process_trade_wrapper(data):
            nonlocal last_message_time
            process_trade(data)
            last_message_time = time.time()

        try:
            # Setup HyperLiquid client
            address, info, _ = setup_hyperliquid_client(constants.MAINNET_API_URL)
            span.set_attribute("hyperliquid.address", address)

            # Get all supported instruments from database
            instruments = get_exchange_instruments()
            span.set_attribute("instruments.count", len(instruments))

            print(
                f"Subscribing to price feeds for {len(instruments)} instruments: {', '.join(instruments)}",
                flush=True,
            )

            # Subscribe to trade feeds for all instruments
            for instrument in instruments:
                try:
                    info.subscribe(
                        {"type": "trades", "coin": instrument}, process_trade_wrapper
                    )
                    print(f"Subscribed to {instrument} trade feed", flush=True)
                    span.add_event(f"Subscribed to {instrument}")
                except Exception as e:
                    print(f"Failed to subscribe to {instrument}: {e}", flush=True)
                    span.add_event(f"Failed to subscribe to {instrument}: {e}")

            print("Price streaming service is running...", flush=True)
            span.add_event("Price streaming service started successfully")

            # Keep the service running and check for stale connection
            while True:
                if time.time() - last_message_time > STALE_CONNECTION_THRESHOLD:
                    raise TimeoutError(
                        "Stale WebSocket connection: No messages received for "
                        f"{STALE_CONNECTION_THRESHOLD} seconds."
                    )
                time.sleep(1)

        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in price streaming service: {e}", flush=True)
            # Re-raise the exception to be caught by the retry loop
            raise


def run_price_streamer_with_retry():
    """
    Run the price streaming service with a retry mechanism to handle connection drops.
    """
    while True:
        try:
            start_price_streaming()
        except Exception as e:
            print(
                f"An unexpected error occurred: {e}. Restarting in 5 seconds...",
                flush=True,
            )
            time.sleep(5)


if __name__ == "__main__":
    run_price_streamer_with_retry()
