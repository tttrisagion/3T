"""
Polling-based price producer for environments where WebSocket connections
are not available (e.g., when using certain proxy configurations).
Falls back to periodic REST API calls to fetch latest prices.
"""

import asyncio
import time

import redis
from opentelemetry import trace

from shared.config import config
from shared.exchange_manager import exchange_manager
from shared.hyperliquid_client import get_exchange_instruments

tracer = trace.get_tracer(__name__)


class PricePollProducer:
    def __init__(self):
        # Redis configuration
        redis_config = config.get("redis")
        self.redis_client = redis.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            db=redis_config["db"],
            decode_responses=True,
        )
        self.stream_key = redis_config["streams"]["price_updates"]

        print("Polling producer: Redis configured.")

        # Polling configuration
        self.poll_interval = config.get(
            "exchanges.hyperliquid.poll_interval", 1.0
        )  # Default 1 second
        self.batch_size = config.get(
            "exchanges.hyperliquid.poll_batch_size", 10
        )  # Fetch multiple symbols at once

        print("Polling producer: Polling configured.")

        # Get exchange instance with timeout handling
        try:
            print("Polling producer: Attempting to get exchange instance...")
            self.exchange = exchange_manager.get_exchange("hyperliquid")
            print("Polling producer: Exchange manager initialized.")
        except Exception as e:
            print(f"Polling producer: Failed to initialize exchange: {e}")
            print("This might be due to proxy connection issues.")
            raise

    def fetch_latest_prices(self, symbols):
        """
        Fetch latest prices for given symbols using REST API.
        Uses fetch_tickers() to get all prices in one call for better performance.
        Returns dict of symbol -> price
        """
        with tracer.start_as_current_span("fetch_latest_prices") as span:
            span.set_attribute("symbols.count", len(symbols))

            prices = {}

            try:
                # Fetch all tickers at once for much better performance
                print(f"Fetching tickers for {len(symbols)} symbols...")
                tickers = exchange_manager.execute_with_retry(
                    self.exchange.fetch_tickers, symbols
                )

                for symbol in symbols:
                    if symbol in tickers:
                        ticker = tickers[symbol]
                        if ticker and "last" in ticker and ticker["last"] is not None:
                            prices[symbol] = ticker["last"]
                            span.add_event(
                                f"Fetched price for {symbol}: {ticker['last']}"
                            )
                        else:
                            print(f"No price data available for {symbol}")
                    else:
                        print(f"Symbol {symbol} not found in ticker response")

            except Exception as e:
                print(f"Error fetching tickers: {e}")
                span.record_exception(e)

                # Fallback to individual ticker fetches if batch fails
                print("Falling back to individual ticker fetches...")
                for symbol in symbols[:5]:  # Limit to first 5 symbols for fallback
                    try:
                        ticker = exchange_manager.execute_with_retry(
                            self.exchange.fetch_ticker, symbol
                        )

                        if ticker and "last" in ticker and ticker["last"] is not None:
                            prices[symbol] = ticker["last"]

                    except Exception as e:
                        print(f"Error fetching price for {symbol}: {e}")

            span.set_attribute("prices.fetched", len(prices))
            print(f"Successfully fetched prices for {len(prices)} symbols")
            return prices

    def publish_prices(self, prices):
        """
        Publish prices to Redis stream in the same format as WebSocket producer.
        """
        with tracer.start_as_current_span("publish_prices") as span:
            span.set_attribute("prices.count", len(prices))

            timestamp = time.time()  # Unix timestamp as float
            published_count = 0

            for symbol, price in prices.items():
                try:
                    # Format message same as WebSocket producer
                    message = {
                        "timestamp": str(timestamp),  # Convert to string for Redis
                        "symbol": symbol,
                        "price": str(price),
                        "source": "polling",  # Indicate this came from polling
                    }

                    # Publish to Redis stream
                    self.redis_client.xadd(self.stream_key, message)
                    print(f"Published price update for {symbol}: {price}")
                    published_count += 1

                except Exception as e:
                    print(f"Error publishing price for {symbol}: {e}")
                    span.record_exception(e)

            span.set_attribute("prices.published", published_count)
            print(f"Published {published_count} price updates via polling")

    async def run(self):
        """
        Main polling loop.
        """
        print("Starting polling-based price producer...")
        print(f"Poll interval: {self.poll_interval} seconds")
        print(f"Batch size: {self.batch_size} symbols")

        # Get all trading instruments
        try:
            print("Polling producer: Fetching instrument list from database...")
            instruments = get_exchange_instruments()
            symbols = [f"{inst}/USDC:USDC" for inst in instruments]
            print(f"Monitoring {len(symbols)} symbols: {symbols}")
        except Exception as e:
            print(f"Polling producer: Failed to get instruments: {e}")
            # Fallback to BTC if database fetch fails
            symbols = ["BTC/USDC:USDC"]
            print(f"Falling back to BTC only: {symbols}")

        while True:
            try:
                with tracer.start_as_current_span("poll_cycle"):
                    start_time = time.time()

                    # Fetch latest prices
                    prices = self.fetch_latest_prices(symbols)

                    # Publish to Redis
                    if prices:
                        self.publish_prices(prices)

                    # Calculate time taken and sleep for remainder of interval
                    elapsed = time.time() - start_time
                    sleep_time = max(0, self.poll_interval - elapsed)

                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                    else:
                        print(
                            f"Warning: Poll cycle took {elapsed:.2f}s, longer than interval {self.poll_interval}s"
                        )

            except Exception as e:
                print(f"Error in polling loop: {e}")
                tracer.get_current_span().record_exception(e)
                # Wait before retrying
                await asyncio.sleep(self.poll_interval)


async def main():
    """
    Entry point for the polling price producer.
    """
    producer = PricePollProducer()
    await producer.run()


if __name__ == "__main__":
    print("=== POLLING PRODUCER STARTING ===")
    print("Initializing polling-based price producer...")
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"FATAL ERROR in polling producer: {e}")
        import traceback

        traceback.print_exc()
        raise
