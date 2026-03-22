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

        # Exchange will be fetched in the run loop to ensure failover resilience.
        print("Polling producer: Exchange manager will be initialized in the run loop.")

    def _fetch_ticker_group(self, exchange, symbols, params, span):
        """Fetch tickers for a group of symbols with the given params."""
        prices = {}
        try:
            tickers = exchange_manager.execute_with_retry(
                exchange.fetch_tickers, symbols, params
            )
            for symbol in symbols:
                if symbol in tickers:
                    ticker = tickers[symbol]
                    if ticker and "last" in ticker and ticker["last"] is not None:
                        prices[symbol] = ticker["last"]
                        span.add_event(f"Fetched price for {symbol}: {ticker['last']}")
                    else:
                        print(f"No price data available for {symbol}")
                else:
                    print(f"Symbol {symbol} not found in ticker response")
        except Exception as e:
            print(f"Error fetching tickers ({params}): {e}")
            span.record_exception(e)
        return prices

    def fetch_latest_prices(self, exchange, symbols, hip3_symbols):
        """
        Fetch latest prices for given symbols using REST API.
        Uses fetch_tickers() to get all prices in one call for better performance.
        Returns dict of symbol -> price
        """
        with tracer.start_as_current_span("fetch_latest_prices") as span:
            span.set_attribute("symbols.count", len(symbols) + len(hip3_symbols))

            prices = {}

            # Fetch native perps with explicit type to avoid fetching all market types
            if symbols:
                print(f"Fetching tickers for {len(symbols)} swap symbols...")
                prices.update(
                    self._fetch_ticker_group(exchange, symbols, {"type": "swap"}, span)
                )

            # Fetch HIP-3 symbols separately
            if hip3_symbols:
                print(f"Fetching tickers for {len(hip3_symbols)} HIP-3 symbols...")
                prices.update(
                    self._fetch_ticker_group(
                        exchange, hip3_symbols, {"hip3": True}, span
                    )
                )

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
                    self.redis_client.xadd(self.stream_key, message, maxlen=10000)
                    print(f"Published price update for {symbol}: {price}")
                    published_count += 1

                except Exception as e:
                    print(f"Error publishing price for {symbol}: {e}")
                    span.record_exception(e)

            span.set_attribute("prices.published", published_count)
            print(f"Published {published_count} price updates via polling")

    async def run(self):
        """
        Main polling loop with exponential backoff for rate limits.
        """
        print("Starting polling-based price producer...")
        print(f"Poll interval: {self.poll_interval} seconds")
        print(f"Batch size: {self.batch_size} symbols")

        symbols = []
        hip3_symbols = []
        last_instrument_fetch_time = 0
        INSTRUMENT_FETCH_INTERVAL = 300  # 5 minutes

        # Backoff state
        current_backoff = 0
        max_backoff = 60  # Maximum backoff of 60 seconds

        while True:
            try:
                # Periodically refresh instrument list from DB
                if (
                    not symbols
                    and not hip3_symbols
                    or (
                        time.time() - last_instrument_fetch_time
                        > INSTRUMENT_FETCH_INTERVAL
                    )
                ):
                    try:
                        print(
                            "Polling producer: (Re)fetching instrument list from database..."
                        )
                        instruments = get_exchange_instruments()
                        if instruments:
                            symbols = []
                            hip3_symbols = []
                            for inst in instruments:
                                name = inst["name"]
                                sym = inst["symbol"]
                                if sym.startswith(f"{name}/"):
                                    symbols.append(sym)
                                else:
                                    hip3_symbols.append(sym)
                            all_syms = symbols + hip3_symbols
                            print(f"Now monitoring {len(all_syms)} symbols: {all_syms}")
                            last_instrument_fetch_time = time.time()
                        else:
                            print(
                                "Polling producer: Received empty instrument list from database."
                            )
                    except Exception as e:
                        print(
                            f"Polling producer: Failed to get instruments: {e}. Will retry."
                        )
                        # If it fails, continue with the old list (or empty) and retry later

                if not symbols and not hip3_symbols:
                    print(
                        "Polling producer: No symbols to monitor. Waiting for instrument list from DB."
                    )
                    await asyncio.sleep(self.poll_interval)
                    continue

                # Get a fresh exchange instance to ensure proxy failover works
                try:
                    exchange = exchange_manager.get_exchange("hyperliquid")
                except Exception as e:
                    if "Circuit breaker open" in str(e):
                        print(
                            f"Polling producer: Circuit breaker open, waiting {self.poll_interval}s..."
                        )
                        await asyncio.sleep(self.poll_interval)
                        continue
                    raise

                with tracer.start_as_current_span("poll_cycle"):
                    start_time = time.time()

                    # Fetch latest prices
                    prices = self.fetch_latest_prices(exchange, symbols, hip3_symbols)

                    # Publish to Redis
                    if prices:
                        self.publish_prices(prices)
                        # Success! Reset backoff
                        current_backoff = 0
                    else:
                        # If no prices were fetched (likely due to errors), consider it a partial failure
                        # but don't apply full exponential backoff unless we get a real exception.
                        pass

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
                error_str = str(e).lower()
                is_rate_limit = "429" in error_str or "rate limit" in error_str

                if is_rate_limit:
                    current_backoff = min(
                        max_backoff, (current_backoff * 2) if current_backoff > 0 else 2
                    )
                    print(
                        f"Polling producer: Rate limited (429). Applying backoff: {current_backoff}s"
                    )
                else:
                    print(f"Polling producer: Error in polling loop: {e}")
                    current_backoff = 2  # Small backoff for other errors

                with tracer.start_as_current_span("polling_error") as error_span:
                    error_span.record_exception(e)
                    error_span.set_attribute("backoff_seconds", current_backoff)

                # Wait for backoff period
                await asyncio.sleep(current_backoff)


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
