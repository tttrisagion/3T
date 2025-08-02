"""
Resilient exchange client manager with connection pooling, retry logic, and health checks.
Prevents network connection buildup and handles transient network failures.
"""

import threading
import time
from datetime import datetime

import ccxt
from opentelemetry import trace

from shared.config import config
from shared.network_monitor import network_monitor

tracer = trace.get_tracer(__name__)


class ExchangeManager:
    """
    Singleton manager for exchange connections with built-in resilience:
    - Connection reuse and pooling
    - Automatic retry with exponential backoff
    - Health checks and connection refresh
    - Circuit breaker pattern for network failures
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._exchanges = {}
        self._last_health_check = {}
        self._circuit_breaker_state = {}
        self._failure_count = {}
        self._last_failure_time = {}
        self._lock = threading.Lock()

        # Configuration
        self.health_check_interval = 300  # 5 minutes
        self.connection_timeout = 30
        self.max_retries = 3
        self.circuit_breaker_threshold = 5
        self.circuit_breaker_reset_time = 300  # 5 minutes

        self._initialized = True

    def get_exchange(self, exchange_name: str = "hyperliquid") -> ccxt.Exchange:
        """
        Get a healthy exchange instance with automatic connection management.
        Includes retry logic for transient network errors during health checks.

        Args:
            exchange_name: Name of the exchange (default: hyperliquid)

        Returns:
            ccxt.Exchange: Healthy exchange instance

        Raises:
            Exception: If circuit breaker is open or all retries are exhausted
            ValueError: If required secrets are missing
        """
        with tracer.start_as_current_span("get_exchange") as span:
            span.set_attribute("exchange.name", exchange_name)

            last_exception = None
            for attempt in range(self.max_retries):
                try:
                    with self._lock:
                        # Check circuit breaker first
                        if self._is_circuit_breaker_open(exchange_name):
                            raise Exception(
                                f"Circuit breaker open for {exchange_name} exchange"
                            )

                        # Get or create exchange instance
                        exchange = self._get_or_create_exchange(exchange_name)

                        # Health check if needed
                        if self._needs_health_check(exchange_name):
                            span.add_event("Performing health check")
                            if not self._health_check(exchange, exchange_name):
                                span.add_event(
                                    "Health check failed, recreating exchange"
                                )
                                exchange = self._recreate_exchange(exchange_name)

                        span.set_attribute("exchange.healthy", True)
                        return exchange

                except ValueError as e:
                    # Do not retry on configuration errors
                    raise e
                except Exception as e:
                    last_exception = e
                    span.add_event(
                        f"Attempt {attempt + 1}/{self.max_retries} to get exchange failed: {e}"
                    )
                    if attempt < self.max_retries - 1:
                        delay = 2**attempt
                        span.add_event(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        span.add_event("All retries failed.")
                        break  # Exit loop after last attempt

            # If all retries failed, raise the last captured exception
            raise last_exception

    def _get_or_create_exchange(self, exchange_name: str) -> ccxt.Exchange:
        """Get existing exchange or create new one."""
        if exchange_name not in self._exchanges:
            self._exchanges[exchange_name] = self._create_exchange(exchange_name)
            self._last_health_check[exchange_name] = datetime.now()

        return self._exchanges[exchange_name]

    def _create_exchange(self, exchange_name: str) -> ccxt.Exchange:
        """Create a new exchange instance with proper configuration."""
        with tracer.start_as_current_span("create_exchange") as span:
            span.set_attribute("exchange.name", exchange_name)

            if exchange_name == "hyperliquid":
                api_key = config.get_secret("exchanges.hyperliquid.apiKey")
                wallet_address = config.get_secret(
                    "exchanges.hyperliquid.walletAddress"
                )
                private_key = config.get_secret("exchanges.hyperliquid.privateKey")

                if not all([api_key, wallet_address, private_key]):
                    missing = [
                        k
                        for k, v in {
                            "apiKey": api_key,
                            "walletAddress": wallet_address,
                            "privateKey": private_key,
                        }.items()
                        if not v
                    ]
                    raise ValueError(
                        f"Missing HyperLiquid secrets: {', '.join(missing)}"
                    )

                # Get proxy configuration
                proxy = config.get("exchanges.hyperliquid.proxy")
                origin = config.get("exchanges.hyperliquid.origin")

                exchange_config = {
                    "apiKey": api_key,
                    "walletAddress": wallet_address,
                    "privateKey": private_key,
                    "timeout": self.connection_timeout * 1000,  # CCXT uses milliseconds
                    "enableRateLimit": True,
                    "rateLimit": 100,  # Be conservative with rate limiting
                    "options": {
                        "defaultType": "swap",  # For perpetual futures
                        "createMarketBuyOrderRequiresPrice": False,
                    },
                }

                # Add proxy configuration if specified
                if proxy:
                    exchange_config["proxy"] = proxy
                    span.add_event(f"Using proxy: {proxy}")

                # Add origin if specified
                if origin:
                    exchange_config["origin"] = origin
                    span.add_event(f"Using origin: {origin}")

                exchange = ccxt.hyperliquid(exchange_config)

                span.add_event("Created HyperLiquid exchange instance")
                return exchange
            else:
                raise ValueError(f"Unsupported exchange: {exchange_name}")

    def _needs_health_check(self, exchange_name: str) -> bool:
        """Check if health check is needed based on time interval."""
        if exchange_name not in self._last_health_check:
            return True

        elapsed = datetime.now() - self._last_health_check[exchange_name]
        return elapsed.total_seconds() > self.health_check_interval

    def _health_check(self, exchange: ccxt.Exchange, exchange_name: str) -> bool:
        """
        Perform health check on exchange connection.

        Args:
            exchange: Exchange instance to check
            exchange_name: Name of the exchange

        Returns:
            bool: True if healthy, False otherwise
        """
        with tracer.start_as_current_span("exchange_health_check") as span:
            try:
                start_time = time.time()
                # Health check - try different methods based on exchange capabilities
                if exchange_name == "hyperliquid":
                    # For HyperLiquid, check markets to verify connection
                    exchange.load_markets()
                else:
                    # For other exchanges, use server time
                    exchange.fetch_time()
                duration = time.time() - start_time

                # Record successful request latency
                network_monitor.record_network_latency(exchange_name, duration)

                self._last_health_check[exchange_name] = datetime.now()
                self._reset_failure_count(exchange_name)
                span.set_attribute("health_check.result", "healthy")
                return True

            except Exception as e:
                span.set_attribute("health_check.result", "failed")
                span.record_exception(e)

                # Record network error for monitoring
                error_type = self._classify_error(e)
                network_monitor.record_network_error(exchange_name, error_type, str(e))

                self._record_failure(exchange_name)
                print(f"Health check failed for {exchange_name}: {e}")
                return False

    def _recreate_exchange(self, exchange_name: str) -> ccxt.Exchange:
        """Recreate exchange instance after health check failure."""
        with tracer.start_as_current_span("recreate_exchange") as span:
            span.set_attribute("exchange.name", exchange_name)

            # Remove old instance
            if exchange_name in self._exchanges:
                old_exchange = self._exchanges[exchange_name]
                try:
                    # Try to close old connection gracefully
                    if hasattr(old_exchange, "close"):
                        old_exchange.close()
                except Exception:
                    pass  # Ignore errors when closing
                del self._exchanges[exchange_name]

            # Create new instance
            new_exchange = self._create_exchange(exchange_name)
            self._exchanges[exchange_name] = new_exchange
            self._last_health_check[exchange_name] = datetime.now()

            span.add_event("Exchange recreated successfully")
            return new_exchange

    def _is_circuit_breaker_open(self, exchange_name: str) -> bool:
        """Check if circuit breaker is open for this exchange."""
        if exchange_name not in self._circuit_breaker_state:
            return False

        if not self._circuit_breaker_state[exchange_name]:
            return False

        # Check if reset time has passed
        if exchange_name in self._last_failure_time:
            elapsed = datetime.now() - self._last_failure_time[exchange_name]
            if elapsed.total_seconds() > self.circuit_breaker_reset_time:
                self._circuit_breaker_state[exchange_name] = False
                self._failure_count[exchange_name] = 0
                return False

        return True

    def _record_failure(self, exchange_name: str):
        """Record a failure and potentially open circuit breaker."""
        self._failure_count[exchange_name] = (
            self._failure_count.get(exchange_name, 0) + 1
        )
        self._last_failure_time[exchange_name] = datetime.now()

        if self._failure_count[exchange_name] >= self.circuit_breaker_threshold:
            was_closed = not self._circuit_breaker_state.get(exchange_name, False)
            self._circuit_breaker_state[exchange_name] = True

            # Record circuit breaker state change
            if was_closed:
                network_monitor.record_circuit_breaker_state(exchange_name, True)

            print(
                f"Circuit breaker opened for {exchange_name} after {self._failure_count[exchange_name]} failures"
            )

    def _reset_failure_count(self, exchange_name: str):
        """Reset failure count after successful operation."""
        was_open = self._circuit_breaker_state.get(exchange_name, False)
        self._failure_count[exchange_name] = 0
        self._circuit_breaker_state[exchange_name] = False

        # Record circuit breaker closing
        if was_open:
            network_monitor.record_circuit_breaker_state(exchange_name, False)

    def execute_with_retry(self, operation, *args, **kwargs):
        """
        Execute an exchange operation with automatic retry and exponential backoff.

        Args:
            operation: Function to execute (e.g., exchange.fetchOHLCV)
            *args, **kwargs: Arguments to pass to the operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retries are exhausted
        """
        with tracer.start_as_current_span("execute_with_retry") as span:
            span.set_attribute(
                "operation.name",
                operation.__name__
                if hasattr(operation, "__name__")
                else str(operation),
            )
            span.set_attribute("retry.max_attempts", self.max_retries)

            last_exception = None

            for attempt in range(self.max_retries):
                try:
                    span.set_attribute("retry.attempt", attempt + 1)
                    result = operation(*args, **kwargs)
                    span.set_attribute("retry.success", True)
                    return result

                except Exception as e:
                    last_exception = e
                    span.add_event(f"Attempt {attempt + 1} failed: {str(e)}")

                    if attempt < self.max_retries - 1:
                        # Exponential backoff: 1s, 2s, 4s
                        delay = 2**attempt
                        span.add_event(f"Retrying in {delay} seconds")
                        time.sleep(delay)
                    else:
                        span.set_attribute("retry.success", False)
                        span.record_exception(e)

            raise last_exception

    def _classify_error(self, error: Exception) -> str:
        """
        Classify network errors for monitoring purposes.

        Args:
            error: The exception that occurred

        Returns:
            str: Error classification
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        if "timeout" in error_str or "timeout" in error_type:
            return "connection_timeout"
        elif "connection refused" in error_str or "connectionrefused" in error_type:
            return "connection_refused"
        elif "network unreachable" in error_str or "network" in error_str:
            return "network_unreachable"
        elif "ssl" in error_str or "certificate" in error_str:
            return "ssl_error"
        elif "dns" in error_str or "name resolution" in error_str:
            return "dns_resolution"
        elif "rate limit" in error_str or "429" in error_str:
            return "rate_limit"
        else:
            return "unknown_network_error"


# Global instance
exchange_manager = ExchangeManager()
