#!/usr/bin/env python3
"""
Test script to verify proxy connection with HyperLiquid exchange.
"""

import os
import sys

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)

import pytest  # noqa: E402

from shared.config import config  # noqa: E402
from shared.exchange_manager import exchange_manager  # noqa: E402


def test_proxy_connection():
    """Test the proxy connection by performing various exchange operations."""
    print("\nTesting HyperLiquid connection through proxy...")
    print("-" * 50)

    # Display proxy configuration
    proxy = config.get("exchanges.hyperliquid.proxy")
    origin = config.get("exchanges.hyperliquid.origin")
    print(f"Proxy URL: {proxy}")
    print(f"Origin header: {origin}")
    print("-" * 50)

    try:
        # Get exchange instance (this will use proxy if configured)
        print("1. Getting exchange instance...")
        exchange = exchange_manager.get_exchange("hyperliquid")
        print("✓ Exchange instance created successfully")

        # Test loading markets
        print("\n2. Loading markets...")
        markets = exchange.load_markets()
        print(f"✓ Loaded {len(markets)} markets")

        # Display a few sample markets
        sample_markets = list(markets.keys())[:5]
        print(f"   Sample markets: {', '.join(sample_markets)}")

        # Test fetching ticker data
        print("\n3. Fetching ticker data for BTC/USDC:USDC...")
        ticker = exchange_manager.execute_with_retry(
            exchange.fetch_ticker, "BTC/USDC:USDC"
        )
        if ticker:
            print(f"✓ BTC price: ${ticker.get('last', 'N/A')}")
            print(f"   24h volume: {ticker.get('quoteVolume', 'N/A')} USDC")
            print(
                f"   Bid: ${ticker.get('bid', 'N/A')}, Ask: ${ticker.get('ask', 'N/A')}"
            )

        # Test fetching balance (this will use API key)
        print("\n4. Fetching account balance...")
        try:
            balance = exchange_manager.execute_with_retry(exchange.fetch_balance)
            print("✓ Balance fetched successfully")

            # Display USDC balance if available
            if "USDC" in balance.get("total", {}):
                print(f"   USDC balance: {balance['total']['USDC']}")
        except Exception as e:
            print(
                f"⚠ Balance fetch failed (this is expected if using a proxy that doesn't forward auth): {e}"
            )

        # Test fetching OHLCV data
        print("\n5. Fetching recent OHLCV data for ETH/USDC:USDC...")
        ohlcv = exchange_manager.execute_with_retry(
            exchange.fetchOHLCV, "ETH/USDC:USDC", "1m", limit=5
        )
        if ohlcv:
            print(f"✓ Fetched {len(ohlcv)} candles")
            latest = ohlcv[-1]
            print(f"   Latest candle - Close: ${latest[4]}, Volume: {latest[5]}")

        print("\n" + "=" * 50)
        print("✅ All tests passed! Proxy connection is working correctly.")
        print("=" * 50)

    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        print("\nPossible issues:")
        print("- Proxy server may be down or unreachable")
        print("- Proxy may not be whitelisted for the origin domain")
        print("- Exchange API may be experiencing issues")
        print("- Network connectivity problems")
        pytest.fail(f"Proxy connection test failed: {e}")


@pytest.mark.skipif(
    config.get("exchanges.hyperliquid.proxy") is None,
    reason="No proxy configured - skipping proxy connection test",
)
def test_proxy_required_when_configured():
    """Verify that proxy is being used when configured."""
    proxy = config.get("exchanges.hyperliquid.proxy")
    assert proxy is not None, "This test should only run when proxy is configured"

    # Get exchange instance and verify it's using the proxy
    exchange = exchange_manager.get_exchange("hyperliquid")
    assert hasattr(exchange, "proxy"), "Exchange should have proxy attribute"
    assert exchange.proxy == proxy, f"Expected proxy {proxy}, got {exchange.proxy}"


if __name__ == "__main__":
    # Allow running as standalone script for debugging
    test_proxy_connection()
