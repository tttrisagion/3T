"""
Standalone test script to cancel all open orders on HyperLiquid.

Uses the same exchange_manager as production to verify the approach
before integrating into the reconciliation engine.

Usage:
    python3 scripts/test_cancel_all.py
"""

import os
import sys

# Add project paths so shared modules can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "celery-services"))

from shared.config import config
from shared.exchange_manager import exchange_manager


def cancel_all_open_orders():
    symbols = config.get("reconciliation_engine.symbols", [])
    if not symbols:
        print("No symbols configured in reconciliation_engine.symbols")
        return

    exchange = exchange_manager.get_exchange()
    print(f"Checking open orders for {len(symbols)} symbols: {symbols}")

    for symbol in symbols:
        try:
            open_orders = exchange.fetch_open_orders(symbol)
            if not open_orders:
                print(f"  {symbol}: no open orders")
                continue

            order_ids = [order["id"] for order in open_orders]
            print(f"  {symbol}: found {len(order_ids)} open orders: {order_ids}")

            exchange.cancel_orders(order_ids, symbol)
            print(f"  {symbol}: cancelled {len(order_ids)} orders")

        except Exception as e:
            print(f"  {symbol}: error - {e}")


if __name__ == "__main__":
    cancel_all_open_orders()
