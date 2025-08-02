#!/usr/bin/env python3
"""
Automatically selects and starts the appropriate price producer based on configuration.
- If proxy is configured: starts polling-based producer
- If no proxy: starts WebSocket-based producer
"""

import subprocess
import sys

from shared.config import config


def main():
    # Check if proxy is configured
    proxy = config.get("exchanges.hyperliquid.proxy")

    if proxy:
        print(f"Proxy configured ({proxy}), starting polling-based price producer...")
        script = "price_poll_producer.py"
    else:
        print("No proxy configured, starting WebSocket-based price producer...")
        script = "price_stream_producer.py"

    # Start the appropriate producer
    subprocess.run([sys.executable, f"src/{script}"])


if __name__ == "__main__":
    main()
