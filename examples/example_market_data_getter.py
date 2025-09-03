#!/usr/bin/env python3
import struct
import sys
import time
from multiprocessing import resource_tracker, shared_memory

import numpy as np

# Must match setter.py constants
MAX_SYMBOLS = 1000
ENTRY_SIZE = 24
HEADER_SIZE = 8
TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)
SHM_NAME = "market_data_shm"

# Staleness threshold
MAX_AGE_SECONDS = 0.5


def symbol_to_hash(symbol):
    """Convert symbol string to 64-bit hash for fast lookup"""
    import hashlib

    # Use SHA256 for deterministic hashing across processes
    hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
    # Take first 8 bytes as 64-bit integer
    return int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF


def get_volatility(symbol, max_retries=None):
    """
    Get volatility for a symbol from shared memory.
    Returns (volatility, timestamp) if found and fresh, otherwise retries.
    """
    symbol_hash = symbol_to_hash(symbol)
    retry_count = 0

    while max_retries is None or retry_count < max_retries:
        try:
            # Connect to existing shared memory
            shm = shared_memory.SharedMemory(name=SHM_NAME)

            # CRITICAL: Unregister from resource tracker to prevent cleanup
            # This prevents the shared memory from being destroyed when getter exits
            resource_tracker.unregister(shm._name, "shared_memory")

            try:
                # Create numpy array view
                buffer = np.ndarray((TOTAL_SIZE,), dtype=np.uint8, buffer=shm.buf)

                # Read number of symbols
                num_symbols = buffer[:HEADER_SIZE].view(np.uint64)[0]

                # Search for our symbol
                current_time = time.time()

                for i in range(min(num_symbols, MAX_SYMBOLS)):
                    offset = HEADER_SIZE + (i * ENTRY_SIZE)

                    # Unpack entry
                    entry_data = buffer[offset : offset + ENTRY_SIZE].tobytes()
                    stored_hash, volatility, timestamp = struct.unpack("<Qdd", entry_data)

                    if stored_hash == symbol_hash:
                        # Check staleness
                        age = current_time - timestamp
                        if age <= MAX_AGE_SECONDS:
                            return volatility, timestamp, age
                        else:
                            print(
                                f"Volatility for {symbol} is stale (age: {age:.1f}s), retrying..."
                            )
                            break

                print(f"Symbol {symbol} not found in shared memory, retrying...")

            finally:
                shm.close()

        except FileNotFoundError:
            print("Shared memory not found, setter may not be running. Retrying...")
        except Exception as e:
            print(f"Error reading shared memory: {e}")

        retry_count += 1
        if max_retries is None or retry_count < max_retries:
            print(f"Sleeping 30 seconds before retry {retry_count}...")
            time.sleep(30)

    raise TimeoutError(
        f"Could not get fresh volatility for {symbol} after {retry_count} attempts"
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: python getter.py <symbol> [--once]")
        print("Example: python getter.py 'BTC/USDC:USDC'")
        print("Add --once to get volatility once and exit")
        sys.exit(1)

    symbol = sys.argv[1]
    once = "--once" in sys.argv

    print(f"Getting volatility for symbol: {symbol}")
    print(f"Symbol hash: {symbol_to_hash(symbol)}")
    print(f"Max age allowed: {MAX_AGE_SECONDS} seconds")
    print()

    try:
        while True:
            try:
                volatility, timestamp, age = get_volatility(symbol)
                print(f"Symbol: {symbol}")
                print(f"Volatility: {volatility:,.5f}")
                print(f"Timestamp: {timestamp:.3f} (age: {age:.1f}s)")
                print(
                    f"Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}"
                )
                print("-" * 50)

                if once:
                    break

                # Wait a bit before next read
                time.sleep(1)

            except TimeoutError as e:
                print(f"Error: {e}")
                if once:
                    sys.exit(1)

    except KeyboardInterrupt:
        print("\nShutting down getter...")


if __name__ == "__main__":
    main()
