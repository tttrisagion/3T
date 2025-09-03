#!/usr/bin/env python3
import struct
import sys
import time
from multiprocessing import shared_memory

import numpy as np
import redis


def get_latest_price(symbol: str):
    """
    Retrieves the latest price for a given symbol from the Redis price stream.

    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC').

    Returns:
        The latest price as a float, or None if not found.
    """
    try:
        redis_host = "192.168.2.215"
        redis_port = 6379
        redis_db = 0
        stream_name = "prices:updated"

        r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )
        r.ping()  # Check connection

        # Fetch the last 100 entries from the stream to find the latest price
        # for the given symbol. This is more efficient than reading the whole stream.
        messages = r.xrevrange(stream_name, count=100)

        for _message_id, message_data in messages:
            if message_data.get("symbol") == symbol:
                price = message_data.get("price")
                if price:
                    return float(price)

        print(
            f"Warning: Price for symbol '{symbol}' not found in the last 100 stream entries.",
            file=sys.stderr,
        )
        return None

    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return None


# Memory layout: [num_symbols][symbol1_hash, price1, timestamp1][symbol2_hash, price2, timestamp2]...
# Each entry: 8 bytes (symbol hash) + 8 bytes (price float64) + 8 bytes (timestamp float64) = 24 bytes
# Max symbols: 1000 (24KB)
MAX_SYMBOLS = 1000
ENTRY_SIZE = 24  # bytes per entry
HEADER_SIZE = 8  # bytes for num_symbols
TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)

# Shared memory name
SHM_NAME = "price_data_shm"

# Test symbols
TEST_SYMBOLS = [
    "BTC/USDC:USDC",
    "ETH/USDC:USDC",
    "SOL/USDC:USDC",
    "HYPE/USDC:USDC",
    "XRP/USDC:USDC",
    "PAXG/USDC:USDC",
    "ENA/USDC:USDC",
    "TRUMP/USDC:USDC",
    "SUI/USDC:USDC",
    "FARTCOIN/USDC:USDC",
    "DOGE/USDC:USDC",
    "KPEPE/USDC:USDC",
    "ADA/USDC:USDC",
    "AVAX/USDC:USDC",
    "PENGU/USDC:USDC",
    "PUMP/USDC:USDC",
    "KBONK/USDC:USDC",
    "LINK/USDC:USDC",
    "IP/USDC:USDC",
    "XPL/USDC:USDC",
    "AAVE/USDC:USDC",
    "ARB/USDC:USDC",
    "POL/USDC:USDC",
]


def symbol_to_hash(symbol):
    """Convert symbol string to 64-bit hash for fast lookup"""
    import hashlib

    # Use SHA256 for deterministic hashing across processes
    hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
    # Take first 8 bytes as 64-bit integer
    return int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF


def main():
    # Try to unlink existing shared memory
    try:
        existing_shm = shared_memory.SharedMemory(name=SHM_NAME)
        existing_shm.close()
        existing_shm.unlink()
        print(f"Unlinked existing shared memory: {SHM_NAME}")
    except FileNotFoundError:
        pass

    # Create shared memory
    shm = shared_memory.SharedMemory(create=True, size=TOTAL_SIZE, name=SHM_NAME)
    print(f"Created shared memory: {SHM_NAME} (size: {TOTAL_SIZE} bytes)")

    # Create numpy array view
    buffer = np.ndarray((TOTAL_SIZE,), dtype=np.uint8, buffer=shm.buf)

    # Initialize header (num_symbols = 0)
    buffer[:HEADER_SIZE].view(np.uint64)[0] = len(TEST_SYMBOLS)

    # Pre-compute symbol hashes
    symbol_hashes = {symbol: symbol_to_hash(symbol) for symbol in TEST_SYMBOLS}

    print(f"Setting prices for {len(TEST_SYMBOLS)} symbols")
    print("Symbol hashes:")
    for symbol, hash_val in symbol_hashes.items():
        print(f"  {symbol}: {hash_val}")

    try:
        iteration = 0
        while True:
            timestamp = time.time()

            # Update all symbols
            for i, symbol in enumerate(TEST_SYMBOLS):
                price = get_latest_price(symbol)

                # Calculate offset for this entry
                offset = HEADER_SIZE + (i * ENTRY_SIZE)

                # Pack data: symbol_hash (uint64), price (float64), timestamp (float64)
                data = struct.pack("<Qdd", symbol_hashes[symbol], price, timestamp)
                buffer[offset : offset + ENTRY_SIZE] = np.frombuffer(
                    data, dtype=np.uint8
                )

            iteration += 1
            if iteration % 10 == 0:
                print(
                    f"Updated prices - iteration {iteration}, timestamp: {timestamp:.2f}"
                )

            # Sleep for a short time to simulate real updates
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nShutting down setter...")
    finally:
        # Clean up
        shm.close()
        shm.unlink()
        print("Cleaned up shared memory")


if __name__ == "__main__":
    main()
