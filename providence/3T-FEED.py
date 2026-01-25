#!/usr/bin/env python3
import struct
import sys
import time
from multiprocessing import shared_memory
from pathlib import Path

import numpy as np
import redis

# Add project root to sys.path to allow importing shared modules
project_root = '/opt/3T'
sys.path.append(str(project_root))

from shared.config import config


def get_redis_config():
    """Constructs Redis configuration from the global config object."""
    redis_config = {
        "host": "192.168.2.157",
        "port": config.get("redis.port"),
        "db": config.get("redis.db", 0),
    }
    if redis_config["host"] is None or redis_config["port"] is None:
        raise ValueError("Redis configuration is incomplete. Check config.yml.")
    return redis_config


def get_latest_price(r, symbol: str):
    """
    Retrieves the latest price for a given symbol from the Redis price stream.
    """
    try:
        stream_name = config.get("redis.streams.price_updates", "prices:updated")
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

    except Exception as e:
        print(f"An unexpected error occurred in get_latest_price: {e}", file=sys.stderr)
        return None


# Shared Memory Configuration
MAX_SYMBOLS = 1000
ENTRY_SIZE = 24
HEADER_SIZE = 8
TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)
SHM_NAME = "price_data_shm"

# Symbols to process
SYMBOLS = config.get("reconciliation_engine.symbols", [])


def symbol_to_hash(symbol):
    """Convert symbol string to 64-bit hash for fast lookup"""
    import hashlib

    hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
    return int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF


def main():
    """Main function to run the price feed setter."""
    if not SYMBOLS:
        print(
            "No symbols found in config.yml under 'reconciliation_engine.symbols'. Exiting."
        )
        return

    shm = None
    r = None
    try:
        redis_config = get_redis_config()
        print(f"Connecting to Redis at {redis_config['host']}...")
        r = redis.Redis(**redis_config, decode_responses=True)
        r.ping()
        print("Successfully connected to Redis.")

        try:
            existing_shm = shared_memory.SharedMemory(name=SHM_NAME)
            existing_shm.close()
            existing_shm.unlink()
            print(f"Unlinked existing shared memory: {SHM_NAME}")
        except FileNotFoundError:
            pass

        shm = shared_memory.SharedMemory(create=True, size=TOTAL_SIZE, name=SHM_NAME)
        print(f"Created shared memory: {SHM_NAME} (size: {TOTAL_SIZE} bytes)")

        buffer = np.ndarray((TOTAL_SIZE,), dtype=np.uint8, buffer=shm.buf)
        buffer[:HEADER_SIZE].view(np.uint64)[0] = len(SYMBOLS)

        symbol_hashes = {symbol: symbol_to_hash(symbol) for symbol in SYMBOLS}
        print(f"Setting prices for {len(SYMBOLS)} symbols...")

        iteration = 0
        while True:
            timestamp = time.time()
            for i, symbol in enumerate(SYMBOLS):
                price = get_latest_price(r, symbol)
                if price is None:
                    # Don't print warning here, get_latest_price already does
                    continue

                offset = HEADER_SIZE + (i * ENTRY_SIZE)
                data = struct.pack("<Qdd", symbol_hashes[symbol], price, timestamp)
                buffer[offset : offset + ENTRY_SIZE] = np.frombuffer(
                    data, dtype=np.uint8
                )

            iteration += 1
            if iteration % 10 == 0:
                print(
                    f"Updated prices - iteration {iteration}, timestamp: {timestamp:.2f}"
                )
            time.sleep(0.1)

    except (ValueError, redis.exceptions.ConnectionError) as err:
        print(f"Error: {err}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\nShutting down setter...")
    finally:
        if shm:
            shm.close()
            shm.unlink()
            print("Cleaned up shared memory.")


if __name__ == "__main__":
    main()
