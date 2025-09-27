#!/usr/bin/env python3
import struct
import sys
import time
from multiprocessing import shared_memory
from pathlib import Path

import mysql.connector
import numpy as np
import pandas as pd

# Add project root to sys.path to allow importing shared modules
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from shared.config import config


def get_db_config():
    """Constructs database configuration from the global config object."""
    db_config = {
        "user": config.get("database.user"),
        "password": config.get_secret("database.password"),
        "host": "localhost",
        "database": config.get("database.database"),
    }
    if not all(db_config.values()):
        raise ValueError(
            "Database configuration is incomplete. Check config.yml and secrets.yml."
        )
    return db_config


def get_volatility(cnx, symbol):
    """
    Calculates the moving average of volatility for a given symbol.
    """
    cursor = cnx.cursor()
    query = f"""
        WITH
        minute_data AS (
          SELECT timestamp, high - low AS range_high_low
          FROM market_data
          WHERE timeframe = '1m' AND symbol = '{symbol}'
            AND timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR) * 1000
        ),
        window_data AS (
          SELECT
            timestamp,
            MAX(high) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS max_high,
            MIN(low) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS min_low
          FROM market_data
          WHERE timeframe = '1m' AND symbol = '{symbol}'
            AND timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR) * 1000
        ),
        volatility_data AS (
          SELECT timestamp, max_high - min_low AS volatility
          FROM window_data
        )
        SELECT
          from_unixtime(timestamp/1000) AS timestamp,
          AVG(volatility) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS moving_volatility_average,
          (SELECT close FROM market_data WHERE timeframe='1m' AND market_data.timestamp = volatility_data.timestamp AND symbol = '{symbol}') as 'close'
        FROM volatility_data
        ORDER BY 1 DESC LIMIT 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    df = pd.DataFrame(
        results, columns=["timestamp", "moving_volatility_average", "close"]
    )
    cursor.close()

    if df.empty:
        return None

    return float(df["moving_volatility_average"].iloc[0])


# Shared Memory Configuration
MAX_SYMBOLS = 1000
ENTRY_SIZE = 24
HEADER_SIZE = 8
TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)
SHM_NAME = "market_data_shm"

# Symbols to process
SYMBOLS = config.get("reconciliation_engine.symbols", [])


def symbol_to_hash(symbol):
    """Convert symbol string to 64-bit hash for fast lookup"""
    import hashlib

    hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
    return int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF


def main():
    """Main function to run the volatility setter."""
    if not SYMBOLS:
        print(
            "No symbols found in config.yml under 'reconciliation_engine.symbols'. Exiting."
        )
        return

    shm = None
    cnx = None
    try:
        db_config = get_db_config()
        print(f"Connecting to database at {db_config['host']}...")
        cnx = mysql.connector.connect(**db_config)
        print("Successfully connected to database.")

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
        print(f"Setting volatility for {len(SYMBOLS)} symbols...")

        iteration = 0
        while True:
            timestamp = time.time()
            for i, symbol in enumerate(SYMBOLS):
                volatility = get_volatility(cnx, symbol)
                if volatility is None:
                    print(f"Warning: Volatility for {symbol} not found. Skipping.")
                    continue

                offset = HEADER_SIZE + (i * ENTRY_SIZE)
                data = struct.pack("<Qdd", symbol_hashes[symbol], volatility, timestamp)
                buffer[offset : offset + ENTRY_SIZE] = np.frombuffer(
                    data, dtype=np.uint8
                )

            iteration += 1
            if iteration % 10 == 0:
                print(
                    f"Updated volatility - iteration {iteration}, timestamp: {timestamp:.2f}"
                )
            time.sleep(0.1)

    except (ValueError, mysql.connector.Error) as err:
        print(f"Error: {err}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\nShutting down setter...")
    finally:
        if cnx and cnx.is_connected():
            cnx.close()
            print("Database connection closed.")
        if shm:
            shm.close()
            shm.unlink()
            print("Cleaned up shared memory.")


if __name__ == "__main__":
    main()
