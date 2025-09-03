#!/usr/bin/env python3
import struct
import sys
import time
from multiprocessing import shared_memory
import numpy as np

def get_volatility( symbol ):
    cursor = cnx.cursor()
    # Define the query to fetch the data
    query = f"""
    WITH
    minute_data AS (
      SELECT 
        timestamp,
        high - low AS range_high_low
      FROM 
        market_data
      WHERE 
        timeframe = '1m' AND symbol = '{symbol}’
        AND timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR) * 1000
    ),

    window_data AS (
      SELECT 
        timestamp,
        MAX(high) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS max_high,
        MIN(low) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS min_low
      FROM 
        market_data
      WHERE 
        timeframe = '1m' AND symbol = '{symbol}'
        AND timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR) * 1000
    ),

    volatility_data AS (
      SELECT 
        timestamp,
        max_high - min_low AS volatility
      FROM 
        window_data
    )

    SELECT 
      from_unixtime(timestamp/1000) AS timestamp,
      AVG(volatility) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS
    moving_volatility_average,
      (select close from market_data where timeframe='1m' and market_data.timestamp = volatility_data.timestamp AND symbol = '{symbol}') as 'close'
    FROM 
      volatility_data
      ORDER BY 1 DESC LIMIT 1
        """
    cursor.execute(query)
    results = cursor.fetchall()
    # Convert the results to a Pandas DataFrame
    df = pd.DataFrame(results, columns=['timestamp', 'moving_volatility_average','close'])
    cursor.close()
    return float( df['moving_volatility_average'].iloc[0] )


# Memory layout: [num_symbols][symbol1_hash, volatility1, timestamp1][symbol2_hash, volatility2, timestamp2]...
# Each entry: 8 bytes (symbol hash) + 8 bytes (volatility float64) + 8 bytes (timestamp float64) = 24 bytes
# Max symbols: 1000 (24KB)
MAX_SYMBOLS = 1000
ENTRY_SIZE = 24  # bytes per entry
HEADER_SIZE = 8  # bytes for num_symbols
TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)

# Shared memory name
SHM_NAME = "market_data_shm"

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

    print(f"Setting volatility for {len(TEST_SYMBOLS)} symbols")
    print("Symbol hashes:")
    for symbol, hash_val in symbol_hashes.items():
        print(f"  {symbol}: {hash_val}")

    try:
        iteration = 0
        while True:
            timestamp = time.time()

            # Update all symbols
            for i, symbol in enumerate(TEST_SYMBOLS):
                volatility = get_volatility(symbol)

                # Calculate offset for this entry
                offset = HEADER_SIZE + (i * ENTRY_SIZE)

                # Pack data: symbol_hash (uint64), volatility (float64), timestamp (float64)
                data = struct.pack("<Qdd", symbol_hashes[symbol], volatility, timestamp)
                buffer[offset : offset + ENTRY_SIZE] = np.frombuffer(
                    data, dtype=np.uint8
                )

            iteration += 1
            if iteration % 10 == 0:
                print(
                    f"Updated volatility - iteration {iteration}, timestamp: {timestamp:.2f}"
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
