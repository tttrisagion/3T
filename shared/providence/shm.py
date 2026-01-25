import struct
import time
import hashlib
import logging
from multiprocessing import resource_tracker, shared_memory
import numpy as np

logger = logging.getLogger(__name__)

# Constants
MAX_SYMBOLS = 1000
ENTRY_SIZE = 24
HEADER_SIZE = 8
TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)
MAX_AGE_SECONDS = 5

SHM_PRICE_NAME = "price_data_shm"
SHM_MARKET_DATA_NAME = "market_data_shm"

def symbol_to_hash(symbol: str) -> int:
    """Convert symbol string to 64-bit hash for fast lookup"""
    # Use SHA256 for deterministic hashing across processes
    hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
    # Take first 8 bytes as 64-bit integer
    return int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF

def get_price_from_shm(symbol: str, shm_name: str = SHM_PRICE_NAME):
    """Gets the latest price for a symbol from shared memory."""
    try:
        shm = shared_memory.SharedMemory(name=shm_name)
        # CRITICAL: Unregister from resource tracker to prevent cleanup
        try:
            resource_tracker.unregister(shm._name, "shared_memory")
        except KeyError:
            pass # Already unregistered or not tracked

        header_size, entry_size = 8, 24
        max_symbols = (shm.size - header_size) // entry_size
        buffer = np.ndarray((shm.size,), dtype=np.uint8, buffer=shm.buf)
        num_symbols = buffer[:header_size].view(np.uint64)[0]

        symbol_hash = symbol_to_hash(symbol)

        for i in range(min(num_symbols, max_symbols)):
            offset = header_size + (i * entry_size)
            entry_data = buffer[offset : offset + entry_size].tobytes()
            stored_hash, price, _ = struct.unpack("<Qdd", entry_data)
            if stored_hash == symbol_hash:
                return price
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error reading price from SHM: {e}")
        return None
    finally:
        if "shm" in locals() and shm:
            shm.close()
    return None

def get_volatility(symbol: str, max_retries: int = None, shm_name: str = SHM_MARKET_DATA_NAME):
    """
    Get volatility for a symbol from shared memory.
    Returns volatility if found and fresh, otherwise retries.
    """
    symbol_hash = symbol_to_hash(symbol)
    retry_count = 0

    while max_retries is None or retry_count < max_retries:
        try:
            # Connect to existing shared memory
            shm = shared_memory.SharedMemory(name=shm_name)

            # CRITICAL: Unregister from resource tracker to prevent cleanup
            # This prevents the shared memory from being destroyed when getter exits
            try:
                resource_tracker.unregister(shm._name, "shared_memory")
            except KeyError:
                pass

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
                            return volatility
                        else:
                            logger.warning(
                                f"Volatility for {symbol} is stale (age: {age:.1f}s), retrying..."
                            )
                            break
                else:
                    logger.warning(f"Symbol {symbol} not found in shared memory, retrying...")

            finally:
                shm.close()

        except FileNotFoundError:
            logger.warning("Shared memory not found, setter may not be running. Retrying...")
        except Exception as e:
            logger.error(f"Error reading shared memory: {e}")

        retry_count += 1
        if max_retries is None or retry_count < max_retries:
            logger.info(f"Sleeping 30 seconds before retry {retry_count}...")
            time.sleep(30)

    raise TimeoutError(
        f"Could not get fresh volatility for {symbol} after {retry_count} attempts"
    )
