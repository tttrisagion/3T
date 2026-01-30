import ctypes
import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


def load_perm_entropy_library():
    """Load the permutation entropy C++ library."""
    try:
        # Standard library path inside the container
        lib_path = Path("/usr/local/lib/libperm_entropy_cpu.so")

        if not lib_path.exists():
            logger.warning(f"Library not found at {lib_path}")
            return None

        lib = ctypes.CDLL(str(lib_path))

        func = lib.calculate_cpu_perm_entropy
        func.argtypes = [
            np.ctypeslib.ndpointer(dtype=np.float64, flags="C_CONTIGUOUS"),
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
        ]
        func.restype = ctypes.c_double
        logger.info("✅ Successfully loaded permutation entropy library")
        return func
    except (OSError, AttributeError) as e:
        logger.warning(
            f"⚠️ WARNING: Could not load permutation entropy library. Error: {e}"
        )
        return None


# Load the library on module import
calculate_cpu_entropy = load_perm_entropy_library()


def calculate_permutation_entropy(data):
    order = 3
    delay = 1
    try:
        if not calculate_cpu_entropy:
            error_msg = "Permutation entropy library not loaded"
            logger.error(error_msg)
            return {"error": error_msg, "result": None}

        # Convert data to numpy array
        x_np = np.array(data, dtype=np.float64)
        n = len(x_np)

        # Validate inputs
        if n < order:
            error_msg = f"Data length ({n}) must be >= order ({order})"
            logger.error(error_msg)
            return {"error": error_msg, "result": None}

        result = calculate_cpu_entropy(x_np, n, order, delay)

        return result

    except Exception as e:
        logger.error(f"Error in calculate_permutation_entropy task: {e}")
        return None
