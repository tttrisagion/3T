import ctypes

import numpy as np


def load_perm_entropy_library():
    """Load the permutation entropy C++ library."""
    try:
        lib_path = "/opt/3T/celery-services/cpp/bin/libperm_entropy_cpu.so"
        lib = ctypes.CDLL(lib_path)

        func = lib.calculate_cpu_perm_entropy
        func.argtypes = [
            np.ctypeslib.ndpointer(dtype=np.float64, flags="C_CONTIGUOUS"),
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
        ]
        func.restype = ctypes.c_double
        print("✅ Successfully loaded permutation entropy library")
        return func
    except (OSError, AttributeError) as e:
        print(f"⚠️ WARNING: Could not load permutation entropy library. Error: {e}")
        return None


# Load the library on module import
calculate_cpu_entropy = load_perm_entropy_library()


def calculate_permutation_entropy(data):
    order = 3
    delay = 1
    try:
        if not calculate_cpu_entropy:
            error_msg = "Permutation entropy library not loaded"
            span.set_attribute("error", True)
            span.add_event(error_msg)
            return {"error": error_msg, "result": None}

        # Convert data to numpy array
        x_np = np.array(data, dtype=np.float64)
        n = len(x_np)

        # Validate inputs
        if n < order:
            error_msg = f"Data length ({n}) must be >= order ({order})"
            span.set_attribute("error", True)
            span.add_event(error_msg)
            return {"error": error_msg, "result": None}

        result = calculate_cpu_entropy(x_np, n, order, delay)

        return result

    except Exception as e:
        print(f"Error in calculate_permutation_entropy task: {e}")
        return None


print("🧪 Testing permutation entropy c-library...")

# Generate some test data
np.random.seed(42)  # For reproducible results
test_data = np.random.randn(100).tolist()

result = calculate_permutation_entropy(test_data)

print("Random", result)


# generate some non random test data
test_data = range(0, 100)

result = calculate_permutation_entropy(test_data)

print("Linear", result)

# 3. Test with a predictable oscillating wave (sine wave)
# Generate 100 data points for two full sine wave cycles
x_values = np.linspace(0, 4 * np.pi, 100)
sine_wave_data = np.sin(x_values)

result = calculate_permutation_entropy(sine_wave_data)

print("Sine Wave", result)


import antropy as ant
import numpy as np

print(" Legacy testing with antropy library...")
print("-" * 30)

# --- Test 1: Random Data ---
# For reproducible results, we use the same seed as your original script.
np.random.seed(42)
random_data = np.random.randn(100)
result_random = ant.perm_entropy(random_data, normalize=True)
print(f"Random (antropy): {result_random}")


# --- Test 2: Linear Data ---
# Generate a simple linear sequence.
linear_data = np.arange(0, 100)
result_linear = ant.perm_entropy(linear_data, normalize=True)
print(f"Linear (antropy): {result_linear}")


# --- Test 3: Oscillating Wave (Sine Wave) ---
# Generate 100 data points for two full sine wave cycles.
x_values = np.linspace(0, 4 * np.pi, 100)
sine_wave_data = np.sin(x_values)
result_sine = ant.perm_entropy(sine_wave_data, normalize=True)
print(f"Sine Wave (antropy): {result_sine}")

print("-" * 30)
