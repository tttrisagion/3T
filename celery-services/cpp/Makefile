# Makefile for building high-performance libraries.

# Compilers
NVCC = nvcc
CPP = g++

# Compiler Flags
NVCCFLAGS = -O3 -shared -Xcompiler -fPIC
CPPFLAGS = -O3 -shared -fPIC -Wall -std=c++17

# Directories
SRCDIR = src
BINDIR = bin

# Source Files
CPU_LIB_SOURCE = $(SRCDIR)/perm_entropy_cpu_lib.cpp

# Target Libraries
TARGET_CPU_LIB = $(BINDIR)/libperm_entropy_cpu.so

# --- Primary Target ---
# 'make' or 'make all' will build both shared libraries.

# --- Build Rules ---


# Rule to build the CPU shared library
$(TARGET_CPU_LIB): $(CPU_LIB_SOURCE)
	@mkdir -p $(BINDIR)
	@echo "Building CPU high-performance library..."
	$(CPP) $(CPPFLAGS) $< -o $@
	@echo "Successfully built $(TARGET_CPU_LIB)"

# --- Clean Target ---
clean:
	@echo "Cleaning up..."
	@rm -rf $(BINDIR)
	@echo "Cleanup complete."

.PHONY: all clean

