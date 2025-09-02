# Unified Makefile for KVT - Supports both ELOQ and MEM configurations
# Usage: make eloq    - Build full version with external libraries
#        make mem     - Build memory-only version
#        make all     - Build both versions

# Compiler settings
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -g

# Paths for ELOQ version
ELOQKV_ROOT = ..
BUILD_DIR = $(ELOQKV_ROOT)/build

# Library paths for ELOQ version
LIB_PATHS = -L$(BUILD_DIR) \
            -L$(BUILD_DIR)/tx_service \
            -L$(BUILD_DIR)/eloq_metrics \
            -L$(BUILD_DIR)/lua \
            -L$(BUILD_DIR)/fpconv \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/hash \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/container \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/debugging \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/base

# Libraries for ELOQ version
LIBS = -leloqkv \
       -ltxservice \
       -leloq-metrics \
       -llua \
       -lfpconv \
       -lbrpc \
       -lgflags \
       -lprotobuf \
       -lglog \
       -lleveldb \
       -lssl \
       -lcrypto \
       -lpthread \
       -ldl \
       -lz \
       -lwasmtime

# Abseil libraries
ABSEIL_LIBS = -labsl_hash \
              -labsl_city \
              -labsl_low_level_hash \
              -labsl_raw_hash_set \
              -labsl_hashtablez_sampler \
              -labsl_exponential_biased \
              -labsl_base \
              -labsl_throw_delegate \
              -labsl_raw_logging_internal \
              -labsl_log_severity \
              -labsl_malloc_internal \
              -labsl_spinlock_wait \
              -labsl_stacktrace \
              -labsl_symbolize \
              -labsl_debugging_internal \
              -labsl_demangle_internal

# Additional system libraries
SYSTEM_LIBS = -lrocksdb \
              -lsnappy \
              -lbz2 \
              -llz4 \
              -lzstd \
              -lmimalloc \
              $(ABSEIL_LIBS)

# Source files
SAMPLE_SOURCES = kvt_sample.cpp
STRESS_SOURCES = kvt_stress_test.cpp

# Object files
SAMPLE_OBJECTS = $(SAMPLE_SOURCES:.cpp=.o)
STRESS_OBJECTS = $(STRESS_SOURCES:.cpp=.o)
MEM_OBJECTS = kvt_mem.o

# Target executables
ELOQ_TARGETS = kvt_sample_eloq kvt_stress_test_eloq
MEM_TARGETS = kvt_sample_mem kvt_stress_test_mem
ALL_TARGETS = $(ELOQ_TARGETS) $(MEM_TARGETS)

# Default target
all: eloq mem

# ELOQ version (full with external libraries)
eloq: $(ELOQ_TARGETS)

# MEM version (memory-only)
mem: $(MEM_TARGETS)

# Build ELOQ targets
kvt_sample_eloq: $(SAMPLE_OBJECTS)
	@echo "Linking kvt_sample_eloq (ELOQ version)..."
	$(CXX) $(CXXFLAGS) $(SAMPLE_OBJECTS) -o kvt_sample_eloq $(LIB_PATHS) -Wl,--start-group $(LIBS) -Wl,--end-group $(SYSTEM_LIBS)
	@echo "Build complete: kvt_sample_eloq"

kvt_stress_test_eloq: $(STRESS_OBJECTS)
	@echo "Linking kvt_stress_test_eloq (ELOQ version)..."
	$(CXX) $(CXXFLAGS) $(STRESS_OBJECTS) -o kvt_stress_test_eloq $(LIB_PATHS) -Wl,--start-group $(LIBS) -Wl,--end-group $(SYSTEM_LIBS)
	@echo "Build complete: kvt_stress_test_eloq"

# Build MEM targets
kvt_sample_mem: $(SAMPLE_OBJECTS) $(MEM_OBJECTS)
	@echo "Linking kvt_sample_mem (MEM version)..."
	$(CXX) $(CXXFLAGS) -fPIC $(SAMPLE_OBJECTS) $(MEM_OBJECTS) -o kvt_sample_mem
	@echo "Build complete: kvt_sample_mem"

kvt_stress_test_mem: $(STRESS_OBJECTS) $(MEM_OBJECTS)
	@echo "Linking kvt_stress_test_mem (MEM version)..."
	$(CXX) $(CXXFLAGS) -fPIC $(STRESS_OBJECTS) $(MEM_OBJECTS) -o kvt_stress_test_mem
	@echo "Build complete: kvt_stress_test_mem"

# Compile kvt_mem.cpp with -fPIC for MEM version
kvt_mem.o: kvt_mem.cpp kvt_mem.h kvt_inc.h
	@echo "Compiling kvt_mem.cpp with -fPIC..."
	$(CXX) $(CXXFLAGS) -fPIC -c kvt_mem.cpp -o kvt_mem.o

# Compile source files
%.o: %.cpp kvt_inc.h
	@echo "Compiling $<..."
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean build files
clean:
	@echo "Cleaning all build files..."
	rm -f $(SAMPLE_OBJECTS) $(STRESS_OBJECTS) $(MEM_OBJECTS) $(ALL_TARGETS)
	@echo "Clean complete"

# Clean specific versions
clean_eloq:
	@echo "Cleaning ELOQ build files..."
	rm -f $(SAMPLE_OBJECTS) $(STRESS_OBJECTS) $(ELOQ_TARGETS)
	@echo "ELOQ clean complete"

clean_mem:
	@echo "Cleaning MEM build files..."
	rm -f $(SAMPLE_OBJECTS) $(STRESS_OBJECTS) $(MEM_OBJECTS) $(MEM_TARGETS)
	@echo "MEM clean complete"

# Run targets
run_sample_eloq: kvt_sample_eloq
	@echo "Running kvt_sample_eloq..."
	./kvt_sample_eloq

run_stress_eloq: kvt_stress_test_eloq
	@echo "Running kvt_stress_test_eloq..."
	./kvt_stress_test_eloq

run_sample_mem: kvt_sample_mem
	@echo "Running kvt_sample_mem..."
	./kvt_sample_mem

run_stress_mem: kvt_stress_test_mem
	@echo "Running kvt_stress_test_mem..."
	./kvt_stress_test_mem

# Test targets
test_eloq: eloq run_sample_eloq
test_mem: mem run_sample_mem

# Debug builds
debug_eloq: CXXFLAGS += -DDEBUG -g3 -O0
debug_eloq: clean_eloq eloq

debug_mem: CXXFLAGS += -DDEBUG -g3 -O0
debug_mem: clean_mem mem

# Show configuration
info:
	@echo "Unified KVT Configuration:"
	@echo "  CXX: $(CXX)"
	@echo "  CXXFLAGS: $(CXXFLAGS)"
	@echo "  BUILD_DIR: $(BUILD_DIR)"
	@echo "  ELOQ Targets: $(ELOQ_TARGETS)"
	@echo "  MEM Targets: $(MEM_TARGETS)"
	@echo "  ELOQ Libraries: $(LIBS) $(SYSTEM_LIBS)"

# Help
help:
	@echo "Unified KVT Makefile Targets:"
	@echo ""
	@echo "Build Targets:"
	@echo "  all         - Build both ELOQ and MEM versions"
	@echo "  eloq        - Build ELOQ version (with external libraries)"
	@echo "  mem         - Build MEM version (memory-only)"
	@echo ""
	@echo "Individual Targets:"
	@echo "  kvt_sample_eloq      - Build ELOQ sample program"
	@echo "  kvt_stress_test_eloq - Build ELOQ stress test"
	@echo "  kvt_sample_mem       - Build MEM sample program"
	@echo "  kvt_stress_test_mem  - Build MEM stress test"
	@echo ""
	@echo "Run Targets:"
	@echo "  run_sample_eloq      - Run ELOQ sample program"
	@echo "  run_stress_eloq      - Run ELOQ stress test"
	@echo "  run_sample_mem       - Run MEM sample program"
	@echo "  run_stress_mem       - Run MEM stress test"
	@echo ""
	@echo "Test Targets:"
	@echo "  test_eloq            - Build and run ELOQ sample"
	@echo "  test_mem             - Build and run MEM sample"
	@echo ""
	@echo "Clean Targets:"
	@echo "  clean                - Clean all build files"
	@echo "  clean_eloq           - Clean ELOQ build files"
	@echo "  clean_mem            - Clean MEM build files"
	@echo ""
	@echo "Debug Targets:"
	@echo "  debug_eloq           - Build ELOQ with debug symbols"
	@echo "  debug_mem            - Build MEM with debug symbols"
	@echo ""
	@echo "Info Targets:"
	@echo "  info                 - Show build configuration"
	@echo "  help                 - Show this help message"

.PHONY: all eloq mem clean clean_eloq clean_mem run_sample_eloq run_stress_eloq run_sample_mem run_stress_mem test_eloq test_mem debug_eloq debug_mem info help
