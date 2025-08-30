# Makefile for KVT Sample Program
# Assumes eloqkv is built in ../build directory

# Compiler settings
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -g

# Paths
ELOQKV_ROOT = ..
BUILD_DIR = $(ELOQKV_ROOT)/build

# Library paths
LIB_PATHS = -L$(BUILD_DIR) \
            -L$(BUILD_DIR)/tx_service \
            -L$(BUILD_DIR)/eloq_metrics \
            -L$(BUILD_DIR)/lua \
            -L$(BUILD_DIR)/fpconv \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/hash \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/container \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/debugging \
            -L$(BUILD_DIR)/tx_service/abseil-cpp/absl/base

# Libraries to link (order matters for static linking)
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

# Additional system libraries that might be needed
SYSTEM_LIBS = -lrocksdb \
              -lsnappy \
              -lbz2 \
              -llz4 \
              -lzstd \
              -lmimalloc \
              $(ABSEIL_LIBS)

# Target executable
TARGET = kvt_sample

# Source files
SOURCES = kvt_sample.cpp

# Object files
OBJECTS = $(SOURCES:.cpp=.o)

# Default target
all: $(TARGET)

# Build the executable
$(TARGET): $(OBJECTS)
	@echo "Linking $(TARGET)..."
	$(CXX) $(CXXFLAGS) $(OBJECTS) -o $(TARGET) $(LIB_PATHS) -Wl,--start-group $(LIBS) -Wl,--end-group $(SYSTEM_LIBS)
	@echo "Build complete: $(TARGET)"

# Compile source files
%.o: %.cpp
	@echo "Compiling $<..."
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean build files
clean:
	@echo "Cleaning..."
	rm -f $(OBJECTS) $(TARGET)
	@echo "Clean complete"

# Run the sample program
run: $(TARGET)
	@echo "Running $(TARGET)..."
	./$(TARGET)

# Build and run
test: all run

# Debug build
debug: CXXFLAGS += -DDEBUG -g3 -O0
debug: all

# Show configuration
info:
	@echo "Configuration:"
	@echo "  CXX: $(CXX)"
	@echo "  CXXFLAGS: $(CXXFLAGS)"
	@echo "  BUILD_DIR: $(BUILD_DIR)"
	@echo "  Libraries: $(LIBS) $(SYSTEM_LIBS)"

# Help
help:
	@echo "Available targets:"
	@echo "  all     - Build the sample program (default)"
	@echo "  clean   - Remove build artifacts"
	@echo "  run     - Run the sample program"
	@echo "  test    - Build and run the sample"
	@echo "  debug   - Build with debug symbols"
	@echo "  info    - Show build configuration"
	@echo "  help    - Show this help message"

.PHONY: all clean run test debug info help