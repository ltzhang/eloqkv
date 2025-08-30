# Makefile for JanusGraph KVT Adapter
# This builds the adapter from within the janusgraph subdirectory

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -g
KVT_DIR = ..

# Include directories
INCLUDES = -I$(KVT_DIR)

# Source files
KVT_SOURCES = $(KVT_DIR)/kvt_mem.cpp
ADAPTER_SOURCES = janusgraph_kvt_test.cpp

# Object files  
KVT_OBJECTS = kvt_mem.o
ADAPTER_OBJECTS = janusgraph_kvt_test.o

# Targets
TARGET = janusgraph_kvt_test

# Default target
all: $(TARGET)

# Build the test program
$(TARGET): $(KVT_OBJECTS) $(ADAPTER_OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -pthread

# Compile KVT implementation
kvt_mem.o: $(KVT_DIR)/kvt_mem.cpp $(KVT_DIR)/kvt_mem.h $(KVT_DIR)/kvt_inc.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Compile adapter test
janusgraph_kvt_test.o: janusgraph_kvt_test.cpp janusgraph_kvt_adapter.h $(KVT_DIR)/kvt_inc.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Run tests
test: $(TARGET)
	./$(TARGET)

# Clean build artifacts
clean:
	rm -f $(TARGET) $(KVT_OBJECTS) $(ADAPTER_OBJECTS)

# Install (for future integration)
install: $(TARGET)
	@echo "Install target not yet implemented"

# Help
help:
	@echo "Available targets:"
	@echo "  all     - Build the JanusGraph KVT adapter test"
	@echo "  test    - Build and run the test suite"  
	@echo "  clean   - Remove build artifacts"
	@echo "  help    - Show this help message"

# Phony targets
.PHONY: all test clean install help