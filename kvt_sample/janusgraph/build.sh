#!/bin/bash

# Build script for JanusGraph KVT Adapter
# This script builds the adapter from within the janusgraph subdirectory

set -e  # Exit on any error

echo "Building JanusGraph KVT Adapter..."
echo "=================================="

# Set paths
KVT_DIR=".."
CXX_FLAGS="-std=c++17 -Wall -Wextra -O2 -g"

echo "Step 1: Compiling KVT implementation..."
g++ $CXX_FLAGS -I$KVT_DIR -c $KVT_DIR/kvt_mem.cpp -o kvt_mem.o

echo "Step 2: Compiling JanusGraph adapter test..."  
g++ $CXX_FLAGS -I$KVT_DIR -c janusgraph_kvt_test.cpp -o janusgraph_kvt_test.o

echo "Step 3: Linking final executable..."
g++ $CXX_FLAGS -o janusgraph_kvt_test kvt_mem.o janusgraph_kvt_test.o -pthread

echo ""
echo "Build completed successfully!"
echo ""
echo "Usage:"
echo "  ./janusgraph_kvt_test    # Run the test suite"
echo "  make test                # Alternative: build and run via Make"
echo "  make clean               # Clean build artifacts"