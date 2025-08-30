# JanusGraph KVT Adapter

This directory contains a C++ adapter that bridges the KVT (Key-Value Transaction) store with JanusGraph's key-column-value data model requirements.

## Overview

JanusGraph requires a storage backend that supports key-column-value operations (i.e., `key -> column -> value` mappings), while the underlying KVT store only supports simple key-value operations. This adapter provides two different approaches to solve this impedance mismatch:

1. **Serialized Columns Method**: All columns for a key are serialized into a single value
2. **Composite Key Method**: Each column is stored as a separate key-value pair using composite keys

## Architecture

### Method 1: Serialized Columns
```
JanusGraph Key: "vertex:123"
Columns: {name: "Alice", age: "30", city: "NYC"}

KVT Storage:
Key: "vertex:123" 
Value: [serialized binary format containing all columns]
```

**Advantages:**
- Efficient retrieval of all columns for a key
- Atomic updates of multiple columns
- Better storage efficiency for keys with many columns

**Disadvantages:**
- Must deserialize to access individual columns
- Updating a single column requires rewriting the entire value

### Method 2: Composite Keys
```
JanusGraph Key: "vertex:123"
Columns: {name: "Alice", age: "30", city: "NYC"}

KVT Storage:
Key: "vertex:123\0name" -> Value: "Alice"
Key: "vertex:123\0age"  -> Value: "30"  
Key: "vertex:123\0city" -> Value: "NYC"
```

**Advantages:**
- Direct access to individual columns
- Efficient single-column updates
- Natural support for column deletion

**Disadvantages:**
- More storage overhead (key duplication)
- Retrieving all columns requires multiple operations

## Directory Structure

```
kvt/
├── janusgraph/                    # JanusGraph adapter (this directory)
│   ├── README.md                  # This file
│   ├── janusgraph_kvt_adapter.h   # Main adapter implementation
│   └── janusgraph_kvt_test.cpp    # Comprehensive test suite
├── kvt_inc.h                      # KVT API interface
├── kvt_mem.h                      # KVT memory implementation header
├── kvt_mem.cpp                    # KVT memory implementation
├── kvt_sample.cpp                 # Basic KVT usage example
├── build.sh                       # Build script
└── Makefile                       # Make-based build system
```

## Building and Testing

### Prerequisites
- C++17 compatible compiler (g++ recommended)
- Make (optional, for Makefile-based builds)

### Quick Build and Test
```bash
# Navigate to the kvt directory
cd /path/to/janusgraph-inmemory/kvt

# Build using the build script
./build.sh

# Run the JanusGraph adapter tests
./janusgraph/janusgraph_kvt_test
```

### Alternative Build Methods

#### Using Makefile
```bash
cd /path/to/janusgraph-inmemory/kvt
make janusgraph_kvt_test
./janusgraph_kvt_test
```

#### Manual Build
```bash
# Build KVT library
g++ -std=c++17 -Wall -Wextra -O2 -g -fPIC -c kvt_mem.cpp -o kvt_mem.o
ar rcs libkvt.a kvt_mem.o

# Build JanusGraph test (from kvt directory)
g++ -std=c++17 -Wall -Wextra -O2 -g \
    janusgraph/janusgraph_kvt_test.cpp kvt_mem.o \
    -o janusgraph/janusgraph_kvt_test -pthread

# Run test
./janusgraph/janusgraph_kvt_test
```

## API Documentation

### Core Adapter Class: `JanusGraphKVTAdapter`

```cpp
#include "janusgraph_kvt_adapter.h"

// Switch between storage methods globally
janusgraph::g_use_composite_key_method = true;  // or false

JanusGraphKVTAdapter adapter;
```

### Key Operations

#### Setting Column Values
```cpp
std::string error;
bool success = adapter.set_column(
    tx_id,           // Transaction ID (0 for auto-commit)
    "table_name",    // Table name
    "vertex:123",    // Key
    "name",          // Column name
    "Alice",         // Value
    error           // Error message output
);
```

#### Getting Column Values
```cpp
std::string value, error;
bool success = adapter.get_column(
    tx_id, "table_name", "vertex:123", "name", value, error
);
```

#### Batch Operations
```cpp
std::vector<janusgraph::ColumnValue> columns = {
    {"name", "Alice"},
    {"age", "30"},
    {"city", "NYC"}
};
bool success = adapter.set_columns(tx_id, "table_name", "vertex:123", columns, error);
```

#### Retrieving All Columns
```cpp
auto columns = adapter.get_all_columns(tx_id, "table_name", "vertex:123", error);
for (const auto& cv : columns) {
    std::cout << cv.column << " = " << cv.value << std::endl;
}
```

## Configuration

### Switching Storage Methods
The adapter uses a global boolean flag to determine which storage method to use:

```cpp
// Use composite key method
janusgraph::g_use_composite_key_method = true;

// Use serialized columns method  
janusgraph::g_use_composite_key_method = false;
```

This flag should be set once at the beginning of your application and not changed during runtime.

## Testing

### Test Suite Overview
The test program (`janusgraph_kvt_test.cpp`) validates both storage methods with:

- **Basic Operations**: Set, get, update, delete operations
- **Transaction Support**: Multi-operation transactions with commit/rollback
- **Batch Operations**: Setting multiple columns atomically
- **Performance Tests**: Write/read performance measurement
- **Edge Cases**: Empty values, binary data, special characters

### Running Tests
```bash
./janusgraph/janusgraph_kvt_test
```

### Expected Output
```
==================================
  JanusGraph KVT Adapter Test    
==================================
✓ KVT system initialized

╔══════════════════════════════════════╗
║  METHOD 1: SERIALIZED COLUMNS       ║
╚══════════════════════════════════════╝

========================================
 Basic Operations Test - Serialized Columns
========================================
  [✓] Set single column
  [✓] Get single column
  [✓] Set/Get multiple columns - age
  ...
```

## Performance Considerations

### Serialized Columns Method
- **Best for**: Applications that frequently access all columns together
- **Write Performance**: Moderate (requires serialization/deserialization)
- **Read Performance**: Excellent for full-column reads, poor for single-column reads
- **Storage Efficiency**: High (no key duplication)

### Composite Key Method  
- **Best for**: Applications that frequently access individual columns
- **Write Performance**: Excellent for single-column updates
- **Read Performance**: Excellent for single-column reads, moderate for full-column reads
- **Storage Efficiency**: Lower (key duplication for each column)

## Integration with JanusGraph

### Recommended Usage Pattern
```cpp
// Initialize KVT system
kvt_initialize();

// Create table
std::string error;
uint64_t table_id = kvt_create_table("janusgraph_vertices", "hash", error);

// Choose storage method based on your use case
janusgraph::g_use_composite_key_method = false; // Recommended for most cases

// Create adapter
JanusGraphKVTAdapter adapter;

// Use within JanusGraph backend implementation
// ... integrate with JanusGraph's KeyColumnValueStore interface
```

## Error Handling

All adapter methods return boolean success indicators and populate error message strings:

```cpp
std::string error;
if (!adapter.get_column(0, "table", "key", "column", value, error)) {
    std::cerr << "Error: " << error << std::endl;
}
```

Common error scenarios:
- Table not found
- Key not found  
- Column not found
- Transaction not found
- Serialization/deserialization errors

## Troubleshooting

### Build Issues
1. **Compiler Errors**: Ensure C++17 support (`-std=c++17`)
2. **Missing Headers**: Verify all required includes are available
3. **Linking Errors**: Make sure `kvt_mem.o` is compiled and linked

### Runtime Issues
1. **Segmentation Faults**: Ensure `kvt_initialize()` is called before any operations
2. **Transaction Errors**: Verify transaction IDs are valid
3. **Performance Issues**: Consider switching storage methods based on access patterns

### Common Problems
```bash
# Problem: kvt_del undefined reference
# Solution: Ensure kvt_mem.cpp includes the kvt_del implementation

# Problem: Tests failing with "Key not found"
# Solution: Check that one-shot operations (tx_id=0) are handled correctly

# Problem: Binary data corruption  
# Solution: Verify serialization handles all byte values (0x00-0xFF)
```

## Contributing

When modifying the adapter:

1. **Test Both Methods**: Ensure changes work with both storage approaches
2. **Update Tests**: Add test cases for new functionality
3. **Document Changes**: Update this README for API changes
4. **Performance Impact**: Consider the performance implications of changes

## License

This code is part of the JanusGraph project and follows the same license terms.