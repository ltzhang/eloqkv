# KVT - Key-Value Transaction System

A high-performance, self-contained transactional key-value store with multiple concurrency control mechanisms and full ACID properties.

## Features

- **Full ACID Transactions**: Atomicity, Consistency, Isolation, and Durability guarantees
- **Multiple Concurrency Control**: Choose from Simple, 2PL, or OCC mechanisms
- **Range Scans**: Efficient ordered key traversal with range partitioning support
- **Batch Operations**: Execute multiple operations atomically within transactions
- **Multi-Table Support**: Create and manage multiple independent tables
- **Clean C API**: Simple, easy-to-use interface with comprehensive error handling

## Quick Start

### Building

The project supports two build configurations:

#### Standalone (Memory-Only)
```bash
make mem                    # Build memory-only version
./kvt_sample_mem           # Run sample program
./kvt_stress_test_mem      # Run stress test
```

#### With EloqKV Integration
```bash
make eloq                  # Build with EloqKV
./kvt_sample_eloq          # Run sample program
./kvt_stress_test_eloq     # Run stress test
```

### Basic Usage

```cpp
#include "kvt_inc.h"

int main() {
    // Initialize KVT system
    kvt_initialize();
    
    // Create a table
    std::string error;
    uint64_t table_id;
    kvt_create_table("users", "hash", table_id, error);
    
    // Start a transaction
    uint64_t tx_id;
    kvt_start_transaction(tx_id, error);
    
    // Perform operations
    kvt_set(tx_id, table_id, "user:1", "Alice", error);
    kvt_set(tx_id, table_id, "user:2", "Bob", error);
    
    // Commit transaction
    kvt_commit_transaction(tx_id, error);
    
    // One-shot read (auto-commit)
    std::string value;
    kvt_get(0, table_id, "user:1", value, error);
    
    // Cleanup
    kvt_shutdown();
    return 0;
}
```

## API Reference

### System Management

- `kvt_initialize()` - Initialize the KVT system
- `kvt_shutdown()` - Shutdown and cleanup resources

### Table Operations

- `kvt_create_table()` - Create a new table with hash or range partitioning
- `kvt_drop_table()` - Drop an existing table
- `kvt_get_table_name()` - Get table name by ID
- `kvt_get_table_id()` - Get table ID by name
- `kvt_list_tables()` - List all tables

### Transaction Control

- `kvt_start_transaction()` - Begin a new transaction
- `kvt_commit_transaction()` - Commit changes
- `kvt_rollback_transaction()` - Abort and rollback

### Data Operations

- `kvt_get()` - Read a key's value
- `kvt_set()` - Write a key-value pair
- `kvt_del()` - Delete a key
- `kvt_scan()` - Range scan (range-partitioned tables only)
- `kvt_batch_execute()` - Execute multiple operations atomically

## Concurrency Control Mechanisms

### Simple Locking
- Single active transaction at a time
- Full isolation with global locking
- Best for low-concurrency workloads

### Two-Phase Locking (2PL)
- Pessimistic concurrency control
- Locks acquired on first access
- Prevents conflicts through exclusive locking

### Optimistic Concurrency Control (OCC)
- Version-based validation
- No locks during execution
- Validation at commit time
- Best for read-heavy workloads

## Testing

### Sample Program (`kvt_sample.cpp`)
Demonstrates basic API usage including:
- Table creation and management
- Transaction operations
- CRUD operations
- Range scans
- Error handling

### Stress Test (`kvt_stress_test.cpp`)
Comprehensive testing with:
- Invariant-based consistency checking
- Three execution modes:
  - Single non-interleaved
  - Single interleaved
  - Multi-threaded
- Automatic consistency validation
- Configurable parameters

Run stress test with custom seed:
```bash
./kvt_stress_test_mem [seed]
```

## Architecture

### Directory Structure
```
eloq_kvt/
├── kvt_inc.h           # Public API interface
├── kvt_mem.h           # Internal implementation header
├── kvt_mem.cpp         # Implementation
├── kvt_sample.cpp      # Sample usage program
├── kvt_stress_test.cpp # Comprehensive stress test
├── Makefile            # Build configuration
├── README.md           # This file
├── CLAUDE.md           # Development guide
└── design.md           # Architecture documentation
```

### Key Components

1. **API Layer** (`kvt_inc.h`)
   - Clean C-style interface
   - Comprehensive error codes
   - Batch operation support

2. **Storage Engine** (`kvt_mem.h/cpp`)
   - In-memory storage with `std::map`
   - Table-key encoding for multi-table support
   - Transaction context management

3. **Concurrency Control**
   - Multiple implementations
   - Configurable at runtime
   - Full ACID compliance

## Build Options

### Debug Build
```bash
make debug_mem    # Debug build with symbols
make debug_eloq   # Debug build with EloqKV
```

### Clean
```bash
make clean        # Clean all build files
make clean_mem    # Clean memory-only build
make clean_eloq   # Clean EloqKV build
```

### Information
```bash
make info         # Show build configuration
make help         # Show all available targets
```

## Error Handling

All operations return `KVTError` codes:
- `SUCCESS` - Operation completed successfully
- `TABLE_NOT_FOUND` - Table doesn't exist
- `KEY_NOT_FOUND` - Key doesn't exist
- `TRANSACTION_HAS_STALE_DATA` - OCC validation failed
- `KEY_IS_LOCKED` - 2PL lock conflict
- And more...

Check error messages for detailed diagnostics.

## Performance Tips

1. **Choose appropriate concurrency control**:
   - Simple for single-threaded apps
   - 2PL for write-heavy workloads
   - OCC for read-heavy workloads

2. **Use batch operations** when possible to reduce overhead

3. **Use table IDs** instead of names for better performance

4. **Minimize transaction duration** to reduce conflicts

## Contributing

When modifying the codebase:
1. Maintain API compatibility in `kvt_inc.h`
2. Add tests for new features
3. Update documentation
4. Run stress tests before committing

## License

This project is part of the EloqKV database system.

## Support

For issues or questions, refer to:
- `CLAUDE.md` - Development guidelines
- `design.md` - Architecture details
- Source code documentation