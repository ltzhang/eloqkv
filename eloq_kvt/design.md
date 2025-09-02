# KVT System Architecture Design

## Overview

KVT (Key-Value Transaction) is a transactional key-value database system that provides full ACID properties with multiple concurrency control mechanisms. The system is designed as a self-contained module that can operate independently or integrate with larger database systems like EloqKV.

## Architecture Components

### 1. API Layer (kvt_inc.h)

The public API provides a clean C-style interface for all database operations:
- **Table Management**: Create, drop, list tables with hash or range partitioning
- **Transaction Control**: Start, commit, rollback transactions
- **Data Operations**: Get, set, delete, scan operations
- **Batch Operations**: Execute multiple operations atomically

Key design decisions:
- Use of table IDs instead of names for efficient operation routing
- Support for both one-shot (tx_id=0) and multi-operation transactions
- Comprehensive error codes for precise error handling

### 2. Storage Engine (kvt_mem.h/cpp)

The in-memory storage engine implements multiple concurrency control strategies:

#### Data Structures
- **Tables**: `std::map<table_name, Table>` - Collection of all tables
- **Table Data**: `std::map<key, Entry>` - Ordered key-value storage per table
- **Entry**: Contains data string and metadata (version/lock information)
- **Transactions**: `std::map<tx_id, Transaction>` - Active transaction tracking

#### Table-Key Encoding
Keys are encoded with an 8-byte table ID prefix for multi-table support:
```
[8 bytes: table_id][variable: user_key] -> value
```
This allows efficient range scans within a single table while maintaining global key uniqueness.

### 3. Concurrency Control Mechanisms

#### 3.1 No Concurrency Control (NoCC)
- Baseline implementation for testing
- Direct operations on shared data structures
- No isolation between transactions

#### 3.2 Simple Locking (KVTMemManagerSimple)
- Single active transaction at a time
- Maintains write-set and delete-set
- Changes visible within transaction before commit
- Global mutex for all operations

#### 3.3 Two-Phase Locking (2PL)
- **Lock Acquisition**: Locks acquired on first access (read or write)
- **Lock Storage**: Entry metadata stores locking transaction ID
- **Lock Release**: All locks released at commit/rollback
- **Deadlock Prevention**: Transaction aborts if lock unavailable

Implementation details:
- Metadata = 0: Unlocked
- Metadata = tx_id: Locked by transaction tx_id
- Metadata = -1: Deleted entry

#### 3.4 Optimistic Concurrency Control (OCC)
- **Read Phase**: Track versions of all read entries
- **Validation Phase**: Check versions haven't changed at commit
- **Write Phase**: Apply changes and increment versions

Implementation details:
- Metadata stores version number (incremented on each write)
- Read-set tracks original versions
- Validation fails if any version changed
- Deleted entries tracked in read-set for version checking

### 4. Transaction Lifecycle

#### Transaction States
1. **Created**: Transaction started, empty read/write sets
2. **Active**: Performing operations, building read/write/delete sets
3. **Committing**: Validating (OCC) or applying changes (2PL)
4. **Committed/Aborted**: Changes applied or discarded

#### Operation Flow
```
Start Transaction -> Build Read/Write Sets -> Validate (OCC only) -> Apply Changes -> Cleanup
```

### 5. Isolation Guarantees

The system provides different isolation levels based on the concurrency control:
- **NoCC**: No isolation (Read Uncommitted)
- **Simple**: Serializable (single transaction)
- **2PL**: Read Committed to Serializable
- **OCC**: Snapshot Isolation

## Testing Strategy

### Invariant-Based Testing

The stress test maintains a mathematical invariant across all transactions:
- Database divided into 100-key ranges
- Sum of values in each range must be divisible by 100
- Transactions must maintain this invariant or abort

### Test Execution Modes

1. **Single Non-Interleaved**: Sequential transaction execution
2. **Single Interleaved**: Concurrent transactions, single thread
3. **Multi-Threaded**: True concurrent execution

### Consistency Validation

- Initial population ensures invariant holds
- Periodic full scans verify consistency
- Immediate test termination on invariant violation
- Transaction isolation verified through concurrent reads

## Integration Points

### Standalone Mode
The system operates independently using the memory-only implementation with full transactional capabilities.

### EloqKV Integration
When integrated with EloqKV:
- Leverages tx_service for distributed transactions
- Uses catalog_factory for schema management
- Integrates with cluster-wide concurrency control

## Performance Considerations

### Optimization Strategies
- Fixed-width key encoding for efficient comparison
- Batch operation support to reduce lock overhead
- Version-based validation to minimize lock duration
- Memory-efficient entry storage with metadata reuse

### Scalability
- Table-level locking granularity
- Efficient range scan with ordered storage
- Minimal memory overhead per transaction

## Future Enhancements

1. **Multi-Version Concurrency Control (MVCC)**
   - Maintain multiple versions per key
   - Non-blocking reads
   - Time-travel queries

2. **Distributed Transaction Support**
   - Two-phase commit protocol
   - Distributed deadlock detection
   - Cross-node consistency

3. **Persistence Layer**
   - Write-ahead logging
   - Checkpoint mechanism
   - Recovery protocols

4. **Advanced Features**
   - Secondary indexes
   - Stored procedures
   - Triggers and constraints 
