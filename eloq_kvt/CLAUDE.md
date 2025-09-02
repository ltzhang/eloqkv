# KVT Memory Implementation - Development Guide

## Overview
KVT (Key-Value Transaction) is a self-contained transactional key-value store implementation with full ACID properties. The project provides multiple concurrency control mechanisms and comprehensive testing framework.

## Current Implementation Status

### Core Components
- **kvt_inc.h**: Public API interface - provides a clean C-style API for all KVT operations
- **kvt_mem.h/cpp**: Internal implementation with multiple concurrency control mechanisms:
  - `KVTMemManagerNoCC`: No concurrency control (baseline)
  - `KVTMemManagerSimple`: Simple locking with single active transaction
  - `KVTMemManager2PL`: Two-phase locking protocol
  - `KVTMemManagerOCC`: Optimistic concurrency control with version checking

### Test Infrastructure
- **kvt_sample.cpp**: Demonstrates API usage with basic operations and transactions
- **kvt_stress_test.cpp**: Comprehensive stress testing with invariant checking

## Stress Test Design

The stress test validates transactional consistency using a constraint-based approach:

### Key Concepts
- **Key Range**: 0-9999 (fixed-width string format for proper ordering)
- **Value Range**: 0-999 (string format)
- **Consistency Ranges**: Keys grouped into 100-key ranges (0-99, 100-199, etc.)
- **Invariant**: Sum of values in each 100-key range must be divisible by 100

### Transaction Constraints
Each transaction must maintain the invariant for all modified ranges:
- When deleting a key, must adjust other keys in the same range to maintain sum divisibility
- When adding keys, must ensure the total change is divisible by 100
- Transactions violating constraints are intentionally aborted

### Test Modes
1. **Single Non-Interleaved**: Sequential transaction execution
2. **Single Interleaved**: Multiple concurrent transactions, single-threaded
3. **Multi-Threaded**: True concurrent execution with multiple threads

### Test Process
1. **Initialization**: Populate ~2000 keys ensuring initial consistency
2. **Transaction Generation**: 
   - 1-20 operations per transaction
   - 1-4 consistency ranges per transaction
   - Predetermined commit/abort decision (70% commit ratio)
3. **Execution**: Random operations with constraint tracking
4. **Validation**: Periodic consistency checks via full or range scans
5. **Error Detection**: Immediate test termination on constraint violation

## Build System

The Makefile supports two build configurations:

### Memory-Only Version (Standalone)
```bash
make mem              # Build memory-only version
./kvt_sample_mem      # Run sample program
./kvt_stress_test_mem # Run stress test
```

### ELOQ Version (Integrated with EloqKV)
```bash
make eloq             # Build with EloqKV integration
./kvt_sample_eloq     # Run sample program  
./kvt_stress_test_eloq # Run stress test
```

## Implementation Notes

### Concurrency Control Mechanisms

#### No Concurrency Control (NoCC)
- Direct operations on shared data structure
- No transaction isolation
- Used as baseline for testing

#### Simple Locking
- Single active transaction at a time
- Write-set and delete-set tracking
- Immediate visibility of changes within transaction

#### Two-Phase Locking (2PL)
- Lock acquisition on first access
- Locks held until commit/rollback
- Prevents concurrent access to locked keys

#### Optimistic Concurrency Control (OCC)
- Version tracking for all entries
- Validation at commit time
- Aborts on version conflicts

### Key Design Decisions

1. **Fixed-Width Keys**: Ensures proper string ordering for range operations
2. **Table-Key Encoding**: 8-byte table ID prefix for multi-table support
3. **Metadata Field**: Dual purpose - lock holder (2PL) or version (OCC)
4. **Batch Operations**: Default implementation with per-operation execution

## Testing Guidelines

When running stress tests:
- Start with single non-interleaved mode for baseline
- Progress to interleaved mode to test isolation
- Use multi-threaded mode for concurrency validation
- Monitor for any constraint violations (test will exit on error)
- Consistency checks run every 100 transactions

## Future Enhancements
- Integration with EloqKV's tx_service for distributed transactions
- Performance optimization for batch operations
- Additional concurrency control mechanisms (MVCC)
- Extended API for advanced transaction features 

