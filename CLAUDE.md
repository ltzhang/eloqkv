# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EloqKV is a high-performance distributed key-value database with Redis/ValKey API compatibility. It provides ACID transactions, full elasticity, tiered storage, and session-style transaction syntax.


## Build System and Commands

### Essential Build Commands
```bash
cd build 
make eloqkv -j 6

# Run EloqKV server
cd build && rm -rf eloq_data && ./eloqkv

### High-Level Architecture

EloqKV follows a decoupled architecture with three main layers:

1. **Frontend**: Redis protocol compatibility (`src/redis_*.cpp`)
2. **TxService**: Transaction management and concurrency control (`tx_service/`)
3. **Storage**: Pluggable backend storage (`store_handler/`)

### Key Components

#### Core Service Layer
- `src/redis_service.cpp` - Main Redis-compatible service implementation
- `src/redis_handler.cpp` - Command routing and processing
- `include/redis_service.h` - Service interface definitions

#### Transaction System
- `tx_service/` - Complete transaction processing system
- `include/kvt_manager.h` - KVT (Key-Value Table) transaction manager
- `src/kvt_manager.cpp` - KVT command handling implementation

#### Storage Backends
- `store_handler/` - Pluggable storage implementations
- `store_handler/rocksdb_handler.cpp` - RocksDB backend
- `store_handler/dynamo_handler.cpp` - DynamoDB backend

#### Data Structures
- `src/redis_*_object.cpp` - Redis data type implementations (string, hash, list, set, zset)
- `include/redis_object.h` - Object interface definitions

### Transaction Flow

EloqKV supports two transaction patterns:

1. **Redis-style**: `MULTI/EXEC/DISCARD`
2. **Session-style**: `BEGIN/COMMIT/ROLLBACK` (enhanced ACID support)

Transaction state is managed per connection in `RedisConnectionContext`, with `TransactionExecution` objects (`txm`) handling the actual transaction lifecycle.

### Module System

- `lua/` - Embedded Lua interpreter for scripting
- `wasm/` - WebAssembly runtime support for user-defined functions
- `eloq_metrics/` - Prometheus-compatible metrics collection

## Development Patterns

### Adding New Commands

1. Add command definition to `include/redis/commands.def`
2. Implement handler in appropriate `src/redis_*_object.cpp` file
3. Update command routing in `src/redis_command.cpp`

### Storage Backend Integration

Storage backends implement the interface defined in `store_handler/kv_store.h`. New backends should:
- Handle key-value operations (get, put, delete, scan)
- Support transaction isolation
- Implement proper error handling

### Transaction Integration

For transaction-aware commands:
- Use `ObjectCommandTxRequest` to wrap operations
- Respect transaction boundaries via `TransactionExecution`
- Handle both auto-commit and explicit transaction modes

## Important Implementation Details

### Memory Management
- Uses `butil::Arena` for connection-scoped memory allocation
- RAII patterns for resource management
- Custom allocators for high-performance scenarios

### Concurrency Control
- Thread-per-core execution model
- Lock-free data structures where possible
- Message-passing between components

### Error Handling
- Consistent error codes defined in `include/redis_errors.h`
- Graceful degradation for storage backend failures
- Transaction rollback on errors

## Configuration and Deployment

- Configuration via INI files (uses `INIReader.cpp`)
- Docker support with pre-built development environments
- Production deployment via EloqCtl cluster management tool
- Supports single-node and distributed cluster configurations

