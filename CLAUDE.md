# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EloqKV is a high-performance distributed key-value database with Redis/ValKey API compatibility. It provides ACID transactions, full elasticity, tiered storage, and session-style transaction syntax while maintaining Redis simplicity.


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

# Project Goal
We will be building a transactional key value store. Both key and value are strings (or thinly wrapped version of strings). We will have a seperate system from the current version implemented in the RedisServiceImpl, but with very similar (but simplified) functionality. A command it sent throught the existing redis framework, but immediately redirected to the KVTManager class to handle everything. KVTManager is very similar to RedisServiceImpl, but allow dynamic table creation, and can choose range partitioned table (thus supports scan operation). 

A command is sent through the current framework and is directed and passed to KVTManager, and the result of the execution is returned. 
kvt cmd args.....

Where `cmd` and `args` can be:
- `create_table tablename partition_method` - Creates a new table
    returns a table_id (int64), or 0 if fails. 

- `start_tx` - Returns a transaction ID
    returns a transaction id, or 0 if fails. Internaly, a TransactionExecution txm is created and associated with the id. 

- `get tx_id tablename key` - Retrieves a value within a transaction
    Using the txm, get the value of a key from a table. If tx_id is not 0, find the transaction (txm) and create a TxRequest to get executed. Otherwise, if tx_id is 0, just make a one shot transaction and execute the request. 

- `set tx_id tablename key value` - Sets a value within a transaction
    Set the value of a key from a table. If tx_id is 0, just make a one shot transaction.

- `scan tx_id tablename keystart keyend num_item_limit` - Scans keys within a range
    Scan a key range. return all items upto the number limit. The return should be pairs of key, value. 

- `commit tx_id` - Commits a transaction
- `rollback tx_id` - Aborts a transaction

The implementation should be stand alone, using tx_service only, not using any EloqKV related code (such as the classes in eloqkv/include). Currently we use existing EloqKV framework to construct a KVTManager object and pass command string into it (through handleCommand()), other than that, nothing in RedisServiceImpl should be used. 

We should try to have modifications limited in KVTManager.h and KVTManager.cpp, unless absolutely necessary. We should make the code simple, try not to over engineer as we only need to deal with the opaque string:string KV operations. We treat the implementation as a prototype, not a toy, so we cannot over-simpify too much and violate the fundmental functionality (e.g. ACID properties). 

However, we should learn from EloqKV and its RedisServiceImpl to learn how a transaction is carried out. In particular, the BEGIN/COMMIT/ROLLBACK commands (which store a txm in RedisConnectionContext) path is a good reference. Inside this block put/get command are also handled. 

## The Following is Current Transation Handling in RedisServiceImpl In our KVTManager implementation, we should follow this pattern but simplify. 

## **Transaction Lifetime Analysis in RedisServiceImpl**

### **1. Major Classes Involved**

#### **Core Service Classes**
- **`RedisServiceImpl`** - Main service that handles Redis commands and transactions
- **`TxService`** - Underlying transaction service for concurrency control
- **`RedisCatalogFactory`** - Manages table schemas and catalog operations
- **`RedisConnectionContext`** - Stores transaction state per connection

#### **Transaction Management Classes**
- **`MultiTransactionHandler`** - Manages MULTI/EXEC transactions
- **`TransactionExecution`** - Represents an active transaction (stored as `txm`)
- **`BeginHandler`** - Handles BEGIN command to start transactions
- **`CommitHandler`** - Handles COMMIT command to commit transactions
- **`RollbackHandler`** - Handles ROLLBACK command to abort transactions

#### **Command Processing Classes**
- **`RedisCommandHandler`** - Base class for command handlers
- **`ObjectCommandTxRequest`** - Wraps Redis commands for transaction execution
- **`MultiObjectCommandTxRequest`** - Handles multi-key operations in transactions

### **2. Object Creation and Deletion Timeline**

#### **A. Server Startup (One-time)**
```cpp
// In main() function
auto redis_service_impl = std::make_unique<EloqKV::RedisServiceImpl>(config_file, VERSION);

// During RedisServiceImpl::Init()
tx_service_ = std::make_unique<TxService>(catalog_factory, ...);
// Creates RedisCatalogFactory and pre-built table schemas
```

**Objects Created:**
- **`RedisServiceImpl`** instance (single instance)
- **`TxService`** instance (single instance)
- **`RedisCatalogFactory`** instance (single instance)
- **Pre-built table schemas** for Redis databases (0-15 by default)

**When Deleted:**
- When server shuts down (main function ends)
- When `redis_service_impl->Stop()` is called

#### **B. Connection Establishment (Per Connection)**
```cpp
// In RedisServiceImpl::NewConnectionContext()
return std::make_unique<RedisConnectionContext>(socket, &pub_sub_mgr_);
```

**Objects Created:**
- **`RedisConnectionContext`** instance (per client connection)
- **`brpc::RedisReply`** for output handling
- **`butil::Arena`** for memory management

**When Deleted:**
- When client disconnects
- When connection context is destroyed

#### **C. Transaction Start (Per Transaction)**
```cpp
// In BeginHandler::Run()
ctx->txm = redis_impl_->NewTxm(redis_impl_->GetTxnIsolationLevel(),
                               redis_impl_->GetTxnProtocol());
```

**Objects Created:**
- **`TransactionExecution`** instance (stored as `ctx->txm`)
- **`MultiTransactionHandler`** (if using MULTI/EXEC)

**When Deleted:**
- **`TransactionExecution`**: After COMMIT/ROLLBACK or connection close
- **`MultiTransactionHandler`**: After EXEC/DISCARD or connection close

#### **D. Command Processing (Per Command)**
```cpp
// In ExecuteCommand()
ObjectCommandTxRequest tx_req(RedisTableName(ctx->db_id), &key, cmd, ...);
return ExecuteTxRequest(txm, &tx_req, output, output);
```

**Objects Created:**
- **`ObjectCommandTxRequest`** instances (per command)
- **`EloqKey`** objects for key handling
- **`RedisCommand`** objects for command execution

**When Deleted:**
- After command execution completes
- When transaction is committed/aborted

### **3. Transaction Flow and Table Selection**

#### **A. Table Selection**
```cpp
// Tables are pre-created during server startup
for (int i = 0; i < databases; i++) {
    std::string table_name("data_table_" + std::to_string(i));
    TableName redis_table_name(table_name, TableType::Primary, TableEngine::EloqKv);
    redis_table_names_.push_back(redis_table_name);
}

// Table selection during command execution
const TableName *RedisServiceImpl::RedisTableName(int db_id) const {
    return &redis_table_names_[db_id];
}
```

**Key Points:**
- **Tables are created once** during server startup
- **No runtime table creation** - uses pre-built schemas
- **Table selection** is based on `ctx->db_id` (SELECT command)
- **Default 16 databases** (0-15) with tables named `data_table_0` through `data_table_15`

#### **B. Transaction Start**
```cpp
// BEGIN command
ctx->txm = redis_impl_->NewTxm(redis_impl_->GetTxnIsolationLevel(),
                               redis_impl_->GetTxnProtocol());

// MULTI command
ctx->multi_transaction_handler.reset(new MultiTransactionHandler(this));
ctx->multi_transaction_handler->Begin();
ctx->in_multi_transaction = true;
```

#### **C. Command Execution in Transaction**
```cpp
// Commands are wrapped in ObjectCommandTxRequest
ObjectCommandTxRequest tx_req(RedisTableName(ctx->db_id), &key, cmd, 
                             auto_commit, always_redirect, txm);

// Commands are executed through TxService
return ExecuteTxRequest(txm, &tx_req, output, output);
```

#### **D. Transaction Completion**
```cpp
// COMMIT
auto [success, err_code] = CommitTx(ctx->txm);
ctx->txm = nullptr;

// ROLLBACK
AbortTx(ctx->txm);
ctx->txm = nullptr;

// MULTI/EXEC
TxErrorCode tx_err_code = redis_impl_->MultiExec(cmd_reqs_, raw_cmd_args_, 
                                                output, txm_, ctx);
```

### **4. Memory Management and Lifecycle**

#### **A. Long-lived Objects (Server Lifetime)**
- **`RedisServiceImpl`** - Single instance, managed by BRPC server
- **`TxService`** - Single instance, managed by RedisServiceImpl
- **`RedisCatalogFactory`** - Single instance, managed by RedisServiceImpl
- **Table schemas** - Pre-built, never deleted during server runtime

#### **B. Medium-lived Objects (Connection Lifetime)**
- **`RedisConnectionContext`** - Per connection, managed by BRPC
- **`TransactionExecution`** - Per transaction, managed by connection context
- **`MultiTransactionHandler`** - Per MULTI transaction, managed by connection context

#### **C. Short-lived Objects (Command Lifetime)**
- **`ObjectCommandTxRequest`** - Per command, created and destroyed during execution
- **`EloqKey`** - Per key operation, temporary during command processing
- **`RedisCommand`** - Per command, temporary during command processing

### **5. Key Design Patterns**

#### **A. Single Service Instance**
- **One RedisServiceImpl** serves all connections
- **One TxService** handles all transactions
- **One catalog factory** manages all table schemas

#### **B. Per-Connection State**
- **Transaction state** (`txm`) stored in `RedisConnectionContext`
- **Multi-transaction state** stored in `MultiTransactionHandler`
- **Database selection** (`db_id`) stored in connection context

#### **C. Pre-built Infrastructure**
- **Tables created once** at startup, never modified
- **Schemas immutable** after initialization
- **No runtime catalog changes** - static table structure

This architecture provides **excellent performance** through pre-built infrastructure while maintaining **clean separation** between service, connection, and transaction lifecycle management.

# Now reference the previous sections to implement KVTManager and achieve our goal.

