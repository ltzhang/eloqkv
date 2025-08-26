# KVT Command Technical Analysis

## Deep Dive into Implementation Details

### 1. Command Structure Analysis

#### 1.1 Redis Command Integration
The KVT command needs to integrate seamlessly with the existing Redis command infrastructure. Based on the codebase analysis:

```cpp
// Current command structure in redis_command.cpp - MINIMAL ADDITION ONLY
enum struct RedisCommandType {
    // ... existing commands ...
    KVT,  // New addition - ONLY THIS LINE ADDED
};

// Command handler signature - MINIMAL STUB ONLY
void kvtCommand(RedisCommandContext& ctx) {
    // Delegate to KVT handler - minimal stub only
    KvtHandler::handleCommand(ctx);
}
```

**Note**: The `redis_command.cpp` file only gets a minimal stub. All actual implementation goes in dedicated KVT handler files.

#### 1.2 Subcommand Architecture
KVT will use Redis's subcommand pattern, similar to CLUSTER and CLIENT commands:

```cpp
// Subcommand structure - GOES IN commands.def FILE
struct KVT_Subcommands[] = {
    {MAKE_CMD("create_table", "Creates a new table", ...)},
    {MAKE_CMD("start_tx", "Starts a new transaction", ...)},
    {MAKE_CMD("get", "Gets a value within transaction", ...)},
    {MAKE_CMD("set", "Sets a value within transaction", ...)},
    {MAKE_CMD("scan", "Scans keys within range", ...)},
    {MAKE_CMD("read_cursor", "Continues cursor reading", ...)},
    {MAKE_CMD("commit_tx", "Commits transaction", ...)},
    {MAKE_CMD("abort_tx", "Aborts transaction", ...)},
};
```

**Note**: This subcommand structure goes in `commands.def`. The actual subcommand handling logic goes in `kvt_handler.cpp`.

### 2. Transaction Service Integration

#### 2.1 Existing Transaction Infrastructure
The codebase already has a robust transaction service (`tx_service/`) that provides:

- Transaction lifecycle management
- Concurrency control
- Conflict resolution
- Data persistence

#### 2.2 Single-Operation Transaction Handling
For transaction ID 0, operations are processed immediately without creating a transaction context:

```cpp
// Special handling for single operations (tx_id = 0)
if (tx_id == 0) {
    // Process immediately through transaction service
    // No transaction context needed
    // Similar to Redis GET/SET commands
    return processImmediateOperation(table_name, key, value);
}
```

This approach provides:
- **Immediate Execution**: No transaction overhead for single operations
- **Redis Compatibility**: Behaves like standard Redis commands
- **Performance**: Bypasses transaction context creation and management
- **Simplicity**: Direct path to storage layer

#### 2.2 Integration Points
```cpp
// Key integration classes
#include "tx_service/include/tx_service.h"
#include "tx_service/include/tx_execution.h"
#include "tx_service/include/tx_worker_pool.h"
#include "tx_service/include/catalog_factory.h"

// Transaction service instance
txservice::TxService* tx_service_;
```

#### 2.3 Transaction Context Management
**File**: `src/kvt_handler.cpp` (NEW FILE)
```cpp
class KvtTransactionContext {
private:
    txservice::TxId tx_id_;
    std::string table_name_;
    std::unordered_map<std::string, std::string> write_set_;
    std::unordered_set<std::string> read_set_;
    bool is_active_;
    
public:
    KvtTransactionContext(txservice::TxId tx_id);
    bool addWrite(const std::string& key, const std::string& value);
    bool addRead(const std::string& key);
    bool commit();
    bool abort();
    // ... other methods
};

// Centralized Transaction Manager
class KvtTransactionManager {
private:
    std::unordered_map<txservice::TxId, std::shared_ptr<KvtTransactionContext>> transactions_;
    std::mutex transactions_mutex_;
    std::atomic<txservice::TxId> next_tx_id_;
    
public:
    static KvtTransactionManager& getInstance();
    std::shared_ptr<KvtTransactionContext> getTransaction(txservice::TxId tx_id);
    txservice::TxId createTransaction();
    bool removeTransaction(txservice::TxId tx_id);
    void cleanupExpiredTransactions();
    
private:
    KvtTransactionManager() = default;
    ~KvtTransactionManager() = default;
};
```

**Note**: All these classes go in dedicated KVT handler files, not in existing Redis files.

### 3. Data Model and Storage
**File**: `src/kvt_handler.cpp` (NEW FILE)

#### 3.1 Table Schema
```cpp
struct KvtTableSchema {
    std::string table_name;
    std::string partition_method;
    std::map<std::string, std::string> metadata;
    std::vector<std::string> indexes;
    
    // Serialization methods
    bool serialize(std::string& output) const;
    bool deserialize(const std::string& input);
};
```

**Note**: All data model structures go in dedicated KVT handler files.

#### 3.2 Key-Value Storage
**File**: `src/kvt_handler.cpp` (NEW FILE)
```cpp
struct KvtKeyValue {
    std::string table_name;
    std::string key;
    std::string value;
    uint64_t timestamp;
    uint64_t transaction_id;
    
    // Key composition: table:key
    std::string getCompositeKey() const {
        return table_name + ":" + key;
    }
};
```

**Note**: All data structures go in dedicated KVT handler files.

#### 3.3 Partition Methods
**File**: `src/kvt_handler.cpp` (NEW FILE)
```cpp
enum class PartitionMethod {
    HASH,           // Hash-based partitioning
    RANGE,          // Range-based partitioning
    ROUND_ROBIN,    // Round-robin distribution
    CUSTOM          // Custom partitioning function
};

class PartitionManager {
public:
    virtual uint32_t getPartition(const std::string& key) = 0;
    virtual std::vector<uint32_t> getPartitions(const std::string& start, const std::string& end) = 0;
};
```

**Note**: All partition management logic goes in dedicated KVT handler files.

### 4. Command Implementation Details
**File**: `src/kvt_handler.cpp` (NEW FILE)

#### 4.1 Main Command Handler
```cpp
// This goes in kvt_manager.cpp, NOT in redis_command.cpp
void KvtManager::handleCommand(RedisCommandContext& ctx) {
    if (ctx.argc < 2) {
        addReplyError(ctx, "ERR wrong number of arguments for KVT command");
        return;
    }
    
    std::string subcommand = std::string(ctx.argv[1]->ptr, ctx.argv[1]->len);
    
    if (subcommand == "create_table") {
        handleCreateTable(ctx);
    } else if (subcommand == "start_tx") {
        handleStartTx(ctx);
    } else if (subcommand == "get") {
        handleGet(ctx);
    } else if (subcommand == "set") {
        handleSet(ctx);
    } else if (subcommand == "scan") {
        handleScan(ctx);
    } else if (subcommand == "read_cursor") {
        handleReadCursor(ctx);
    } else if (subcommand == "commit_tx") {
        handleCommitTx(ctx);
    } else if (subcommand == "abort_tx") {
        handleAbortTx(ctx);
    } else {
        addReplyError(ctx, "ERR unknown subcommand or wrong number of arguments");
    }
}
```

**Note**: This main command handler goes in `kvt_manager.cpp`. The `redis_command.cpp` only gets a minimal stub that calls this.

#### 4.2 Subcommand Handlers
**File**: `src/kvt_manager.cpp` (NEW FILE)

##### Create Table Handler
```cpp
void KvtManager::handleCreateTable(RedisCommandContext& ctx) {
    if (ctx.argc != 4) {
        addReplyError(ctx, "ERR wrong number of arguments for 'create_table' subcommand");
        return;
    }
    
    std::string table_name = std::string(ctx.argv[2]->ptr, ctx.argv[2]->len);
    std::string partition_method = std::string(ctx.argv[3]->ptr, ctx.argv[3]->len);
    
    // Validate table name
    if (!isValidTableName(table_name)) {
        addReplyError(ctx, "ERR invalid table name");
        return;
    }
    
    // Validate partition method
    PartitionMethod method = parsePartitionMethod(partition_method);
    if (method == PartitionMethod::CUSTOM) {
        addReplyError(ctx, "ERR invalid partition method");
        return;
    }
    
    // Create table through transaction service
    try {
        auto schema = std::make_unique<KvtTableSchema>();
        schema->table_name = table_name;
        schema->partition_method = partition_method;
        
        bool success = tx_service_->createTable(*schema);
        if (success) {
            addReplySimpleString(ctx, "OK");
        } else {
            addReplyError(ctx, "ERR failed to create table");
        }
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

##### Start Transaction Handler
```cpp
void handleStartTx(RedisCommandContext& ctx) {
    if (ctx.argc != 2) {
        addReplyError(ctx, "ERR wrong number of arguments for 'start_tx' subcommand");
        return;
    }
    
    try {
        // Generate new transaction ID through centralized manager
        txservice::TxId tx_id = KvtTransactionManager::getInstance().createTransaction();
        
        // Return transaction ID
        addReplyBulkLongLong(ctx, tx_id);
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR failed to start transaction");
    }
}
```

##### Get Handler
```cpp
void handleGet(RedisCommandContext& ctx) {
    if (ctx.argc != 4) {
        addReplyError(ctx, "ERR wrong number of arguments for 'get' subcommand");
        return;
    }
    
    // Parse arguments
    txservice::TxId tx_id = parseTxId(ctx.argv[2]);
    std::string table_name = std::string(ctx.argv[3]->ptr, ctx.argv[3]->len);
    std::string key = std::string(ctx.argv[4]->ptr, ctx.argv[4]->len);
    
    // Handle single-operation transaction (tx_id = 0)
    if (tx_id == 0) {
        try {
            std::string value;
            bool found = tx_service_->getValueImmediate(table_name, key, value);
            
            if (found) {
                addReplyBulk(ctx, value);
            } else {
                addReply(ctx, shared.nullbulk);
            }
        } catch (const std::exception& e) {
            addReplyError(ctx, "ERR internal error");
        }
        return;
    }
    
    // Validate multi-operation transaction
    auto tx_context = KvtTransactionManager::getInstance().getTransaction(tx_id);
    if (!tx_context || !tx_context->isActive()) {
        addReplyError(ctx, "ERR invalid or inactive transaction");
        return;
    }
    
    try {
        // Check write set first (for read-your-writes)
        auto write_it = tx_context->getWriteSet().find(key);
        if (write_it != tx_context->getWriteSet().end()) {
            addReplyBulk(ctx, write_it->second);
            return;
        }
        
        // Read from storage
        std::string value;
        bool found = tx_service_->getValue(tx_id, table_name, key, value);
        
        if (found) {
            addReplyBulk(ctx, value);
            tx_context->addRead(key);
        } else {
            addReply(ctx, shared.nullbulk);
        }
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

##### Set Handler
```cpp
void handleSet(RedisCommandContext& ctx) {
    if (ctx.argc != 5) {
        addReplyError(ctx, "ERR wrong number of arguments for 'set' subcommand");
        return;
    }
    
    // Parse arguments
    txservice::TxId tx_id = parseTxId(ctx.argv[2]);
    std::string table_name = std::string(ctx.argv[3]->ptr, ctx.argv[3]->len);
    std::string key = std::string(ctx.argv[4]->ptr, ctx.argv[4]->len);
    std::string value = std::string(ctx.argv[5]->ptr, ctx.argv[5]->len);
    
    // Handle single-operation transaction (tx_id = 0)
    if (tx_id == 0) {
        try {
            bool success = tx_service_->setValueImmediate(table_name, key, value);
            
            if (success) {
                addReplySimpleString(ctx, "OK");
            } else {
                addReplyError(ctx, "ERR failed to set value");
            }
        } catch (const std::exception& e) {
            addReplyError(ctx, "ERR internal error");
        }
        return;
    }
    
    // Validate multi-operation transaction
    auto tx_context = KvtTransactionManager::getInstance().getTransaction(tx_id);
    if (!tx_context || !tx_context->isActive()) {
        addReplyError(ctx, "ERR invalid or inactive transaction");
        return;
    }
    
    try {
        // Add to write set
        bool success = tx_context->addWrite(key, value);
        if (success) {
            addReplySimpleString(ctx, "OK");
        } else {
            addReplyError(ctx, "ERR failed to add write operation");
        }
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

##### Scan Handler
```cpp
void handleScan(RedisCommandContext& ctx) {
    if (ctx.argc != 5) {
        addReplyError(ctx, "ERR wrong number of arguments for 'scan' subcommand");
        return;
    }
    
    // Parse arguments
    txservice::TxId tx_id = parseTxId(ctx.argv[2]);
    std::string table_name = std::string(ctx.argv[3]->ptr, ctx.argv[3]->len);
    std::string key_start = std::string(ctx.argv[4]->ptr, ctx.argv[4]->len);
    std::string key_end = std::string(ctx.argv[5]->ptr, ctx.argv[5]->len);
    
    // Validate transaction
    auto tx_context = ctx.connection->getTransactionContext(tx_id);
    if (!tx_context || !tx_context->isActive()) {
        addReplyError(ctx, "ERR invalid or inactive transaction");
        return;
    }
    
    try {
        // Create or get scanner
        auto scanner = tx_service_->createScanner(tx_id, table_name, key_start, key_end);
        
        // Get first batch of results
        std::vector<std::pair<std::string, std::string>> results;
        uint64_t cursor_id = scanner->getNextBatch(results, 100); // Default batch size
        
        // Format response: [cursor_id, [key1, value1, key2, value2, ...]]
        addReplyArray(ctx, 2);
        addReplyBulkLongLong(ctx, cursor_id);
        
        addReplyArray(ctx, results.size() * 2);
        for (const auto& kv : results) {
            addReplyBulk(ctx, kv.first);
            addReplyBulk(ctx, kv.second);
        }
        
        // Store scanner for cursor operations
        ctx.connection->storeScanner(cursor_id, scanner);
        
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

##### Read Cursor Handler
```cpp
void handleReadCursor(RedisCommandContext& ctx) {
    if (ctx.argc != 3) {
        addReplyError(ctx, "ERR wrong number of arguments for 'read_cursor' subcommand");
        return;
    }
    
    uint64_t cursor_id = parseCursorId(ctx.argv[2]);
    
    try {
        // Get stored scanner
        auto scanner = ctx.connection->getScanner(cursor_id);
        if (!scanner) {
            addReplyError(ctx, "ERR invalid cursor");
            return;
        }
        
        // Get next batch
        std::vector<std::pair<std::string, std::string>> results;
        uint64_t next_cursor = scanner->getNextBatch(results, 100);
        
        // Format response
        addReplyArray(ctx, 2);
        addReplyBulkLongLong(ctx, next_cursor);
        
        addReplyArray(ctx, results.size() * 2);
        for (const auto& kv : results) {
            addReplyBulk(ctx, kv.first);
            addReplyBulk(ctx, kv.second);
        }
        
        // Clean up if cursor is complete
        if (next_cursor == 0) {
            ctx.connection->removeScanner(cursor_id);
        }
        
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

##### Commit Transaction Handler
```cpp
void handleCommitTx(RedisCommandContext& ctx) {
    if (ctx.argc != 3) {
        addReplyError(ctx, "ERR wrong number of arguments for 'commit_tx' subcommand");
        return;
    }
    
    txservice::TxId tx_id = parseTxId(ctx.argv[2]);
    
    // Get transaction context
    auto tx_context = ctx.connection->getTransactionContext(tx_id);
    if (!tx_context || !tx_context->isActive()) {
        addReplyError(ctx, "ERR invalid or inactive transaction");
        return;
    }
    
    try {
        // Commit through transaction service
        bool success = tx_service_->commitTransaction(tx_id);
        
        if (success) {
            addReplySimpleString(ctx, "OK");
            // Clean up transaction context
            ctx.connection->removeTransactionContext(tx_id);
        } else {
            addReplyError(ctx, "ERR transaction commit failed");
        }
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

##### Abort Transaction Handler
```cpp
void handleAbortTx(RedisCommandContext& ctx) {
    if (ctx.argc != 3) {
        addReplyError(ctx, "ERR wrong number of arguments for 'abort_tx' subcommand");
        return;
    }
    
    txservice::TxId tx_id = parseTxId(ctx.argv[2]);
    
    // Get transaction context
    auto tx_context = ctx.connection->getTransactionContext(tx_id);
    if (!tx_context || !tx_context->isActive()) {
        addReplyError(ctx, "ERR invalid or inactive transaction");
        return;
    }
    
    try {
        // Abort through transaction service
        bool success = tx_service_->abortTransaction(tx_id);
        
        if (success) {
            addReplySimpleString(ctx, "OK");
            // Clean up transaction context
            ctx.connection->removeTransactionContext(tx_id);
        } else {
            addReplyError(ctx, "ERR transaction abort failed");
        }
    } catch (const std::exception& e) {
        addReplyError(ctx, "ERR internal error");
    }
}
```

### 5. Connection Context Management

#### 5.1 Transaction Context Storage
```cpp
class RedisConnectionContext {
private:
    // Note: Transaction contexts are now managed centrally, not per connection
    std::unordered_map<uint64_t, std::shared_ptr<Scanner>> scanners_;
    std::mutex context_mutex_;
    
public:
    // Scanner management methods
    std::shared_ptr<Scanner> getScanner(uint64_t cursor_id);
    void storeScanner(uint64_t cursor_id, std::shared_ptr<Scanner> scanner);
    void removeScanner(uint64_t cursor_id);
    
    void cleanup(); // Called on connection close
};

// Centralized Transaction Manager (Singleton)
class KvtTransactionManager {
private:
    std::unordered_map<txservice::TxId, std::shared_ptr<KvtTransactionContext>> transactions_;
    std::mutex transactions_mutex_;
    std::atomic<txservice::TxId> next_tx_id_;
    
public:
    static KvtTransactionManager& getInstance();
    std::shared_ptr<KvtTransactionContext> getTransaction(txservice::TxId tx_id);
    txservice::TxId createTransaction();
    bool removeTransaction(txservice::TxId tx_id);
    void cleanupExpiredTransactions();
    
private:
    KvtTransactionManager() = default;
    ~KvtTransactionManager() = default;
};
```

#### 5.2 Scanner Management
```cpp
class Scanner {
private:
    std::string table_name_;
    std::string key_start_;
    std::string key_end_;
    std::string current_position_;
    uint32_t batch_size_;
    
public:
    Scanner(const std::string& table_name, const std::string& start, const std::string& end);
    uint64_t getNextBatch(std::vector<std::pair<std::string, std::string>>& results, uint32_t max_count);
    bool hasMore() const;
    void reset();
};
```

### 6. Error Handling and Validation

#### 6.1 Error Codes
```cpp
enum class KvtErrorCode {
    SUCCESS = 0,
    INVALID_TRANSACTION = 1,
    INVALID_TABLE = 2,
    INVALID_KEY = 3,
    TRANSACTION_CONFLICT = 4,
    INSUFFICIENT_PERMISSIONS = 5,
    RESOURCE_EXHAUSTED = 6,
    INTERNAL_ERROR = 7
};

struct KvtError {
    KvtErrorCode code;
    std::string message;
    std::string details;
};
```

#### 6.2 Input Validation
```cpp
class KvtValidator {
public:
    static bool isValidTableName(const std::string& name);
    static bool isValidKey(const std::string& key);
    static bool isValidPartitionMethod(const std::string& method);
    static txservice::TxId parseTxId(robj* obj);
    static uint64_t parseCursorId(robj* obj);
    
private:
    static const std::regex TABLE_NAME_REGEX;
    static const std::regex KEY_REGEX;
};
```

### 7. Performance Considerations

#### 7.1 Batch Operations
```cpp
class KvtBatchProcessor {
public:
    void addOperation(const KvtOperation& op);
    bool executeBatch();
    void clear();
    
private:
    std::vector<KvtOperation> operations_;
    size_t max_batch_size_;
};
```

#### 7.2 Connection Pooling
```cpp
class KvtConnectionPool {
public:
    std::shared_ptr<RedisConnectionContext> acquireConnection();
    void releaseConnection(std::shared_ptr<RedisConnectionContext> conn);
    
private:
    std::queue<std::shared_ptr<RedisConnectionContext>> available_connections_;
    std::mutex pool_mutex_;
    size_t max_connections_;
};
```

### 8. Testing Strategy

#### 8.1 Unit Tests
```cpp
TEST(KvtCommandTest, CreateTableValidInput) {
    // Test table creation with valid input
}

TEST(KvtCommandTest, StartTxGeneratesUniqueId) {
    // Test transaction ID uniqueness
}

TEST(KvtCommandTest, GetSetOperations) {
    // Test get/set within transaction
}
```

#### 8.2 Integration Tests
```cpp
TEST(KvtIntegrationTest, CompleteTransactionFlow) {
    // Test complete transaction lifecycle
}

TEST(KvtIntegrationTest, ConcurrentTransactions) {
    // Test concurrent transaction handling
}
```

#### 8.3 Performance Tests
```cpp
TEST(KvtPerformanceTest, TransactionThroughput) {
    // Measure transactions per second
}

TEST(KvtPerformanceTest, ScanningPerformance) {
    // Measure scan operation performance
}
```

### 9. Monitoring and Metrics

#### 9.1 Metrics Collection
```cpp
class KvtMetrics {
public:
    void recordTransactionStart();
    void recordTransactionCommit();
    void recordTransactionAbort();
    void recordOperation(const std::string& operation_type, uint64_t duration_us);
    
private:
    metrics::Counter transaction_counter_;
    metrics::Histogram operation_duration_;
    metrics::Gauge active_transactions_;
};
```

#### 9.2 Health Checks
```cpp
class KvtHealthChecker {
public:
    bool checkTransactionService();
    bool checkStorageBackend();
    bool checkConnectionPool();
    
private:
    std::chrono::steady_clock::time_point last_check_;
    std::chrono::milliseconds check_interval_;
};
```

This technical analysis provides the foundation for implementing the KVT command with proper integration into the existing EloqKV architecture while maintaining Redis compatibility and performance standards.

## Benefits of Centralized Transaction Management

### 1. **Improved Resource Management**
- **Centralized Cleanup**: Single point for managing transaction lifecycle
- **Memory Efficiency**: No duplicate transaction contexts across connections
- **Resource Pooling**: Better utilization of system resources

### 2. **Enhanced Performance**
- **Single-Operation Mode**: Transaction ID 0 provides Redis-like performance for simple operations
- **Reduced Overhead**: No transaction context creation for immediate operations
- **Optimized Lookups**: Centralized hash map for transaction retrieval

### 3. **Better Scalability**
- **Connection Independence**: Transactions persist across connection changes
- **Load Distribution**: Transactions can be processed by any worker thread
- **Resource Sharing**: Efficient sharing of transaction contexts

### 4. **Simplified Architecture**
- **Clear Separation**: Transaction management separated from connection management
- **Easier Testing**: Centralized logic easier to unit test
- **Maintenance**: Single code path for transaction operations

### 5. **Redis Compatibility**
- **Standard Behavior**: Transaction ID 0 behaves like traditional Redis commands
- **Seamless Integration**: Existing Redis clients work without modification
- **Performance Parity**: Single operations maintain Redis performance characteristics

## File Organization and Implementation Strategy

### **Existing Files - MINIMAL MODIFICATIONS ONLY**

#### 1. **`include/redis_command.h`**
```cpp
// ONLY ADD THIS ONE LINE to RedisCommandType enum
KVT,
```

#### 2. **`include/redis/commands.def`**
```cpp
// ONLY ADD KVT command entry to redisCommandTable[]
{MAKE_CMD("kvt", "Key-Value Transaction operations", ...)},
```

#### 3. **`src/redis_command.cpp`**
```cpp
// ONLY ADD minimal routing stub
case RedisCommandType::KVT:
    kvtCommand(ctx);
    break;

// ONLY ADD minimal function stub
void kvtCommand(RedisCommandContext& ctx) {
    KvtHandler::handleCommand(ctx);  // Delegate to KVT handler
}
```

### **New Files - ALL IMPLEMENTATION LOGIC**

#### 1. **`include/kvt_manager.h`** - Complete KVT Manager Interface
```cpp
class KvtManager {
public:
    static void handleCommand(RedisCommandContext& ctx);
    
private:
    static void handleCreateTable(RedisCommandContext& ctx);
    static void handleStartTx(RedisCommandContext& ctx);
    static void handleGet(RedisCommandContext& ctx);
    static void handleSet(RedisCommandContext& ctx);
    static void handleScan(RedisCommandContext& ctx);
    static void handleReadCursor(RedisCommandContext& ctx);
    static void handleCommitTx(RedisCommandContext& ctx);
    static void handleAbortTx(RedisCommandContext& ctx);
};
```

#### 2. **`src/kvt_manager.cpp`** - Complete KVT Implementation
- All subcommand handlers
- Transaction management logic
- Error handling and response formatting
- Data model implementations
- Integration with transaction service

#### 3. **`src/kvt_transaction.h/cpp`** - Transaction Context Classes
- `KvtTransactionContext`
- `KvtTransactionManager`
- All transaction-related logic

### **Key Implementation Principle**
- **Existing Redis files**: Only command parsing and routing stubs
- **New KVT files**: All business logic, transaction management, and implementation details
- **Clean separation**: Maintain existing Redis architecture while adding KVT functionality
- **Minimal impact**: Existing Redis code remains largely unchanged
