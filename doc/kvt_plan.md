# KVT Implementation Plan

## Overview
We are implementing a transactional key-value store (KVT) system that operates alongside the existing Redis implementation. The system provides dynamic table creation with support for hash partitioned (default) and range partitioned tables (for scan operations). All operations work with string keys and string values.

## Architecture
The KVT system integrates with the existing transaction service (`tx_service`) and provides a simplified interface through the `KVTManager` class. Commands are received through the Redis framework and immediately redirected to KVTManager for processing.

## Key Design Decisions

### 1. Keep It Simple
- No complex inheritance hierarchies
- Direct integration with tx_service components
- Minimal abstraction layers
- Focus on string key/value operations only

### 2. Data Structures

#### KVTTable Class
A simple table metadata holder:
```cpp
class KVTTable {
public:
    enum PartitionMethod {
        HASH_PARTITIONED = 0,
        RANGE_PARTITIONED = 1
    };
    
    KVTTable(const std::string& name, PartitionMethod method)
        : table_name_(name), 
          partition_method_(method),
          table_id_(++next_table_id_) {}
    
    uint64_t table_id() const { return table_id_; }
    const std::string& name() const { return table_name_; }
    PartitionMethod partition_method() const { return partition_method_; }
    
private:
    std::string table_name_;
    PartitionMethod partition_method_;
    uint64_t table_id_;
    static std::atomic<uint64_t> next_table_id_;
};
```

#### KVTManager Member Variables
```cpp
private:
    // Transaction service components (initialized externally)
    txservice::TxService* tx_service_{nullptr};
    txservice::CatalogFactory* catalog_factory_{nullptr};
    
    // Table management
    std::unordered_map<std::string, std::unique_ptr<KVTTable>> tables_;
    std::unordered_map<uint64_t, std::string> table_id_to_name_;
    
    // Transaction management  
    std::unordered_map<uint64_t, std::unique_ptr<txservice::TransactionExecution>> active_transactions_;
    std::atomic<uint64_t> next_transaction_id_{1};
    
    // Mutex for thread safety
    std::mutex tables_mutex_;
    std::mutex transactions_mutex_;
```

### 3. Command Handling Flow

#### handleCommand Implementation
```cpp
bool KVTManager::handleCommand(const std::vector<butil::StringPiece>& args,
                              brpc::RedisReply* output) {
    if (args.size() < 2) {
        output->SetError("ERR wrong number of arguments for 'kvt' command");
        return false;
    }
    
    std::string cmd(args[1].data(), args[1].size());
    
    try {
        if (cmd == "create_table") {
            return handleCreateTable(args, output);
        } else if (cmd == "start_tx") {
            return handleStartTx(args, output);
        } else if (cmd == "get") {
            return handleGet(args, output);
        } else if (cmd == "set") {
            return handleSet(args, output);
        } else if (cmd == "scan") {
            return handleScan(args, output);
        } else if (cmd == "commit") {
            return handleCommit(args, output);
        } else if (cmd == "rollback") {
            return handleRollback(args, output);
        } else {
            output->SetError("ERR unknown kvt command: " + cmd);
            return false;
        }
    } catch (const std::exception& e) {
        output->SetError("ERR " + std::string(e.what()));
        return false;
    }
}
```

### 4. Transaction Management

#### Transaction Creation
```cpp
uint64_t KVTManager::doStartTx(std::string& error_msg) {
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    
    uint64_t tx_id = next_transaction_id_++;
    
    // Create TransactionExecution with proper handler
    auto txm = std::make_unique<txservice::TransactionExecution>(
        nullptr,  // CcHandler will be set during InitTx
        nullptr,  // TxLog 
        nullptr   // TxProcessor
    );
    
    // Initialize the transaction with default isolation level
    txm->InitTx(
        txservice::IsolationLevel::SNAPSHOT_ISOLATION,
        txservice::CcProtocol::OCC,  
        UINT32_MAX,  // tx_ng_id
        true         // start_now
    );
    
    active_transactions_[tx_id] = std::move(txm);
    return tx_id;
}
```

#### Get/Set Operations
```cpp
bool KVTManager::doGet(uint64_t tx_id, const std::string& table_name, 
                       const std::string& key, std::string& value, 
                       std::string& error_msg) {
    // Find or create transaction
    txservice::TransactionExecution* txm = nullptr;
    std::unique_ptr<txservice::TransactionExecution> one_shot_tx;
    
    if (tx_id == 0) {
        // One-shot transaction
        one_shot_tx = std::make_unique<txservice::TransactionExecution>(
            nullptr, nullptr, nullptr);
        one_shot_tx->InitTx(
            txservice::IsolationLevel::READ_COMMITTED,
            txservice::CcProtocol::OCC,
            UINT32_MAX, true);
        txm = one_shot_tx.get();
    } else {
        std::lock_guard<std::mutex> lock(transactions_mutex_);
        auto it = active_transactions_.find(tx_id);
        if (it == active_transactions_.end()) {
            error_msg = "Transaction not found: " + std::to_string(tx_id);
            return false;
        }
        txm = it->second.get();
    }
    
    // Create TableName and TxKey
    txservice::TableName tbl_name(table_name, 
                                  txservice::TableType::Primary,
                                  txservice::TableEngine::EloqKv);
    txservice::TxKey tx_key(key.data(), key.size());
    
    // Create and execute read request
    auto read_req = std::make_unique<txservice::ReadTxRequest>(
        nullptr, nullptr, txm);
    read_req->table_name_ = tbl_name;
    read_req->tx_key_ = std::move(tx_key);
    
    txm->Execute(read_req.get());
    read_req->Wait();
    
    if (read_req->IsError()) {
        error_msg = read_req->ErrorMsg();
        return false;
    }
    
    // Extract value from result
    const auto& result = read_req->Result();
    if (result.rec_ && result.rec_->RecordSize() > 0) {
        value.assign(result.rec_->RecordData(), result.rec_->RecordSize());
    } else {
        value.clear();  // Key not found
    }
    
    // Commit one-shot transaction if needed
    if (tx_id == 0 && one_shot_tx) {
        txservice::CommitTxRequest commit_req(nullptr, nullptr, txm);
        txm->CommitTx(commit_req);
    }
    
    return true;
}
```

### 5. Scan Operation for Range Partitioned Tables

```cpp
bool KVTManager::doScan(uint64_t tx_id, const std::string& table_name,
                       const std::string& key_start, const std::string& key_end,
                       size_t num_item_limit,
                       std::vector<std::pair<std::string, std::string>>& results,
                       std::string& error_msg) {
    // Check if table supports scan
    {
        std::lock_guard<std::mutex> lock(tables_mutex_);
        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            error_msg = "Table not found: " + table_name;
            return false;
        }
        if (it->second->partition_method() != KVTTable::RANGE_PARTITIONED) {
            error_msg = "Scan only supported on range partitioned tables";
            return false;
        }
    }
    
    // Get or create transaction (similar to doGet)
    txservice::TransactionExecution* txm = GetOrCreateTransaction(tx_id);
    
    // Create scan request
    txservice::TableName tbl_name(table_name,
                                 txservice::TableType::Primary,
                                 txservice::TableEngine::EloqKv);
    
    // Open scan
    auto scan_open_req = std::make_unique<txservice::ScanOpenTxRequest>(
        nullptr, nullptr, txm);
    scan_open_req->table_name_ = tbl_name;
    scan_open_req->start_key_ = txservice::TxKey(key_start.data(), key_start.size());
    scan_open_req->end_key_ = txservice::TxKey(key_end.data(), key_end.size());
    
    txm->Execute(scan_open_req.get());
    scan_open_req->Wait();
    
    if (scan_open_req->IsError()) {
        error_msg = scan_open_req->ErrorMsg();
        return false;
    }
    
    uint64_t scan_alias = scan_open_req->Result().alias_;
    
    // Fetch batch
    size_t fetched = 0;
    while (fetched < num_item_limit) {
        auto scan_batch_req = std::make_unique<txservice::ScanBatchTxRequest>(
            nullptr, nullptr, txm);
        scan_batch_req->alias_ = scan_alias;
        scan_batch_req->batch_size_ = std::min(num_item_limit - fetched, size_t(100));
        
        txm->Execute(scan_batch_req.get());
        scan_batch_req->Wait();
        
        if (scan_batch_req->IsError()) {
            break;  // End of scan or error
        }
        
        const auto& batch_result = scan_batch_req->Result();
        for (const auto& item : batch_result.tuples_) {
            results.emplace_back(
                std::string(item.key_.Data(), item.key_.Size()),
                std::string(item.rec_->RecordData(), item.rec_->RecordSize())
            );
            fetched++;
            if (fetched >= num_item_limit) break;
        }
        
        if (batch_result.has_more_ == false) break;
    }
    
    // Close scan
    auto scan_close_req = std::make_unique<txservice::ScanCloseTxRequest>(
        nullptr, nullptr, txm);
    scan_close_req->alias_ = scan_alias;
    txm->Execute(scan_close_req.get());
    scan_close_req->Wait();
    
    return true;
}
```

## Implementation Steps

### Phase 1: Basic Infrastructure
1. Implement KVTTable class definition
2. Update KVTManager constructor/destructor
3. Implement initialize() method to get tx_service and catalog_factory pointers
4. Implement handleCommand() with basic command parsing

### Phase 2: Table Management
1. Implement doCreateTable() with table metadata storage
2. Add table lookup utilities
3. Add partition method validation

### Phase 3: Transaction Management  
1. Implement doStartTx() to create TransactionExecution instances
2. Implement doCommitTx() and doAbortTx()
3. Add transaction cleanup on abort/timeout

### Phase 4: Basic Operations
1. Implement doGet() with one-shot and multi-statement transaction support
2. Implement doSet() with proper write handling
3. Add proper error handling and result formatting

### Phase 5: Scan Support
1. Implement doScan() for range partitioned tables
2. Add scan cursor management
3. Implement batch fetching with limits

### Phase 6: Integration and Testing
1. Connect KVTManager to Redis command handler
2. Initialize tx_service components properly
3. Add logging and metrics
4. Test with various workloads

## Error Handling Strategy

1. **Command Parsing Errors**: Return Redis error replies with descriptive messages
2. **Transaction Errors**: Propagate tx_service error codes with context
3. **Table Not Found**: Clear error message with table name
4. **Invalid Operations**: E.g., scan on hash-partitioned table
5. **Resource Limits**: Transaction count limits, memory limits

## Thread Safety Considerations

1. Use mutexes for table creation/deletion (infrequent operations)
2. Use atomic counters for ID generation
3. TransactionExecution instances are thread-safe internally
4. Avoid holding locks during blocking operations

## Integration Points

### With Redis Framework
- Command routing in redis_service.cpp
- Response formatting using brpc::RedisReply
- Connection/session management reused

### With Transaction Service
- Use existing TxRequest types
- Leverage TransactionExecution state machine
- Reuse isolation levels and CC protocols
- No custom catalog factory needed - use default

## Simplifications from Full Implementation

1. **No Schema Management**: Just string keys and values
2. **No Index Support**: Only primary key access and range scans
3. **No Custom Catalog**: Use existing catalog_factory as-is
4. **No Persistence**: Rely on tx_service for durability
5. **No Partitioning Logic**: Let tx_service handle distribution
6. **No Custom Types**: Everything is strings/bytes

## Code Organization

```
kvt_manager.h:
  - Class definition with public API
  - Private implementation methods
  - Member variables

kvt_manager.cpp:
  - handleCommand() - main entry point
  - Command handlers (handleXxx methods)
  - Business logic (doXxx methods)
  - Utility functions
```

This design keeps the implementation focused and simple while leveraging the existing transaction infrastructure effectively.