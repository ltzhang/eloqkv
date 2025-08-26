#ifndef KVT_MANAGER_H
#define KVT_MANAGER_H

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <unordered_set>
#include <string_view>
#include <utility>

// Include tx_service types for enums and other types
#include "tx_service/include/type.h"
#include "tx_service/include/tx_execution.h"

// Forward declarations for tx_service components
namespace txservice {
    class TxService;
    class TransactionExecution;
    class TableName;
    class TxKey;
    class TxRecord;
    class CatalogFactory;
}

namespace EloqKV {

// Forward declarations
class KvtTransactionContext;
class KvtCursor;

// Common types for KVT operations
using KeyValuePair = std::pair<std::string, std::string>;
using KeyValueBatch = std::vector<KeyValuePair>;
using KeyBatch = std::vector<std::string>;
using TableList = std::vector<std::string>;

// Return types for different operations
struct CreateTableResult {
    bool success;
    std::string message;
};

struct DropTableResult {
    bool success;
    std::string message;
};

struct ListTablesResult {
    bool success;
    std::string message;
    TableList tables;
};

struct StartTxResult {
    bool success;
    std::string message;
    uint64_t transaction_id;
};

struct GetResult {
    bool success;
    std::string message;
    std::string value;
    bool found;
};

struct BatchGetResult {
    bool success;
    std::string message;
    KeyValueBatch results;
    std::vector<std::string> missing_keys;
};

struct SetResult {
    bool success;
    std::string message;
};

struct BatchSetResult {
    bool success;
    std::string message;
    std::vector<std::string> failed_keys;
};

struct DeleteResult {
    bool success;
    std::string message;
    bool deleted;
};

struct BatchDeleteResult {
    bool success;
    std::string message;
    std::vector<std::string> failed_keys;
};

struct ScanResult {
    bool success;
    std::string message;
    uint64_t cursor_id;
    KeyValueBatch results;
    bool has_more;
};

struct ReadCursorResult {
    bool success;
    std::string message;
    KeyValueBatch results;
    bool has_more;
};

struct CommitTxResult {
    bool success;
    std::string message;
};

struct AbortTxResult {
    bool success;
    std::string message;
};

/**
 * KVT Manager - Main class that handles KVT (Key-Value Transaction) requests
 * 
 * This class manages:
 * - Table creation and management
 * - Transaction lifecycle (start, commit, abort)
 * - Key-value operations within transactions
 * - Scanning operations with cursor management
 * - Integration with the underlying transaction service
 */
class KVTManager {
public:
    // Singleton pattern for global access
    static KVTManager& getInstance();
    
    // Main command handler entry point - parses args and formats responses
    void handleCommand(const std::vector<std::string_view>& args, std::string& output);
    
    // Initialize the manager with transaction service dependencies
    void initialize(txservice::TxService* tx_service, 
                   txservice::CatalogFactory* catalog_factory);
    
    // Cleanup and shutdown
    void shutdown();

private:
    // Private constructor for singleton
    KVTManager();
    ~KVTManager();
    
    // Disable copy and assignment
    KVTManager(const KVTManager&) = delete;
    KVTManager& operator=(const KVTManager&) = delete;

    // Business logic methods (doXXXX) - called by handleCommand
    CreateTableResult doCreateTable(const std::string& table_name, const std::string& partition_method);
    DropTableResult doDropTable(const std::string& table_name);
    ListTablesResult doListTables();
    StartTxResult doStartTx();
    GetResult doGet(uint64_t tx_id, const std::string& table_name, const std::string& key);
    BatchGetResult doBatchGet(uint64_t tx_id, const std::string& table_name, const KeyBatch& keys);
    SetResult doSet(uint64_t tx_id, const std::string& table_name, const std::string& key, const std::string& value);
    BatchSetResult doBatchSet(uint64_t tx_id, const std::string& table_name, const KeyValueBatch& key_values);
    DeleteResult doDelete(uint64_t tx_id, const std::string& table_name, const std::string& key);
    BatchDeleteResult doBatchDelete(uint64_t tx_id, const std::string& table_name, const KeyBatch& keys);
    struct ScanResult doScan(uint64_t tx_id, const std::string& table_name, const std::string& start_key, const std::string& end_key);
    ReadCursorResult doReadCursor(uint64_t cursor_id);
    CommitTxResult doCommitTx(uint64_t tx_id);
    AbortTxResult doAbortTx(uint64_t tx_id);

    // Helper methods
    bool validateTableName(const std::string& table_name, std::string& error_msg);
    bool validatePartitionMethod(const std::string& partition_method, std::string& error_msg);
    bool validateOperationForPartitionMethod(const std::string& operation, 
                                          const std::string& table_name,
                                          std::string& error_msg);
    uint64_t generateTransactionId();
    uint64_t generateCursorId();
    
    // Transaction management
    std::shared_ptr<KvtTransactionContext> getTransactionContext(uint64_t tx_id);
    std::shared_ptr<KvtTransactionContext> createTransactionContext();
    void removeTransactionContext(uint64_t tx_id);
    void cleanupExpiredTransactions();
    
    // Cursor management
    std::shared_ptr<KvtCursor> getCursor(uint64_t cursor_id);
    std::shared_ptr<KvtCursor> createCursor();
    void removeCursor(uint64_t cursor_id);
    void cleanupExpiredCursors();
    
    // Table management
    bool createTable(const std::string& table_name, const std::string& partition_method);
    bool dropTable(const std::string& table_name);
    bool tableExists(const std::string& table_name);
    bool isTableRangePartitioned(const std::string& table_name);
    std::string getTablePartitionMethod(const std::string& table_name);
    
    // Response formatting helpers
    std::string formatErrorResponse(const std::string& error_msg);
    std::string formatSuccessResponse(const std::string& message);
    std::string formatIntegerResponse(int64_t value);
    std::string formatArrayResponse(const std::vector<std::string>& items);
    std::string formatKeyValueArrayResponse(const KeyValueBatch& key_values);
    
    // Command parsing helpers
    bool parseTransactionId(const std::string_view& arg, uint64_t& tx_id, bool allow_zero = false);
    bool parseTableName(const std::string_view& arg, std::string& table_name);
    bool parseKey(const std::string_view& arg, std::string& key);
    bool parseValue(const std::string_view& arg, std::string& value);
    bool parseCursorId(const std::string_view& arg, uint64_t& cursor_id);
    bool parseRangeKeys(const std::string_view& start_key, 
                       const std::string_view& end_key,
                       std::string& start, std::string& end);

    // Member variables
    txservice::TxService* tx_service_{nullptr};
    txservice::CatalogFactory* catalog_factory_{nullptr};
    
    // Transaction management
    std::unordered_map<uint64_t, std::shared_ptr<KvtTransactionContext>> active_transactions_;
    uint64_t next_transaction_id_{1};
    
    // Cursor management
    std::unordered_map<uint64_t, std::shared_ptr<KvtCursor>> active_cursors_;
    uint64_t next_cursor_id_{1};
    
    // Table management
    std::unordered_map<std::string, std::string> table_partition_methods_; // table_name -> partition_method
    
    // Configuration
    std::chrono::seconds transaction_timeout_{300}; // 5 minutes default
    std::chrono::seconds cursor_timeout_{60};      // 1 minute default
    size_t max_active_transactions_{10000};
    size_t max_active_cursors_{1000};
    
    // Constants
    static constexpr uint64_t INVALID_TRANSACTION_ID = 0;
    static constexpr uint64_t INVALID_CURSOR_ID = 0;
    static constexpr size_t MAX_TABLE_NAME_LENGTH = 64;
    static constexpr size_t MAX_KEY_LENGTH = 1024;
    static constexpr size_t MAX_VALUE_LENGTH = 1024 * 1024; // 1MB
    static constexpr size_t SCAN_BATCH_SIZE = 100;
    
    // Supported partition methods
    static const std::vector<std::string> SUPPORTED_PARTITION_METHODS;
    
    // Partition method constants
    static constexpr const char* PARTITION_METHOD_HASH = "hash";
    static constexpr const char* PARTITION_METHOD_RANGE = "range";
    static constexpr const char* PARTITION_METHOD_LIST = "list";
};

/**
 * KVT Transaction Context - Manages the state of a single transaction
 */
class KvtTransactionContext {
public:
    explicit KvtTransactionContext(uint64_t tx_id);
    
    // Transaction state
    uint64_t getTransactionId() const { return tx_id_; }
    txservice::TxnStatus getStatus() const { return status_; }
    void setStatus(txservice::TxnStatus status) { status_ = status; }
    
    // Transaction execution
    txservice::TransactionExecution* getTransactionExecution() const { return tx_execution_.get(); }
    void setTransactionExecution(std::unique_ptr<txservice::TransactionExecution> tx_execution);
    
    // Timestamps
    std::chrono::steady_clock::time_point getStartTime() const { return start_time_; }
    std::chrono::steady_clock::time_point getLastAccessTime() const { return last_access_time_; }
    void updateLastAccessTime();
    
    // Table operations tracking
    void addTableOperation(const std::string& table_name);
    bool hasTableOperation(const std::string& table_name) const;
    
    // Cleanup
    bool isExpired(const std::chrono::seconds& timeout) const;
    void cleanup();

private:
    uint64_t tx_id_;
    txservice::TxnStatus status_;
    std::unique_ptr<txservice::TransactionExecution> tx_execution_;
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point last_access_time_;
    std::unordered_set<std::string> accessed_tables_;
};

/**
 * KVT Cursor - Manages scanning state for range operations
 */
class KvtCursor {
public:
    explicit KvtCursor(uint64_t cursor_id);
    
    // Cursor state
    uint64_t getCursorId() const { return cursor_id_; }
    bool isActive() const { return active_; }
    void setActive(bool active) { active_ = active; }
    
    // Scanning state
    void setScanState(const std::string& table_name, 
                     const std::string& start_key,
                     const std::string& end_key,
                     size_t batch_size);
    bool hasMoreResults() const { return has_more_; }
    void setHasMoreResults(bool has_more) { has_more_ = has_more; }
    bool hasMore() const { return has_more_; } // Alias for convenience
    
    // Current position tracking
    std::string getCurrentKey() const { return current_key_; }
    void setCurrentKey(const std::string& key) { current_key_ = key; }
    size_t getBatchSize() const { return batch_size_; }
    
    // Results management
    void addResults(const std::vector<std::pair<std::string, std::string>>& results);
    std::vector<std::pair<std::string, std::string>> getNextBatch(size_t batch_size);
    
    // Timestamps
    std::chrono::steady_clock::time_point getLastAccessTime() const { return last_access_time_; }
    void updateLastAccessTime();
    
    // Cleanup
    bool isExpired(const std::chrono::seconds& timeout) const;
    void cleanup();

private:
    uint64_t cursor_id_;
    bool active_;
    bool has_more_;
    
    // Scan state
    std::string table_name_;
    std::string start_key_;
    std::string end_key_;
    std::string current_key_;
    size_t batch_size_;
    
    // Timestamps
    std::chrono::steady_clock::time_point last_access_time_;
};

} // namespace EloqKV

// Static constant definitions
namespace EloqKV {
    const std::vector<std::string> KVTManager::SUPPORTED_PARTITION_METHODS = {
        "hash", "range", "list"
    };
}

#endif // KVT_MANAGER_H