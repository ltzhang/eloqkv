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
#include <mutex>
#include <thread>
#include <functional>
#include <brpc/redis_reply.h>
#include <butil/strings/string_piece.h>

// Include only essential tx_service types
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
    class TableSchema;
    struct TxCommand;
    struct ObjectCommandTxRequest;
}

// Hash function for pair<uint64_t, string> used in transaction_data_
namespace std {
    template<>
    struct hash<std::pair<uint64_t, std::string>> {
        size_t operator()(const std::pair<uint64_t, std::string>& p) const {
            return std::hash<uint64_t>()(p.first) ^ (std::hash<std::string>()(p.second) << 1);
        }
    };
}

namespace EloqKV {
// Common types for KVT operations
using KeyValuePair = std::pair<std::string, std::string>;

// Note: For simplification, we'll not use a catalog factory initially
// and keep the TxService creation simpler

class KVTTable {
public:
    KVTTable(const std::string& name, const std::string& partition_method, 
             std::unique_ptr<txservice::TableName> table_name)
        : name_(name), partition_method_(partition_method), 
          table_name_(std::move(table_name)) {}
    
    const std::string& name() const { return name_; }
    const std::string& partition_method() const { return partition_method_; }
    const txservice::TableName* table_name() const { return table_name_.get(); }

private:
    std::string name_;
    std::string partition_method_;
    std::unique_ptr<txservice::TableName> table_name_;
};

/**
 * KVT Manager - Main class that handles KVT (Key-Value Transaction) requests
 * 
 * This class manages:
 * - Table creation and management
 * - Transaction lifecycle (start, commit, abort)
 * - Key-value operations within transactions
 * - Integration with the underlying transaction service
 */
class KVTManager {
public:
    // Main command handler entry point - parses args and formats responses
    void handleCommand(const std::vector<butil::StringPiece> &args,
                        brpc::RedisReply *output);
    
    // Initialize the manager with transaction service dependencies
    void initialize();
    
    // Cleanup and shutdown
    void shutdown();

    KVTManager() {};

    ~KVTManager(); // Implementation in cpp to handle incomplete types

    void startTestInBackground();

private:
    KVTManager(const KVTManager&) = delete;
    KVTManager& operator=(const KVTManager&) = delete;

    // Business logic methods (doXXXX) - called by handleCommand
    uint64_t doCreateTable(const std::string& table_name, const std::string& partition_method, 
                          std::string& error_msg);
    uint64_t doStartTx(std::string& error_msg); //return 0 if fails
    bool doGet(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                std::string& value, std::string& error_msg);
    bool doSet(uint64_t tx_id, const std::string& table_name, 
                const std::string& key, const std::string& value, std::string& error_msg);
    bool doScan(uint64_t tx_id, const std::string& table_name, 
                const std::string& key_start, const std::string& key_end, size_t num_item_limit, 
                std::vector<std::pair<std::string, std::string>>& results,
                std::string& error_msg);
    bool doCommitTx(uint64_t tx_id, std::string& error_msg);
    bool doAbortTx(uint64_t tx_id, std::string& error_msg);
    
    // Helper methods
    txservice::TransactionExecution* getTransaction(uint64_t tx_id);
    txservice::TransactionExecution* getOrCreateTransaction(uint64_t tx_id);
    KVTTable* getTable(const std::string& table_name);
    
    // Test methods
    void runComprehensiveTest();

private:    // Member variables
    txservice::TxService* tx_service_{nullptr}; // Raw pointer to avoid incomplete type issues
    
    // Transaction management
    std::unordered_map<uint64_t, txservice::TransactionExecution *> active_transactions_;
    std::mutex transactions_mutex_;

    // Table management
    std::unordered_map<std::string, std::unique_ptr<KVTTable>> tables_; // table_name -> table
    std::mutex tables_mutex_;

    uint64_t next_transaction_id_{1};
    uint64_t next_table_id_{1};

    // Storage for actual key-value data
    // For committed data: full_key (table_name:key) -> value
    std::unordered_map<std::string, std::string> committed_data_;
    std::mutex committed_data_mutex_;
    
    // For transaction data: (tx_id, full_key) -> value  
    std::unordered_map<std::pair<uint64_t, std::string>, std::string> transaction_data_;
    std::mutex tx_data_mutex_;
};

} // namespace EloqKV

#endif // KVT_MANAGER_H