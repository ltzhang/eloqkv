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
#include "tx_service/include/tx_request.h"
#include "tx_service/include/tx_key.h"
#include "tx_service/include/catalog_factory.h"
#include "tx_service/include/tx_service.h"
#include "tx_service/include/tx_command.h"
#include "kvt_inc.h"

// Forward declarations for tx_service components
namespace txservice {
    class TxService;
    class TransactionExecution;
    struct TableName;
    class TxKey;
    struct TxRecord;
    class CatalogFactory;
    struct TableSchema;
    struct TxCommand;
    struct ObjectCommandTxRequest;
}

namespace EloqKV {
// Common types for KVT operations
using KeyValuePair = std::pair<std::string, std::string>;

// NOTE: For this prototype, we simplify TxCommand implementation
// In a full implementation, these would be complete TxCommand implementations
// that integrate properly with the tx_service command processing pipeline

class KVTTable {
public:
    KVTTable(uint64_t id, const std::string& name, const std::string& partition_method)
        : id_(id), name_(name), partition_method_(partition_method) {}
    
    uint64_t id() const { return id_; }
    const std::string& name() const { return name_; }
    const std::string& partition_method() const { return partition_method_; }

private:
    uint64_t id_;
    std::string name_;
    std::string partition_method_;
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
    void initialize(txservice::TxService* tx_service = nullptr, 
                   txservice::CatalogFactory* catalog_factory = nullptr);
    
    // Cleanup and shutdown  
    void shutdown();

    KVTManager() {};

    ~KVTManager(); // Implementation in cpp to handle incomplete types

    void startTestInBackground();

    // Business logic methods (doXXXX) - exposed for external API usage
    uint64_t doCreateTable(const std::string& table_name, const std::string& partition_method, 
                          std::string& error_msg);
    bool doDropTable(uint64_t table_id, std::string& error_msg);
    bool doGetTableName(uint64_t table_id, std::string& table_name, std::string& error_msg);
    bool doGetTableId(const std::string& table_name, uint64_t& table_id, std::string& error_msg);
    bool doListTables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg);
    uint64_t doStartTx(std::string& error_msg); //return 0 if fails
    bool doGet(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                std::string& value, std::string& error_msg);
    bool doSet(uint64_t tx_id, uint64_t table_id, 
                const std::string& key, const std::string& value, std::string& error_msg);
    bool doDel(uint64_t tx_id, uint64_t table_id, 
                const std::string& key, std::string& error_msg);
    bool doScan(uint64_t tx_id, uint64_t table_id, 
                const std::string& key_start, const std::string& key_end, size_t num_item_limit, 
                std::vector<std::pair<std::string, std::string>>& results,
                std::string& error_msg);

    bool doUpdate(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                KVUpdateFunc & func, const std::string& parameter, std::string& result_value, std::string& error_msg){
                    assert(0 && "Not Implemented Yet");
                    return false;
                }

    bool doRangeUpdate(uint64_t tx_id, uint64_t table_id, const std::string& key_start, const std::string& key_end, size_t num_item_limit, 
                KVUpdateFunc & func, const std::string& parameter, std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg)
                {
                    assert(0 && "Not Implemented Yet");
                    return false;
                }

    bool doCommitTx(uint64_t tx_id, std::string& error_msg);
    bool doAbortTx(uint64_t tx_id, std::string& error_msg);

private:
    KVTManager(const KVTManager&) = delete;
    KVTManager& operator=(const KVTManager&) = delete;
    
    // Helper methods
    txservice::TransactionExecution* getTransaction(uint64_t tx_id);
    txservice::TransactionExecution* getOrCreateTransaction(uint64_t tx_id);
    KVTTable* getTable(uint64_t table_id);
    KVTTable* getTableByName(const std::string& table_name);
    
    // Transaction execution helpers
    txservice::TransactionExecution* newTxm(txservice::IsolationLevel iso_level,
                                           txservice::CcProtocol protocol);
    bool executeTxRequest(txservice::TransactionExecution* txm,
                         txservice::ObjectCommandTxRequest* tx_req,
                         std::string& error_msg);
    
    // Test methods
    void runComprehensiveTest();
    void runComprehensiveStressTest();
    
    // Stress test and performance benchmark methods
    void runStressTest();
    void runSingleThreadedStressTest();
    void runMultiThreadedStressTest();
    void runBenchmarkSuite();
    void runThroughputBenchmark();
    void runLatencyBenchmark();
    void runConcurrencyBenchmark();

private:    // Member variables
    // Using existing catalog factory - will be set during initialization
    txservice::CatalogFactory* catalog_factory_{nullptr}; // Borrowed from RedisServiceImpl
    txservice::TxService* tx_service_{nullptr}; // Borrowed from RedisServiceImpl
    
    // Transaction management
    std::unordered_map<uint64_t, txservice::TransactionExecution *> active_transactions_;
    std::mutex transactions_mutex_;

    // Table management - using table_id as primary key
    std::unordered_map<uint64_t, std::unique_ptr<KVTTable>> tables_by_id_; // table_id -> table
    std::unordered_map<std::string, uint64_t> table_name_to_id_; // table_name -> table_id
    std::mutex tables_mutex_;

    uint64_t next_transaction_id_{1};
    uint64_t next_table_id_{1};
};

} // namespace EloqKV

#endif // KVT_MANAGER_H