#include "kvt_manager.h"
#include <sstream>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <cstring>
#include <glog/logging.h>

namespace EloqKV {

// Singleton instance
KVTManager& KVTManager::getInstance() {
    static KVTManager instance;
    return instance;
}

// Constructor
KVTManager::KVTManager() 
    : tx_service_(nullptr)
    , catalog_factory_(nullptr)
    , next_transaction_id_(1)
    , next_cursor_id_(1)
    , transaction_timeout_(300)
    , cursor_timeout_(60)
    , max_active_transactions_(10000)
    , max_active_cursors_(1000) {
}

// Destructor
KVTManager::~KVTManager() {
    shutdown();
}

// Initialize the manager
void KVTManager::initialize(txservice::TxService* tx_service, 
                           txservice::CatalogFactory* catalog_factory) {
    tx_service_ = tx_service;
    catalog_factory_ = catalog_factory;
    LOG(INFO) << "KVTManager initialized with tx_service and catalog_factory";
}

// Shutdown and cleanup
void KVTManager::shutdown() {
    // Clean up all active transactions and cursors
    active_transactions_.clear();
    active_cursors_.clear();
    
    LOG(INFO) << "KVTManager shutdown completed";
}

// Main command handler - parses args and formats responses
void KVTManager::handleCommand(const std::vector<std::string_view>& args, std::string& output) {
    if (args.size() < 2) {
        output = formatErrorResponse("ERR wrong number of arguments for KVT command");
        return;
    }
    
    const std::string& subcommand = std::string(args[1]);
    
    try {
        if (subcommand == "create_table") {
            if (args.size() != 4) {
                output = formatErrorResponse("ERR wrong number of arguments for create_table");
                return;
            }
            std::string table_name, partition_method;
            if (!parseTableName(args[2], table_name)) {
                output = formatErrorResponse("ERR invalid table name");
                return;
            }
            partition_method = std::string(args[3]);
            auto result = getInstance().doCreateTable(table_name, partition_method);
            output = result.success ? formatSuccessResponse(result.message) : formatErrorResponse(result.message);
            
        } else if (subcommand == "drop_table") {
            if (args.size() != 3) {
                output = formatErrorResponse("ERR wrong number of arguments for drop_table");
                return;
            }
            std::string table_name;
            if (!parseTableName(args[2], table_name)) {
                output = formatErrorResponse("ERR invalid table name");
                return;
            }
            auto result = getInstance().doDropTable(table_name);
            output = result.success ? formatSuccessResponse(result.message) : formatErrorResponse(result.message);
            
        } else if (subcommand == "list_tables") {
            if (args.size() != 2) {
                output = formatErrorResponse("ERR wrong number of arguments for list_tables");
                return;
            }
            auto result = getInstance().doListTables();
            if (result.success) {
                output = formatArrayResponse(result.tables);
            } else {
                output = formatErrorResponse(result.message);
            }
            
        } else if (subcommand == "start_tx") {
            if (args.size() != 2) {
                output = formatErrorResponse("ERR wrong number of arguments for start_tx");
                return;
            }
            auto result = getInstance().doStartTx();
            if (result.success) {
                output = formatIntegerResponse(static_cast<int64_t>(result.transaction_id));
            } else {
                output = formatErrorResponse(result.message);
            }
            
        } else if (subcommand == "get") {
            if (args.size() != 4) {
                output = formatErrorResponse("ERR wrong number of arguments for get");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, true)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name, key;
            if (!parseTableName(args[3], table_name) || !parseKey(args[4], key)) {
                output = formatErrorResponse("ERR invalid table name or key");
                return;
            }
            auto result = getInstance().doGet(tx_id, table_name, key);
            if (result.success) {
                if (result.found) {
                    output = result.value;
                } else {
                    output = "(nil)";
                }
            } else {
                output = formatErrorResponse(result.message);
            }
            
        } else if (subcommand == "batch_get") {
            if (args.size() < 4) {
                output = formatErrorResponse("ERR wrong number of arguments for batch_get");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, true)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name;
            if (!parseTableName(args[3], table_name)) {
                output = formatErrorResponse("ERR invalid table name");
                return;
            }
            KeyBatch keys;
            for (size_t i = 4; i < args.size(); ++i) {
                std::string key;
                if (parseKey(args[i], key)) {
                    keys.push_back(key);
                }
            }
            auto result = getInstance().doBatchGet(tx_id, table_name, keys);
            if (result.success) {
                output = formatKeyValueArrayResponse(result.results);
            } else {
                output = formatErrorResponse(result.message);
            }
            
        } else if (subcommand == "set") {
            if (args.size() != 5) {
                output = formatErrorResponse("ERR wrong number of arguments for set");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, true)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name, key, value;
            if (!parseTableName(args[3], table_name) || !parseKey(args[4], key) || !parseValue(args[5], value)) {
                output = formatErrorResponse("ERR invalid table name, key, or value");
                return;
            }
            auto result = getInstance().doSet(tx_id, table_name, key, value);
            output = result.success ? formatSuccessResponse(result.message) : formatErrorResponse(result.message);
            
        } else if (subcommand == "batch_set") {
            if (args.size() < 4) {
                output = getInstance().formatErrorResponse("ERR wrong number of arguments for batch_set");
                return;
            }
            uint64_t tx_id;
            if (!getInstance().parseTransactionId(args[2], tx_id, true)) {
                output = getInstance().formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name;
            if (!getInstance().parseTableName(args[3], table_name)) {
                output = getInstance().formatErrorResponse("ERR invalid table name");
                return;
            }
            KeyValueBatch key_values;
            for (size_t i = 4; i < args.size(); i += 2) {
                if (i + 1 < args.size()) {
                    std::string key, value;
                    if (getInstance().parseKey(args[i], key) && getInstance().parseValue(args[i + 1], value)) {
                        key_values.emplace_back(key, value);
                    }
                }
            }
            auto result = getInstance().doBatchSet(tx_id, table_name, key_values);
            output = result.success ? getInstance().formatSuccessResponse(result.message) : getInstance().formatErrorResponse(result.message);
            
        } else if (subcommand == "delete") {
            if (args.size() != 4) {
                output = formatErrorResponse("ERR wrong number of arguments for delete");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, true)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name, key;
            if (!parseTableName(args[3], table_name) || !parseKey(args[4], key)) {
                output = formatErrorResponse("ERR invalid table name or key");
                return;
            }
            auto result = getInstance().doDelete(tx_id, table_name, key);
            output = result.success ? formatIntegerResponse(result.deleted ? 1 : 0) : formatErrorResponse(result.message);
            
        } else if (subcommand == "batch_delete") {
            if (args.size() < 4) {
                output = formatErrorResponse("ERR wrong number of arguments for batch_delete");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, true)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name;
            if (!parseTableName(args[3], table_name)) {
                output = formatErrorResponse("ERR invalid table name");
                return;
            }
            KeyBatch keys;
            for (size_t i = 4; i < args.size(); ++i) {
                std::string key;
                if (parseKey(args[i], key)) {
                    keys.push_back(key);
                }
            }
            auto result = getInstance().doBatchDelete(tx_id, table_name, keys);
            output = result.success ? formatSuccessResponse(result.message) : formatErrorResponse(result.message);
            
        } else if (subcommand == "scan") {
            if (args.size() != 6) {
                output = formatErrorResponse("ERR wrong number of arguments for scan");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, true)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            std::string table_name, start_key, end_key;
            if (!parseTableName(args[3], table_name) || !parseRangeKeys(args[4], args[5], start_key, end_key)) {
                output = formatErrorResponse("ERR invalid table name or range keys");
                return;
            }
            auto result = getInstance().doScan(tx_id, table_name, start_key, end_key);
            if (result.success) {
                std::vector<std::string> response = {
                    std::to_string(result.cursor_id),
                    std::to_string(result.results.size())
                };
                output = formatArrayResponse(response);
            } else {
                output = formatErrorResponse(result.message);
            }
            
        } else if (subcommand == "read_cursor") {
            if (args.size() != 3) {
                output = formatErrorResponse("ERR wrong number of arguments for read_cursor");
                return;
            }
            uint64_t cursor_id;
            if (!parseCursorId(args[2], cursor_id)) {
                output = formatErrorResponse("ERR invalid cursor ID");
                return;
            }
            auto result = getInstance().doReadCursor(cursor_id);
            if (result.success) {
                if (result.has_more) {
                    output = formatKeyValueArrayResponse(result.results);
                } else {
                    output = "0"; // No more results
                }
            } else {
                output = formatErrorResponse(result.message);
            }
            
        } else if (subcommand == "commit_tx") {
            if (args.size() != 3) {
                output = formatErrorResponse("ERR wrong number of arguments for commit_tx");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, false)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            auto result = getInstance().doCommitTx(tx_id);
            output = result.success ? formatSuccessResponse(result.message) : formatErrorResponse(result.message);
            
        } else if (subcommand == "abort_tx") {
            if (args.size() != 3) {
                output = formatErrorResponse("ERR wrong number of arguments for abort_tx");
                return;
            }
            uint64_t tx_id;
            if (!parseTransactionId(args[2], tx_id, false)) {
                output = formatErrorResponse("ERR invalid transaction ID");
                return;
            }
            auto result = getInstance().doAbortTx(tx_id);
            output = result.success ? formatSuccessResponse(result.message) : formatErrorResponse(result.message);
            
        } else {
            output = formatErrorResponse("ERR unknown subcommand '" + subcommand + "'");
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in KVT command handler: " << e.what();
        output = formatErrorResponse("ERR internal error: " + std::string(e.what()));
    }
}

// Business logic methods (doXXXX)
CreateTableResult KVTManager::doCreateTable(const std::string& table_name, const std::string& partition_method) {
    CreateTableResult result;
    
    std::string error_msg;
    if (!validateTableName(table_name, error_msg)) {
        result.success = false;
        result.message = error_msg;
        return result;
    }
    
    if (!validatePartitionMethod(partition_method, error_msg)) {
        result.success = false;
        result.message = error_msg;
        return result;
    }
    
    if (tableExists(table_name)) {
        result.success = false;
        result.message = "ERR table '" + table_name + "' already exists";
        return result;
    }
    
    if (createTable(table_name, partition_method)) {
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR failed to create table '" + table_name + "'";
    }
    
    return result;
}

DropTableResult KVTManager::doDropTable(const std::string& table_name) {
    DropTableResult result;
    
    if (!tableExists(table_name)) {
        result.success = false;
        result.message = "ERR table '" + table_name + "' does not exist";
        return result;
    }
    
    if (dropTable(table_name)) {
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR failed to drop table '" + table_name + "'";
    }
    
    return result;
}

ListTablesResult KVTManager::doListTables() {
    ListTablesResult result;
    
    for (const auto& [table_name, partition_method] : table_partition_methods_) {
        result.tables.push_back(table_name + " (" + partition_method + ")");
    }
    
    result.success = true;
    result.message = "OK";
    return result;
}

StartTxResult KVTManager::doStartTx() {
    StartTxResult result;
    
    auto tx_context = createTransactionContext();
    if (!tx_context) {
        result.success = false;
        result.message = "ERR failed to create transaction";
        return result;
    }
    
    result.success = true;
    result.message = "OK";
    result.transaction_id = tx_context->getTransactionId();
    return result;
}

GetResult KVTManager::doGet(uint64_t tx_id, const std::string& table_name, const std::string& key) {
    GetResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation get
        // For now, simulate a successful single-operation get
        if (tx_service_ && catalog_factory_) {
            // Simulate getting a value without transaction context
            result.success = true;
            result.message = "OK";
            result.value = "simulated_value_for_" + key; // Placeholder
            result.found = true;
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual get operation through tx_service
    // For now, simulate a successful get operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate getting a value (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
        result.value = "simulated_value_for_" + key; // Placeholder
        result.found = true;
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

BatchGetResult KVTManager::doBatchGet(uint64_t tx_id, const std::string& table_name, const KeyBatch& keys) {
    BatchGetResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation batch get
        // For now, simulate a successful single-operation batch get
        if (tx_service_ && catalog_factory_) {
            // Simulate getting values for all keys without transaction context
            for (const auto& key : keys) {
                result.results.emplace_back(key, "simulated_value_for_" + key);
            }
            
            result.success = true;
            result.message = "OK";
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual batch get operation through tx_service
    // For now, simulate a successful batch get operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate getting values for all keys (in real implementation, this would call tx_service)
        for (const auto& key : keys) {
            result.results.emplace_back(key, "simulated_value_for_" + key);
        }
        
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

SetResult KVTManager::doSet(uint64_t tx_id, const std::string& table_name, const std::string& key, const std::string& value) {
    SetResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation set
        // For now, simulate a successful single-operation set
        if (tx_service_ && catalog_factory_) {
            // Simulate setting a value without transaction context
            result.success = true;
            result.message = "OK";
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual set operation through tx_service
    // For now, simulate a successful set operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate setting a value (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

BatchSetResult KVTManager::doBatchSet(uint64_t tx_id, const std::string& table_name, const KeyValueBatch& key_values) {
    BatchSetResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation batch set
        // For now, simulate a successful single-operation batch set
        if (tx_service_ && catalog_factory_) {
            // Simulate setting values for all key-value pairs without transaction context
            result.success = true;
            result.message = "OK";
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual batch set operation through tx_service
    // For now, simulate a successful batch set operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate setting values for all key-value pairs (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

DeleteResult KVTManager::doDelete(uint64_t tx_id, const std::string& table_name, const std::string& key) {
    DeleteResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation delete
        // For now, simulate a successful single-operation delete
        if (tx_service_ && catalog_factory_) {
            // Simulate deleting a key without transaction context
            result.success = true;
            result.message = "OK";
            result.deleted = true;
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual delete operation through tx_service
    // For now, simulate a successful delete operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate deleting a key (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
        result.deleted = true;
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

BatchDeleteResult KVTManager::doBatchDelete(uint64_t tx_id, const std::string& table_name, const KeyBatch& keys) {
    BatchDeleteResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation batch delete
        // For now, simulate a successful single-operation batch delete
        if (tx_service_ && catalog_factory_) {
            // Simulate deleting all keys without transaction context
            result.success = true;
            result.message = "OK";
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual batch delete operation through tx_service
    // For now, simulate a successful batch delete operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate deleting all keys (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

ScanResult KVTManager::doScan(uint64_t tx_id, const std::string& table_name, const std::string& start_key, const std::string& end_key) {
    ScanResult result;
    
    // Handle single-operation mode (tx_id == 0)
    if (tx_id == INVALID_TRANSACTION_ID) {
        // TODO: Implement single-operation scan
        // For now, simulate a successful single-operation scan
        if (tx_service_ && catalog_factory_) {
            // Validate operation for table's partition method
            std::string error_msg;
            if (!validateOperationForPartitionMethod("scan", table_name, error_msg)) {
                result.success = false;
                result.message = error_msg;
                return result;
            }
            
            // Create cursor for scanning
            auto cursor = createCursor();
            if (!cursor) {
                result.success = false;
                result.message = "ERR failed to create scan cursor";
                return result;
            }
            
            cursor->setScanState(table_name, start_key, end_key, SCAN_BATCH_SIZE);
            
            // Simulate scan results (in real implementation, this would call tx_service)
            result.success = true;
            result.message = "OK";
            result.cursor_id = cursor->getCursorId();
            result.has_more = true;
            
            // Add some simulated scan results
            for (size_t i = 0; i < std::min(SCAN_BATCH_SIZE, size_t(10)); ++i) {
                std::string simulated_key = start_key + "_" + std::to_string(i);
                if (simulated_key <= end_key) {
                    result.results.emplace_back(simulated_key, "simulated_value_for_" + simulated_key);
                }
            }
        } else {
            result.success = false;
            result.message = "ERR tx_service not initialized";
        }
        return result;
    }
    
    // Handle transaction mode
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // Validate operation for table's partition method
    std::string error_msg;
    if (!validateOperationForPartitionMethod("scan", table_name, error_msg)) {
        result.success = false;
        result.message = error_msg;
        return result;
    }
    
    // Create cursor for scanning
    auto cursor = createCursor();
    if (!cursor) {
        result.success = false;
        result.message = "ERR failed to create scan cursor";
        return result;
    }
    
    cursor->setScanState(table_name, start_key, end_key, SCAN_BATCH_SIZE);
    
    // TODO: Implement actual scan operation through tx_service
    // For now, simulate a successful scan operation
    if (tx_service_ && catalog_factory_) {
        // Add table operation tracking
        tx_context->addTableOperation(table_name);
        
        // Simulate scan results (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
        result.cursor_id = cursor->getCursorId();
        result.has_more = true;
        
        // Add some simulated scan results
        for (size_t i = 0; i < std::min(SCAN_BATCH_SIZE, size_t(10)); ++i) {
            std::string simulated_key = start_key + "_" + std::to_string(i);
            if (simulated_key <= end_key) {
                result.results.emplace_back(simulated_key, "simulated_value_for_" + simulated_key);
            }
        }
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

ReadCursorResult KVTManager::doReadCursor(uint64_t cursor_id) {
    ReadCursorResult result;
    
    auto cursor = getCursor(cursor_id);
    if (!cursor) {
        result.success = false;
        result.message = "ERR cursor not found";
        return result;
    }
    
    if (!cursor->isActive()) {
        result.success = false;
        result.message = "ERR cursor is not active";
        return result;
    }
    
    // TODO: Implement actual cursor reading through tx_service
    // For now, simulate a successful cursor reading operation
    if (tx_service_ && catalog_factory_) {
        // Simulate reading from cursor (in real implementation, this would call tx_service)
        result.success = true;
        result.message = "OK";
        result.has_more = cursor->hasMore();
        
        // Add some simulated results based on cursor state
        if (cursor->hasMore()) {
            std::string current_key = cursor->getCurrentKey();
            for (size_t i = 0; i < cursor->getBatchSize() && cursor->hasMore(); ++i) {
                std::string simulated_key = current_key + "_" + std::to_string(i);
                result.results.emplace_back(simulated_key, "simulated_value_for_" + simulated_key);
            }
            
            // Update cursor position
            cursor->setCurrentKey("simulated_key_next");
        }
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

CommitTxResult KVTManager::doCommitTx(uint64_t tx_id) {
    CommitTxResult result;
    
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual commit through tx_service
    // For now, simulate a successful commit operation
    if (tx_service_ && catalog_factory_) {
        // Simulate committing the transaction (in real implementation, this would call tx_service)
        tx_context->setStatus(txservice::TxnStatus::Committed);
        
        // Clean up transaction context
        removeTransactionContext(tx_id);
        
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

AbortTxResult KVTManager::doAbortTx(uint64_t tx_id) {
    AbortTxResult result;
    
    auto tx_context = getTransactionContext(tx_id);
    if (!tx_context) {
        result.success = false;
        result.message = "ERR transaction not found";
        return result;
    }
    
    if (tx_context->getStatus() != txservice::TxnStatus::Ongoing) {
        result.success = false;
        result.message = "ERR transaction is not active";
        return result;
    }
    
    // TODO: Implement actual abort through tx_service
    // For now, simulate a successful abort operation
    if (tx_service_ && catalog_factory_) {
        // Simulate aborting the transaction (in real implementation, this would call tx_service)
        tx_context->setStatus(txservice::TxnStatus::Aborted);
        
        // Clean up transaction context
        removeTransactionContext(tx_id);
        
        result.success = true;
        result.message = "OK";
    } else {
        result.success = false;
        result.message = "ERR tx_service not initialized";
    }
    return result;
}

// Helper methods
bool KVTManager::validateTableName(const std::string& table_name, std::string& error_msg) {
    if (table_name.empty()) {
        error_msg = "ERR table name cannot be empty";
        return false;
    }
    
    if (table_name.length() > MAX_TABLE_NAME_LENGTH) {
        error_msg = "ERR table name too long (max " + std::to_string(MAX_TABLE_NAME_LENGTH) + " characters)";
        return false;
    }
    
    // Check for invalid characters
    if (table_name.find_first_of(" \t\n\r\f\v") != std::string::npos) {
        error_msg = "ERR table name contains invalid characters";
        return false;
    }
    
    return true;
}

bool KVTManager::validatePartitionMethod(const std::string& partition_method, std::string& error_msg) {
    auto it = std::find(SUPPORTED_PARTITION_METHODS.begin(), 
                        SUPPORTED_PARTITION_METHODS.end(), 
                        partition_method);
    
    if (it == SUPPORTED_PARTITION_METHODS.end()) {
        std::string supported_methods;
        for (size_t i = 0; i < SUPPORTED_PARTITION_METHODS.size(); ++i) {
            if (i > 0) supported_methods += ", ";
            supported_methods += SUPPORTED_PARTITION_METHODS[i];
        }
        error_msg = "ERR unsupported partition method '" + partition_method + 
                   "'. Supported methods: " + supported_methods;
        return false;
    }
    
    return true;
}

bool KVTManager::validateOperationForPartitionMethod(const std::string& operation, 
                                                   const std::string& table_name,
                                                   std::string& error_msg) {
    if (!tableExists(table_name)) {
        error_msg = "ERR table '" + table_name + "' does not exist";
        return false;
    }
    
    std::string partition_method = getTablePartitionMethod(table_name);
    
    if (operation == "scan" && partition_method != PARTITION_METHOD_RANGE) {
        error_msg = "ERR operation '" + operation + "' requires a range-partitioned table. " +
                   "Table '" + table_name + "' is " + partition_method + "-partitioned.";
        return false;
    }
    
    if (operation == "get" || operation == "set" || operation == "delete") {
        // Get, set, and delete operations work with any partition method
        return true;
    }
    
    // For other operations, add specific validation as needed
    return true;
}

uint64_t KVTManager::generateTransactionId() {
    return next_transaction_id_++;
}

uint64_t KVTManager::generateCursorId() {
    return next_cursor_id_++;
}

// Transaction management
std::shared_ptr<KvtTransactionContext> KVTManager::getTransactionContext(uint64_t tx_id) {
    auto it = active_transactions_.find(tx_id);
    if (it != active_transactions_.end()) {
        it->second->updateLastAccessTime();
        return it->second;
    }
    return nullptr;
}

std::shared_ptr<KvtTransactionContext> KVTManager::createTransactionContext() {
    if (active_transactions_.size() >= max_active_transactions_) {
        LOG(WARNING) << "Maximum active transactions reached: " << max_active_transactions_;
        return nullptr;
    }
    
    uint64_t tx_id = generateTransactionId();
    auto tx_context = std::make_shared<KvtTransactionContext>(tx_id);
    
    active_transactions_[tx_id] = tx_context;
    
    return tx_context;
}

void KVTManager::removeTransactionContext(uint64_t tx_id) {
    active_transactions_.erase(tx_id);
}

void KVTManager::cleanupExpiredTransactions() {
    auto it = active_transactions_.begin();
    while (it != active_transactions_.end()) {
        if (it->second->isExpired(transaction_timeout_)) {
            LOG(INFO) << "Cleaning up expired transaction: " << it->first;
            it = active_transactions_.erase(it);
        } else {
            ++it;
        }
    }
}

// Cursor management
std::shared_ptr<KvtCursor> KVTManager::getCursor(uint64_t cursor_id) {
    auto it = active_cursors_.find(cursor_id);
    if (it != active_cursors_.end()) {
        it->second->updateLastAccessTime();
        return it->second;
    }
    return nullptr;
}

std::shared_ptr<KvtCursor> KVTManager::createCursor() {
    if (active_cursors_.size() >= max_active_cursors_) {
        LOG(WARNING) << "Maximum active cursors reached: " << max_active_cursors_;
        return nullptr;
    }
    
    uint64_t cursor_id = generateCursorId();
    auto cursor = std::make_shared<KvtCursor>(cursor_id);
    
    active_cursors_[cursor_id] = cursor;
    
    return cursor;
}

void KVTManager::removeCursor(uint64_t cursor_id) {
    active_cursors_.erase(cursor_id);
}

void KVTManager::cleanupExpiredCursors() {
    auto it = active_cursors_.begin();
    while (it != active_cursors_.end()) {
        if (it->second->isExpired(cursor_timeout_)) {
            LOG(INFO) << "Cleaning up expired cursor: " << it->first;
            it = active_cursors_.erase(it);
        } else {
            ++it;
        }
    }
}

// Table management
bool KVTManager::createTable(const std::string& table_name, const std::string& partition_method) {
    // TODO: Implement actual table creation through catalog_factory
    // For now, simulate table creation and store the partition method
    if (catalog_factory_) {
        // In real implementation, this would call catalog_factory->createTable()
        // For now, just store the partition method locally
        table_partition_methods_[table_name] = partition_method;
        LOG(INFO) << "Created table '" << table_name << "' with partition method '" << partition_method << "'";
        return true;
    } else {
        LOG(ERROR) << "Catalog factory not initialized";
        return false;
    }
}

bool KVTManager::dropTable(const std::string& table_name) {
    // TODO: Implement actual table dropping through catalog_factory
    // For now, simulate table dropping
    if (catalog_factory_) {
        auto it = table_partition_methods_.find(table_name);
        if (it != table_partition_methods_.end()) {
            // In real implementation, this would call catalog_factory->dropTable()
            // For now, just remove from local map
            table_partition_methods_.erase(it);
            LOG(INFO) << "Dropped table '" << table_name << "'";
            return true;
        }
        return false;
    } else {
        LOG(ERROR) << "Catalog factory not initialized";
        return false;
    }
}

bool KVTManager::tableExists(const std::string& table_name) {
    return table_partition_methods_.find(table_name) != table_partition_methods_.end();
}

bool KVTManager::isTableRangePartitioned(const std::string& table_name) {
    auto it = table_partition_methods_.find(table_name);
    if (it != table_partition_methods_.end()) {
        return it->second == PARTITION_METHOD_RANGE;
    }
    return false;
}

std::string KVTManager::getTablePartitionMethod(const std::string& table_name) {
    auto it = table_partition_methods_.find(table_name);
    if (it != table_partition_methods_.end()) {
        return it->second;
    }
    return "";
}

// Response formatting helpers
std::string KVTManager::formatErrorResponse(const std::string& error_msg) {
    return "-" + error_msg + "\r\n";
}

std::string KVTManager::formatSuccessResponse(const std::string& message) {
    return "+" + message + "\r\n";
}

std::string KVTManager::formatIntegerResponse(int64_t value) {
    return ":" + std::to_string(value) + "\r\n";
}

std::string KVTManager::formatArrayResponse(const std::vector<std::string>& items) {
    std::string result = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        result += "$" + std::to_string(item.length()) + "\r\n";
        result += item + "\r\n";
    }
    return result;
}

std::string KVTManager::formatKeyValueArrayResponse(const KeyValueBatch& key_values) {
    std::string result = "*" + std::to_string(key_values.size() * 2) + "\r\n";
    for (const auto& [key, value] : key_values) {
        result += "$" + std::to_string(key.length()) + "\r\n";
        result += key + "\r\n";
        result += "$" + std::to_string(value.length()) + "\r\n";
        result += value + "\r\n";
    }
    return result;
}

// Command parsing helpers
bool KVTManager::parseTransactionId(const std::string_view& arg, uint64_t& tx_id, bool allow_zero) {
    try {
        tx_id = std::stoull(std::string(arg));
        if (!allow_zero && tx_id == INVALID_TRANSACTION_ID) {
            return false;
        }
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool KVTManager::parseTableName(const std::string_view& arg, std::string& table_name) {
    table_name = std::string(arg);
    std::string error_msg;
    return validateTableName(table_name, error_msg);
}

bool KVTManager::parseKey(const std::string_view& arg, std::string& key) {
    key = std::string(arg);
    if (key.empty() || key.length() > MAX_KEY_LENGTH) {
        return false;
    }
    return true;
}

bool KVTManager::parseValue(const std::string_view& arg, std::string& value) {
    value = std::string(arg);
    if (value.length() > MAX_VALUE_LENGTH) {
        return false;
    }
    return true;
}

bool KVTManager::parseCursorId(const std::string_view& arg, uint64_t& cursor_id) {
    try {
        cursor_id = std::stoull(std::string(arg));
        return cursor_id != INVALID_CURSOR_ID;
    } catch (const std::exception&) {
        return false;
    }
}

bool KVTManager::parseRangeKeys(const std::string_view& start_key, 
                               const std::string_view& end_key,
                               std::string& start, std::string& end) {
    start = std::string(start_key);
    end = std::string(end_key);
    
    // Validate keys
    if (start.length() > MAX_KEY_LENGTH || end.length() > MAX_KEY_LENGTH) {
        return false;
    }
    
    return true;
}

// KvtTransactionContext implementation
KvtTransactionContext::KvtTransactionContext(uint64_t tx_id)
    : tx_id_(tx_id)
    , status_(txservice::TxnStatus::Ongoing)
    , tx_execution_(nullptr)
    , start_time_(std::chrono::steady_clock::now())
    , last_access_time_(start_time_) {
}

void KvtTransactionContext::setTransactionExecution(std::unique_ptr<txservice::TransactionExecution> tx_execution) {
    tx_execution_ = std::move(tx_execution);
}

void KvtTransactionContext::updateLastAccessTime() {
    last_access_time_ = std::chrono::steady_clock::now();
}

void KvtTransactionContext::addTableOperation(const std::string& table_name) {
    accessed_tables_.insert(table_name);
}

bool KvtTransactionContext::hasTableOperation(const std::string& table_name) const {
    return accessed_tables_.find(table_name) != accessed_tables_.end();
}

bool KvtTransactionContext::isExpired(const std::chrono::seconds& timeout) const {
    auto now = std::chrono::steady_clock::now();
    return (now - last_access_time_) > timeout;
}

void KvtTransactionContext::cleanup() {
    if (tx_execution_) {
        // TODO: Implement proper cleanup of transaction execution
        // In real implementation, this would call tx_execution_->cleanup() or similar
        // For now, just reset the pointer
        tx_execution_.reset();
    }
    accessed_tables_.clear();
}

// KvtCursor implementation
KvtCursor::KvtCursor(uint64_t cursor_id)
    : cursor_id_(cursor_id)
    , active_(true)
    , has_more_(true)
    , batch_size_(100)
    , current_key_("")
    , last_access_time_(std::chrono::steady_clock::now()) {
}

void KvtCursor::setScanState(const std::string& table_name, 
                            const std::string& start_key,
                            const std::string& end_key,
                            size_t batch_size) {
    table_name_ = table_name;
    start_key_ = start_key;
    end_key_ = end_key;
    batch_size_ = batch_size;
    current_key_ = start_key;
    has_more_ = true;
}

void KvtCursor::addResults(const std::vector<std::pair<std::string, std::string>>& results) {
    // This method is not used in the current cursor design
    // The cursor now tracks position with current_key_ for continuation
}

std::vector<std::pair<std::string, std::string>> KvtCursor::getNextBatch(size_t batch_size) {
    // This method is not used in the current cursor design
    // The cursor now tracks position with current_key_ for continuation
    return {};
}

void KvtCursor::updateLastAccessTime() {
    last_access_time_ = std::chrono::steady_clock::now();
}

bool KvtCursor::isExpired(const std::chrono::seconds& timeout) const {
    auto now = std::chrono::steady_clock::now();
    return (now - last_access_time_) > timeout;
}

void KvtCursor::cleanup() {
    active_ = false;
    has_more_ = false;
    table_name_.clear();
    start_key_.clear();
    end_key_.clear();
    current_key_.clear();
}

} // namespace EloqKV