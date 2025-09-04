#include "eloq_kvt/kvt_inc.h"
#include "kvt_inc.h"
#include "kvt_manager.h"
#include <iostream>
#include <memory>

// Forward declaration wrapper to hide KVTManager implementation
// Global instance
std::unique_ptr<EloqKV::KVTManager> g_kvt_manager;
int g_verbosity = 0;
int g_sanity_check_level = 0;

EloqKV::KVTManager* kvt_manager() {
    if (!g_kvt_manager) {
        //kvt_initialize(); could do this also
        throw std::runtime_error("KVTManager not initialized");
    }
    return g_kvt_manager.get();
}
// Implementation of public API functions
KVTError kvt_set_sanity_check_level(int level) {
    g_sanity_check_level = level;
    return KVTError::SUCCESS;
}

KVTError kvt_set_verbosity(int verbosity) {
    g_verbosity = verbosity;
    return KVTError::SUCCESS;
}

KVTError kvt_initialize() {
    try {
        if (g_kvt_manager) {
            // Already initialized
            return KVTError::SUCCESS;
        }
        std::cout << "KVT Manager initialization with nullptr tx_service and catalog_factory" << std::endl;
        g_kvt_manager = std::make_unique<EloqKV::KVTManager>();
        g_kvt_manager->initialize();
        return KVTError::SUCCESS;
    } catch (const std::exception& e) {
        std::cerr << "KVT initialization failed: " << e.what() << std::endl;
        return KVTError::UNKNOWN_ERROR;
    } catch (...) {
        std::cerr << "KVT initialization failed: unknown error" << std::endl;
        return KVTError::UNKNOWN_ERROR;
    }
}

void kvt_shutdown() {
    if (g_kvt_manager) {
        g_kvt_manager.reset();
    }
}

KVTError kvt_create_table(const std::string& table_name, 
                          const std::string& partition_method,
                          uint64_t& table_id,
                          std::string& error_msg) {
    table_id = kvt_manager()->doCreateTable(table_name, partition_method, error_msg);
    if (table_id == 0) {
        return KVTError::UNKNOWN_ERROR;
    }
    return KVTError::SUCCESS;
}

KVTError kvt_drop_table(uint64_t table_id, 
                        std::string& error_msg) {
    if (kvt_manager()->doDropTable(table_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_get_table_name(uint64_t table_id, 
                            std::string& table_name, 
                            std::string& error_msg) {
    if (kvt_manager()->doGetTableName(table_id, table_name, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_get_table_id(const std::string& table_name, 
                          uint64_t& table_id, 
                          std::string& error_msg) {
    if (kvt_manager()->doGetTableId(table_name, table_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_list_tables(std::vector<std::pair<std::string, uint64_t>>& results, 
                         std::string& error_msg) {
    if (kvt_manager()->doListTables(results, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_start_transaction(uint64_t& tx_id, std::string& error_msg) {
    tx_id = kvt_manager()->doStartTx(error_msg);
    if (tx_id == 0) {
        return KVTError::UNKNOWN_ERROR;
    }
    return KVTError::SUCCESS;
}

KVTError kvt_get(uint64_t tx_id, 
                uint64_t table_id,
                const std::string& key,
                std::string& value,
                std::string& error_msg) {
    if (kvt_manager()->doGet(tx_id, table_id, key, value, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_set(uint64_t tx_id,
                uint64_t table_id,
                const std::string& key,
                const std::string& value,
                std::string& error_msg) {
    if (kvt_manager()->doSet(tx_id, table_id, key, value, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_del(uint64_t tx_id, 
                uint64_t table_id,
                const std::string& key,
                std::string& error_msg) {
    if (kvt_manager()->doDel(tx_id, table_id, key, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_scan(uint64_t tx_id,
                uint64_t table_id,
                const std::string& key_start,
                const std::string& key_end,
                size_t num_item_limit,
                std::vector<std::pair<std::string, std::string>>& results,
                std::string& error_msg) {
    if (kvt_manager()->doScan(tx_id, table_id, key_start, key_end, 
                           num_item_limit, results, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (kvt_manager()->doCommitTx(tx_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (kvt_manager()->doAbortTx(tx_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_batch_execute(uint64_t tx_id,
                           const KVTBatchOps& batch_ops,
                           KVTBatchResults& batch_results,
                           std::string& error_msg) {
    // Clear previous results
    batch_results.clear();
    batch_results.reserve(batch_ops.size());
    
    bool all_successful = true;
    std::string concatenated_errors;
    auto manager = kvt_manager();
    // Execute each operation individually
    for (size_t i = 0; i < batch_ops.size(); ++i) {
        const KVTOp& op = batch_ops[i];
        KVTOpResult result;
        
        std::string op_error_msg;
        
        switch (op.op) {
            case OP_GET: {
                std::string value;
                if (manager->doGet(tx_id, op.table_id, op.key, value, op_error_msg)) {
                    result.error = KVTError::SUCCESS;
                    result.value = value;
                } else {
                    result.error = KVTError::UNKNOWN_ERROR;
                    result.value = "";
                    all_successful = false;
                }
                break;
            }
            case OP_SET: {
                if (manager->doSet(tx_id, op.table_id, op.key, op.value, op_error_msg)) {
                    result.error = KVTError::SUCCESS;
                    result.value = "";
                } else {
                    result.error = KVTError::UNKNOWN_ERROR;
                    result.value = "";
                    all_successful = false;
                }
                break;
            }
            case OP_DEL: {
                if (manager->doDel(tx_id, op.table_id, op.key, op_error_msg)) {
                    result.error = KVTError::SUCCESS;
                    result.value = "";
                } else {
                    result.error = KVTError::UNKNOWN_ERROR;
                    result.value = "";
                    all_successful = false;
                }
                break;
            }
            default: {
                result.error = KVTError::UNKNOWN_ERROR;
                result.value = "";
                op_error_msg = "Unknown operation type";
                all_successful = false;
                break;
            }
        }
        
        // Add error message to concatenated errors if operation failed and error string is not empty
        if (result.error != KVTError::SUCCESS && !op_error_msg.empty()) {
            if (!concatenated_errors.empty()) {
                concatenated_errors += "; ";
            }
            concatenated_errors += "op[" + std::to_string(i) + "]: " + op_error_msg;
        }
        
        batch_results.push_back(result);
    }
    
    // Set the final error message
    if (!all_successful) {
        error_msg = concatenated_errors;
        return KVTError::BATCH_NOT_FULLY_SUCCESS;
    } else {
        error_msg = "";
        return KVTError::SUCCESS;
    }
}