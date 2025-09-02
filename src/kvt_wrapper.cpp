#include "kvt_inc.h"
#include "kvt_manager.h"
#include <iostream>
#include <memory>

// Forward declaration wrapper to hide KVTManager implementation
class KVTManagerWrapper {
public:
    KVTManagerWrapper() : manager_(std::make_unique<EloqKV::KVTManager>()) {}
    
    ~KVTManagerWrapper() {
        if (manager_) {
            manager_->shutdown();
        }
    }
    
    // Initialize with null pointers since we're running standalone
    bool initialize() {
        if (!manager_) return false;
        
        // Initialize with nullptr for tx_service and catalog_factory
        // This means the KVTManager will work in a limited standalone mode
        manager_->initialize(nullptr, nullptr);
        return true;
    }
    
    uint64_t createTable(const std::string& table_name, 
                        const std::string& partition_method,
                        std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return 0;
        }
        return manager_->doCreateTable(table_name, partition_method, error_msg);
    }
    
    bool dropTable(uint64_t table_id, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doDropTable(table_id, error_msg);
    }
    
    bool getTableName(uint64_t table_id, std::string& table_name, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doGetTableName(table_id, table_name, error_msg);
    }
    
    bool getTableId(const std::string& table_name, uint64_t& table_id, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doGetTableId(table_name, table_id, error_msg);
    }
    
    bool listTables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doListTables(results, error_msg);
    }
    
    uint64_t startTransaction(std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return 0;
        }
        return manager_->doStartTx(error_msg);
    }
    
    bool get(uint64_t tx_id, uint64_t table_id, 
            const std::string& key, std::string& value, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doGet(tx_id, table_id, key, value, error_msg);
    }
    
    bool set(uint64_t tx_id, uint64_t table_id,
            const std::string& key, const std::string& value, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doSet(tx_id, table_id, key, value, error_msg);
    }

    bool del(uint64_t tx_id, uint64_t table_id, 
        const std::string& key, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doDel(tx_id, table_id, key, error_msg);
    }

    bool scan(uint64_t tx_id, uint64_t table_id,
             const std::string& key_start, const std::string& key_end,
             size_t num_item_limit, std::vector<std::pair<std::string, std::string>>& results,
             std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doScan(tx_id, table_id, key_start, key_end, 
                               num_item_limit, results, error_msg);
    }
    
    bool commitTransaction(uint64_t tx_id, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doCommitTx(tx_id, error_msg);
    }
    
    bool rollbackTransaction(uint64_t tx_id, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doAbortTx(tx_id, error_msg);
    }
    
private:
    std::unique_ptr<EloqKV::KVTManager> manager_;
};

// Global instance
std::unique_ptr<KVTManagerWrapper> g_kvt_manager;

// Implementation of public API functions

KVTError kvt_initialize() {
    try {
        if (g_kvt_manager) {
            // Already initialized
            return KVTError::SUCCESS;
        }
        
        g_kvt_manager = std::make_unique<KVTManagerWrapper>();
        if (g_kvt_manager->initialize()) {
            return KVTError::SUCCESS;
        } else {
            return KVTError::UNKNOWN_ERROR;
        }
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
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    table_id = g_kvt_manager->createTable(table_name, partition_method, error_msg);
    if (table_id == 0) {
        return KVTError::UNKNOWN_ERROR;
    }
    return KVTError::SUCCESS;
}

KVTError kvt_drop_table(uint64_t table_id, 
                        std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->dropTable(table_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_get_table_name(uint64_t table_id, 
                            std::string& table_name, 
                            std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->getTableName(table_id, table_name, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_get_table_id(const std::string& table_name, 
                          uint64_t& table_id, 
                          std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->getTableId(table_name, table_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_list_tables(std::vector<std::pair<std::string, uint64_t>>& results, 
                         std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->listTables(results, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_start_transaction(uint64_t& tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    tx_id = g_kvt_manager->startTransaction(error_msg);
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
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->get(tx_id, table_id, key, value, error_msg)) {
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
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->set(tx_id, table_id, key, value, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_del(uint64_t tx_id, 
                uint64_t table_id,
                const std::string& key,
                std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->del(tx_id, table_id, key, error_msg)) {
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
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->scan(tx_id, table_id, key_start, key_end, 
                           num_item_limit, results, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->commitTransaction(tx_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    if (g_kvt_manager->rollbackTransaction(tx_id, error_msg)) {
        return KVTError::SUCCESS;
    } else {
        return KVTError::UNKNOWN_ERROR;
    }
}

KVTError kvt_batch_execute(uint64_t tx_id,
                           const KVTBatchOps& batch_ops,
                           KVTBatchResults& batch_results,
                           std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    // Clear previous results
    batch_results.clear();
    batch_results.reserve(batch_ops.size());
    
    bool all_successful = true;
    std::string concatenated_errors;
    
    // Execute each operation individually
    for (size_t i = 0; i < batch_ops.size(); ++i) {
        const KVTOp& op = batch_ops[i];
        KVTOpResult result;
        
        std::string op_error_msg;
        
        switch (op.op) {
            case OP_GET: {
                std::string value;
                if (g_kvt_manager->get(tx_id, op.table_id, op.key, value, op_error_msg)) {
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
                if (g_kvt_manager->set(tx_id, op.table_id, op.key, op.value, op_error_msg)) {
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
                if (g_kvt_manager->del(tx_id, op.table_id, op.key, op_error_msg)) {
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