/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvt_inc.h"
#include <unordered_set>

#include <memory>

// Implementation of the KVT API using the sample KVT manager
std::unique_ptr<KVTManagerWrapper> g_kvt_manager = nullptr;

KVTError kvt_initialize() {
    if (g_kvt_manager) {
        return KVTError::SUCCESS;  // Already initialized
    }
    
    try {
        g_kvt_manager = std::make_unique<KVTManagerWrapper>();
        return KVTError::SUCCESS;
    } catch (...) {
        return KVTError::UNKNOWN_ERROR;
    }
}

void kvt_shutdown() {
    g_kvt_manager.reset();
}

KVTError kvt_create_table(const std::string& table_name, 
                          const std::string& partition_method,
                          uint64_t& table_id,
                          std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->create_table(table_name, partition_method, table_id, error_msg);
}

KVTError kvt_start_transaction(uint64_t& tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->start_transaction(tx_id, error_msg);
}

KVTError kvt_get(uint64_t tx_id, 
            const std::string& table_name,
            const std::string& key,
            std::string& value,
            std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->get(tx_id, table_name, key, value, error_msg);
}

KVTError kvt_set(uint64_t tx_id,
            const std::string& table_name,
            const std::string& key,
            const std::string& value,
            std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->set(tx_id, table_name, key, value, error_msg);
}

KVTError kvt_del(uint64_t tx_id, 
    const std::string& table_name,
    const std::string& key,
    std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->del(tx_id, table_name, key, error_msg);
}

KVTError kvt_scan(uint64_t tx_id,
             const std::string& table_name,
             const std::string& key_start,
             const std::string& key_end,
             size_t num_item_limit,
             std::vector<std::pair<std::string, std::string>>& results,
             std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->scan(tx_id, table_name, key_start, key_end, num_item_limit, results, error_msg);
}

KVTError kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->commit_transaction(tx_id, error_msg);
}

KVTError kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->rollback_transaction(tx_id, error_msg);
}

KVTError kvt_batch_execute(uint64_t tx_id,
                          const KVTBatchOps& batch_ops,
                          KVTBatchResults& batch_results,
                          std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    
    return g_kvt_manager->batch_execute(tx_id, batch_ops, batch_results, error_msg);
}