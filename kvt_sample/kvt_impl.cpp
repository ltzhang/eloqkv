/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvt_inc.h"
#include "kvt_mem.h"
#include <unordered_set>

#include <memory>

// Implementation of the KVT API using the sample KVT manager
std::unique_ptr<KVTManagerWrapper> g_kvt_manager = nullptr;

bool kvt_initialize() {
    if (g_kvt_manager) {
        return true;  // Already initialized
    }
    
    try {
        g_kvt_manager = std::make_unique<KVTManagerWrapper>();
        return true;
    } catch (...) {
        return false;
    }
}

void kvt_shutdown() {
    g_kvt_manager.reset();
}

uint64_t kvt_create_table(const std::string& table_name, 
                          const std::string& partition_method,
                          std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return 0;
    }
    
    return g_kvt_manager->create_table(table_name, partition_method, error_msg);
}

uint64_t kvt_start_transaction(std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return 0;
    }
    
    return g_kvt_manager->start_transaction(error_msg);
}

bool kvt_get(uint64_t tx_id, 
            const std::string& table_name,
            const std::string& key,
            std::string& value,
            std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return false;
    }
    
    return g_kvt_manager->get(tx_id, table_name, key, value, error_msg);
}

bool kvt_set(uint64_t tx_id,
            const std::string& table_name,
            const std::string& key,
            const std::string& value,
            std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return false;
    }
    
    return g_kvt_manager->set(tx_id, table_name, key, value, error_msg);
}

bool kvt_del(uint64_t tx_id, 
    const std::string& table_name,
    const std::string& key,
    std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return false;
    }
    
    return g_kvt_manager->del(tx_id, table_name, key, error_msg);
}

bool kvt_scan(uint64_t tx_id,
             const std::string& table_name,
             const std::string& key_start,
             const std::string& key_end,
             size_t num_item_limit,
             std::vector<std::pair<std::string, std::string>>& results,
             std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return false;
    }
    
    return g_kvt_manager->scan(tx_id, table_name, key_start, key_end, num_item_limit, results, error_msg);
}

bool kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return false;
    }
    
    return g_kvt_manager->commit_transaction(tx_id, error_msg);
}

bool kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT not initialized";
        return false;
    }
    
    return g_kvt_manager->rollback_transaction(tx_id, error_msg);
}