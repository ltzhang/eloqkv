#include "kvt_mem.h"
#include <algorithm>
#include <stdexcept>

// Global KVT manager instance
std::unique_ptr<KVTManagerWrapperInterface> g_kvt_manager;

// Global KVT interface functions
KVTError kvt_initialize() {
    try {
        g_kvt_manager = std::make_unique<KVTManagerWrapperSimple>(); // Create simple wrapper
        return KVTError::SUCCESS;
    } catch (const std::exception& e) {
        return KVTError::UNKNOWN_ERROR;
    }
}

void kvt_shutdown() {
    g_kvt_manager.reset();
}

KVTError kvt_create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->create_table(table_name, partition_method, table_id, error_msg);
}

KVTError kvt_start_transaction(uint64_t& tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->start_transaction(tx_id, error_msg);
}

KVTError kvt_get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
             std::string& value, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->get(tx_id, table_name, key, value, error_msg);
}

KVTError kvt_set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
             const std::string& value, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->set(tx_id, table_name, key, value, error_msg);
}

KVTError kvt_del(uint64_t tx_id, const std::string& table_name, const std::string& key, 
             std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->del(tx_id, table_name, key, error_msg);
}

KVTError kvt_scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
              const std::string& key_end, size_t num_item_limit, 
              std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->scan(tx_id, table_name, key_start, key_end, num_item_limit, results, error_msg);
}

KVTError kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->commit_transaction(tx_id, error_msg);
}

KVTError kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return KVTError::KVT_NOT_INITIALIZED;
    }
    return g_kvt_manager->rollback_transaction(tx_id, error_msg);
}


//=============================================================================================
