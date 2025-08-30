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
    
    uint64_t startTransaction(std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return 0;
        }
        return manager_->doStartTx(error_msg);
    }
    
    bool get(uint64_t tx_id, const std::string& table_name, 
            const std::string& key, std::string& value, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doGet(tx_id, table_name, key, value, error_msg);
    }
    
    bool set(uint64_t tx_id, const std::string& table_name,
            const std::string& key, const std::string& value, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doSet(tx_id, table_name, key, value, error_msg);
    }

    bool del(uint64_t tx_id, const std::string& table_name, 
        const std::string& key, std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doDel(tx_id, table_name, key, error_msg);
    }

    bool scan(uint64_t tx_id, const std::string& table_name,
             const std::string& key_start, const std::string& key_end,
             size_t num_item_limit, std::vector<std::pair<std::string, std::string>>& results,
             std::string& error_msg) {
        if (!manager_) {
            error_msg = "KVTManager not initialized";
            return false;
        }
        return manager_->doScan(tx_id, table_name, key_start, key_end, 
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

bool kvt_initialize() {
    try {
        if (g_kvt_manager) {
            // Already initialized
            return true;
        }
        
        g_kvt_manager = std::make_unique<KVTManagerWrapper>();
        return g_kvt_manager->initialize();
    } catch (const std::exception& e) {
        std::cerr << "KVT initialization failed: " << e.what() << std::endl;
        return false;
    } catch (...) {
        std::cerr << "KVT initialization failed: unknown error" << std::endl;
        return false;
    }
}

void kvt_shutdown() {
    if (g_kvt_manager) {
        g_kvt_manager.reset();
    }
}

uint64_t kvt_create_table(const std::string& table_name, 
                          const std::string& partition_method,
                          std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return 0;
    }
    return g_kvt_manager->createTable(table_name, partition_method, error_msg);
}

uint64_t kvt_start_transaction(std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return 0;
    }
    return g_kvt_manager->startTransaction(error_msg);
}

bool kvt_get(uint64_t tx_id, 
            const std::string& table_name,
            const std::string& key,
            std::string& value,
            std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
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
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return false;
    }
    return g_kvt_manager->set(tx_id, table_name, key, value, error_msg);
}

bool kvt_scan(uint64_t tx_id,
             const std::string& table_name,
             const std::string& key_start,
             const std::string& key_end,
             size_t num_item_limit,
             std::vector<std::pair<std::string, std::string>>& results,
             std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return false;
    }
    return g_kvt_manager->scan(tx_id, table_name, key_start, key_end, 
                               num_item_limit, results, error_msg);
}

bool kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return false;
    }
    return g_kvt_manager->commitTransaction(tx_id, error_msg);
}

bool kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized. Call kvt_initialize() first.";
        return false;
    }
    return g_kvt_manager->rollbackTransaction(tx_id, error_msg);
}