#include "kvt_mem.h"
#include <algorithm>
#include <stdexcept>
// Global KVT manager instance
std::unique_ptr<KVTManagerWrapper> g_kvt_manager;

#if 0 //old code
bool KVTManagerWrapper::acquire_lock_2pl(Transaction* tx, const std::string& table_name, const std::string& key, std::string& error_msg) {
    std::string table_key = make_table_key(table_name, key);
    
    // Check if we already have the lock (check in read_set or write_set)
    if (tx->read_set.find(table_key) != tx->read_set.end() || 
        tx->write_set.find(table_key) != tx->write_set.end()) {
        return true;
    }
    
    // Check if the key is locked by another transaction
    Table* table = get_table(table_name);
    if (!table) {
        error_msg = "Table '" + table_name + "' not found";
        return false;
    }
    
    auto it = table->data.find(key);
    if (it != table->data.end() && it->second.is_locked) {
        error_msg = "Key '" + key + "' is locked by another transaction";
        return false;
    }
    
    // Acquire the lock
    if (it != table->data.end()) {
        it->second.is_locked = true;
    } else {
        // Create new entry with lock
        table->data[key] = Value("", 0);
        table->data[key].is_locked = true;
    }
    
    // Store in read_set to track the lock
    tx->read_set[table_key] = Value("", 0);
    tx->read_set[table_key].is_locked = true;
    return true;
}

// void KVTManagerWrapper::release_locks_2pl(Transaction* tx) {
//     for (const auto& read_pair : tx->read_set) {
//         // Parse table_key back to table_name and key
//         size_t separator_pos = read_pair.first.find('\0');
//         if (separator_pos != std::string::npos) {
//             std::string table_name = read_pair.first.substr(0, separator_pos);
//             std::string key = read_pair.first.substr(separator_pos + 1);
            
//             Table* table = get_table(table_name);
//             if (table) {
//                 auto it = table->data.find(key);
//                 if (it != table->data.end()) {
//                     it->second.is_locked = false;
//                 }
//             }
//         }
//     }
// }

bool KVTManagerWrapper::validate_versions_occ(Transaction* tx, std::string& error_msg) {
    for (const auto& read_pair : tx->read_set) {
        size_t separator_pos = read_pair.first.find('\0');
        if (separator_pos != std::string::npos) {
            std::string table_name = read_pair.first.substr(0, separator_pos);
            std::string key = read_pair.first.substr(separator_pos + 1);
            
            Table* table = get_table(table_name);
            if (table) {
                auto it = table->data.find(key);
                uint64_t current_version = (it != table->data.end()) ? it->second.version : 0;
                
                if (read_pair.second.version != current_version) {
                    error_msg = "Version mismatch for key '" + key + "' - concurrent modification detected";
                    return false;
                }
            }
        }
    }
    return true;
}

bool KVTManagerWrapper::get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                           std::string& value, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    // Handle one-shot operations (auto-commit)
    if (tx_id == 0) {
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table '" + table_name + "' not found";
            return false;
        }
            // For one-shot, directly access the data since we have global lock
        auto it = table->data.find(key);
        if (it == table->data.end()) {
            error_msg = "Key '" + key + "' not found";
            return false;
        }
        //single item read do not need to care about lock or version. 
        value = it->second.data;
        return true;
    }
    
    // Handle transaction-based operations
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    
    // Check if we already have this key in our transaction context
    std::string table_key = make_table_key(table_name, key);
    auto write_itr = tx->write_set.find(table_key);
    if (write_itr != tx->write_set.end()) {
        value = write_itr->second.data;
        return true;
    }
    auto read_itr = tx->read_set.find(table_key);
    if (read_itr != tx->read_set.end()) {
        value = read_itr->second.data;
        return true;
    }
    //we get data from the table.
    Table* table = get_table(table_name);
    if (!table) {
        error_msg = "Table '" + table_name + "' not found";
        return false;
    }
    auto it = table->data.find(key);
    if (it == table->data.end()) {
        error_msg = "Key '" + key + "' not found";
        return false;
    }
    if (use_2pl && it->second.is_locked) {
        error_msg = "Key '" + key + "' is locked by another transaction";
        return false;
    }
    value = it->second.data;
    it->second.is_locked = true;
    if (use_2pl) {
        tx->read_set[table_key] = Value(value, true /*is_locked*/);
    }
    else {
        tx->read_set[table_key] = Value(value, 0 /*version*/);
    }
    return true;
}

bool KVTManagerWrapper::set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                           const std::string& value_str, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    // Handle one-shot operations (auto-commit)
    if (tx_id == 0) {
        // For one-shot, directly access the data since we have global lock
        // Check if key is locked by another transaction (for 2PL)
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table '" + table_name + "' not found";
            return false;
        }
        auto it = table->data.find(key);
        if (use_2pl && it != table->data.end() && it->second.is_locked) {
            error_msg = "Key '" + key + "' is locked by another transaction";
            return false;
        }
        // Perform the write
        if (use_2pl) {
            table->data[key] = Value(value_str, false);
        } else {
            // For OCC, increment version
            uint64_t new_version = it != table->data.end() ? 
                                  table->data[key].version + 1 : 1;
            table->data[key] = Value(value_str, new_version);
        }
        return true;
    }
    // Handle transaction-based operations
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    std::string table_key = make_table_key(table_name, key);
    //1. already in write set, just update the value.
    auto write_itr = tx->write_set.find(table_key);
    if (write_itr != tx->write_set.end()) {
        write_itr->second.data = value_str;
        return true;
    }
    auto read_itr = tx->read_set.find(table_key);
    if (read_itr == tx->read_set.end()) {
        //2. not in read set, just put in the writeset. 
        if (use_2pl)
            tx->write_set[table_key] = Value(value_str, false);
        else
            tx->write_set[table_key] = Value(value_str, 0); //0 means we have not checked from table. 
        return true;
    }
    else {
        tx->write_set[table_key] = Value(value_str, read_itr->second.is_locked, read_itr->second.version);
        tx->read_set.erase(read_itr);
    }
    return true;
}

bool KVTManagerWrapper::scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
                            const std::string& key_end, size_t num_item_limit, 
                            std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    Table* table = get_table(table_name);
    if (!table) {
        error_msg = "Table '" + table_name + "' not found";
        return false;
    }
    
    if (table->partition_method != "range") {
        error_msg = "Scan operation only supported on range-partitioned tables";
        return false;
    }
    
    results.clear();
    
    // Handle one-shot operations (auto-commit)
    if (tx_id == 0) {
        auto itr = table->data.lower_bound(key_start);
        auto end_itr = table->data.upper_bound(key_end);
        while (itr != end_itr && results.size() < num_item_limit) {
            results.emplace_back(itr->first, itr->second.data);
            ++itr;
        }
        return true;
    }
    // Handle transaction-based operations
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    
    // For scan, we need to acquire locks or check versions for all keys in range
    auto itr = table->data.lower_bound(key_start);
    auto end_itr = table->data.upper_bound(key_end);
    std::vector<std::pair<std::string, std::string>> temp_results;

    while (itr != end_itr && temp_results.size() < num_item_limit) {
        if (use_2pl && itr->second.is_locked) {
            error_msg = "Key '" + itr->first + "' is locked by another transaction";
            //clean up the temp_results.
            for (const auto& kv_pair : temp_results) {
                table->data[kv_pair.first].is_locked = false;
            }
            temp_results.clear();
            error_msg = "Key '" + itr->first + "' is locked by another transaction";
            return false;
        }
        temp_results.emplace_back(itr->first, itr->second.data);
        ++itr;
    }
    for (const auto& kv_pair : temp_results) {
        if (results.size() < num_item_limit) {
            results.emplace_back(kv_pair.first, kv_pair.second);
        }
    }


    return true;


    for (const auto& kv_pair : table->data) {
        if (kv_pair.first >= key_start && kv_pair.first <= key_end) {
            if (use_2pl) {
                if (!acquire_lock_2pl(tx, table_name, kv_pair.first, error_msg)) {
                    return false;
                }
            } else {
                if (!check_version_occ(tx, table_name, kv_pair.first, error_msg)) {
                    return false;
                }
            }
        }
    }
    
    // Perform the scan - merge table data with transaction read/write sets
    std::map<std::string, std::string> merged_results;
    
    // First, add results from the table
    for (const auto& kv_pair : table->data) {
        if (kv_pair.first >= key_start && kv_pair.first <= key_end) {
            merged_results[kv_pair.first] = kv_pair.second.data;
        }
    }
    
    // Then, override with transaction write_set values
    for (const auto& write_pair : tx->write_set) {
        size_t separator_pos = write_pair.first.find('\0');
        if (separator_pos != std::string::npos) {
            std::string table_name_part = write_pair.first.substr(0, separator_pos);
            std::string key_part = write_pair.first.substr(separator_pos + 1);
            
            if (table_name_part == table_name && key_part >= key_start && key_part <= key_end) {
                merged_results[key_part] = write_pair.second.data;
            }
        }
    }
    
    // Convert to results vector
    for (const auto& kv_pair : merged_results) {
        if (results.size() < num_item_limit) {
            results.emplace_back(kv_pair.first, kv_pair.second);
        }
    }
    
    return true;
}

bool KVTManagerWrapper::commit_transaction(uint64_t tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    
    if (use_2pl) {
        // For 2PL, just release locks and install values
        for (const auto& write_pair : tx->write_set) {
            size_t separator_pos = write_pair.first.find('\0');
            if (separator_pos != std::string::npos) {
                std::string table_name = write_pair.first.substr(0, separator_pos);
                std::string key = write_pair.first.substr(separator_pos + 1);
                
                Table* table = get_table(table_name);
                if (table) {
                    table->data[key] = Value(write_pair.second.data, 0);
                }
            }
        }
        
        release_locks_2pl(tx);
    } else {
        // For OCC, validate versions first
        if (!validate_versions_occ(tx, error_msg)) {
            return false;
        }
        
        // Install new values with incremented versions
        for (const auto& write_pair : tx->write_set) {
            size_t separator_pos = write_pair.first.find('\0');
            if (separator_pos != std::string::npos) {
                std::string table_name = write_pair.first.substr(0, separator_pos);
                std::string key = write_pair.first.substr(separator_pos + 1);
                
                Table* table = get_table(table_name);
                if (table) {
                    uint64_t new_version = (table->data.find(key) != table->data.end()) ? 
                                          table->data[key].version + 1 : 1;
                    table->data[key] = Value(write_pair.second.data, new_version);
                }
            }
        }
    }
    
    // Clean up transaction
    transactions.erase(tx_id);
    
    return true;
}

bool KVTManagerWrapper::rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    
    // Release locks if using 2PL
    if (use_2pl) {
        release_locks_2pl(tx);
    }
    
    // Clean up transaction
    transactions.erase(tx_id);
    
    return true;
}
#endif
// Global KVT interface functions
bool kvt_initialize() {
    try {
        g_kvt_manager = std::make_unique<KVTManagerWrapper>(); // Create simple wrapper
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

void kvt_shutdown() {
    g_kvt_manager.reset();
}

bool kvt_create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return 0;
    }
    return g_kvt_manager->create_table(table_name, partition_method, table_id, error_msg);
}

bool kvt_start_transaction(uint64_t& tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return 0;
    }
    return g_kvt_manager->start_transaction(error_msg);
}

bool kvt_get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
             std::string& value, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return false;
    }
    return g_kvt_manager->get(tx_id, table_name, key, value, error_msg);
}

bool kvt_set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
             const std::string& value, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return false;
    }
    return g_kvt_manager->set(tx_id, table_name, key, value, error_msg);
}

bool kvt_scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
              const std::string& key_end, size_t num_item_limit, 
              std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return false;
    }
    return g_kvt_manager->scan(tx_id, table_name, key_start, key_end, num_item_limit, results, error_msg);
}

bool kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return false;
    }
    return g_kvt_manager->commit_transaction(tx_id, error_msg);
}

bool kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    if (!g_kvt_manager) {
        error_msg = "KVT system not initialized";
        return false;
    }
    return g_kvt_manager->rollback_transaction(tx_id, error_msg);
}
