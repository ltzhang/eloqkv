#include <algorithm>      
#include <stdexcept>
#include <iostream>
#include "kvt_mem.h"    

// Global KVT manager instance
std::unique_ptr<KVTWrapper> g_kvt_manager;
int g_verbosity = 0;
int g_sanity_check_level = 0;

KVTError kvt_set_verbosity(int verbosity) {
    g_verbosity = verbosity;
    return KVTError::SUCCESS;
}

KVTError kvt_set_sanity_check_level(int level) {
    g_sanity_check_level = level;
    return KVTError::SUCCESS;
}

// Global KVT interface functions
KVTError kvt_initialize() {
    try {
        //g_kvt_manager = std::make_unique<KVTMemManagerNoCC>(); // Create simple wrapper
        //g_kvt_manager = std::make_unique<KVTMemManagerSimple>(); // Create simple wrapper
        //g_kvt_manager = std::make_unique<KVTMemManager2PL>(); // Create simple wrapper
        g_kvt_manager = std::make_unique<KVTMemManagerOCC>(); // Create simple wrapper
        return KVTError::SUCCESS;
    } catch (const std::exception& e) {
        return KVTError::UNKNOWN_ERROR;
    }
}

void kvt_shutdown() {
    g_kvt_manager.reset();
}

KVTWrapper& kvt_manager() {
    if (g_kvt_manager == nullptr) {
        std::cerr << "^^^Warning: kvt_manager() is called before kvt_initialize()" << std::endl;
        kvt_initialize();
    }
    return *g_kvt_manager.get();
}

KVTError kvt_create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_create_table: table_name=" << table_name << ", partition_method=" << partition_method);
    KVTError result = kvt_manager().create_table(table_name, partition_method, table_id, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, table_id=" << table_id << std::endl; );
    return result;
}

KVTError kvt_drop_table(uint64_t table_id, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_drop_table: table_id=" << table_id);
    KVTError result = kvt_manager().drop_table(table_id, error_msg);
    VERBOSE( 
        if (result != KVTError::SUCCESS) 
            std::cout << " -> ERROR: " << error_msg << std::endl; 
        else 
            std::cout << " -> SUCCESS" << std::endl;);
    return result;
}

KVTError kvt_get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_get_table_name: table_id=" << table_id);
    KVTError result = kvt_manager().get_table_name(table_id, table_name, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, table_name=" << table_name << std::endl;
    );
    return result;
}

KVTError kvt_get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_get_table_id: table_name=" << table_name);
    KVTError result = kvt_manager().get_table_id(table_name, table_id, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, table_id=" << table_id << std::endl;
    );
    return result;
}

KVTError kvt_list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_list_tables");
    KVTError result = kvt_manager().list_tables(results, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, count=" << results.size() << std::endl;
    );
    return result;
}

KVTError kvt_start_transaction(uint64_t& tx_id, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_start_transaction");
    KVTError result = kvt_manager().start_transaction(tx_id, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, tx_id=" << tx_id << std::endl;
    );
    return result;
}

KVTError kvt_get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
             std::string& value, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_get: tx_id=" << tx_id << ", table_id=" << table_id << ", key=" << key);
    KVTError result = kvt_manager().get(tx_id, table_id, key, value, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, value=" << value << std::endl;
    );
    return result;
}

KVTError kvt_set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
             const std::string& value, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_set: tx_id=" << tx_id << ", table_id=" << table_id << ", key=" << key << ", value=" << value);
    KVTError result = kvt_manager().set(tx_id, table_id, key, value, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS" << std::endl;
    );
    return result;
}

KVTError kvt_del(uint64_t tx_id, uint64_t table_id, const std::string& key, 
             std::string& error_msg) {
    VERBOSE(std::cout << "kvt_del: tx_id=" << tx_id << ", table_id=" << table_id << ", key=" << key);
    KVTError result = kvt_manager().del(tx_id, table_id, key, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS" << std::endl;
    );
    return result;
}

KVTError kvt_scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
              const std::string& key_end, size_t num_item_limit, 
              std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_scan: tx_id=" << tx_id << ", table_id=" << table_id << ", key_start=" << key_start << ", key_end=" << key_end << ", limit=" << num_item_limit);
    KVTError result = kvt_manager().scan(tx_id, table_id, key_start, key_end, num_item_limit, results, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, count=" << results.size() << std::endl;
    );       
    return result;
}


KVTError kvt_update(uint64_t tx_id, 
                        uint64_t table_id,
                        const std::string& key,
                        KVUpdateFunc & func,
                        const std::string& parameter,
                        std::string& result_value,
                        std::string& error_msg) 
{
    VERBOSE(std::cout << "kvt_update: tx_id=" << tx_id << ", table_id=" << table_id << ", key=" << key);
    KVTError result = kvt_manager().update(tx_id, table_id, key, func, parameter, result_value, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, result_value=" << result_value << std::endl;
    );
    return result;
}

KVTError kvt_range_update(uint64_t tx_id, 
                            uint64_t table_id,
                            const std::string& key_start,
                            const std::string& key_end,
                            size_t num_item_limit,
                            KVUpdateFunc & func,
                            const std::string& parameter,
                            std::vector<std::pair<std::string, std::string>>& results,
                            std::string& error_msg)
{
    VERBOSE(std::cout << "kvt_range_update: tx_id=" << tx_id << ", table_id=" << table_id << ", key_start=" << key_start << ", key_end=" << key_end << ", limit=" << num_item_limit);
    KVTError result = kvt_manager().range_update(tx_id, table_id, key_start, key_end, num_item_limit, func, parameter, results, error_msg);
    VERBOSE(
        if (result != KVTError::SUCCESS)
            std::cout << " -> ERROR: " << error_msg << std::endl;
        else
            std::cout << " -> SUCCESS, count=" << results.size() << std::endl;
    );
    return result;
}



// KVTError kvt_ext_get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
//              KVTExtGetFunc & func, std::string& value, std::string& error_msg) {
//     std::string intermediate_value;
//     KVTError result = kvt_manager().get(tx_id, table_id, key, intermediate_value, error_msg);
//     if (result != KVTError::SUCCESS) 
//         return result;
//     result = func(intermediate_value, value);
//     return result;
// }

// KVTError kvt_ext_set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
//              const std::string& value, KVTExtSetFunc & func, std::string& error_msg) {
//     std::string intermediate_value;
//     return result;
// }

// KVTError kvt_ext_del(uint64_t tx_id, uint64_t table_id, const std::string& key, 
//              KVTExtDelFunc & func, std::string& error_msg) {
//     VERBOSE(std::cout << "kvt_ext_del: tx_id=" << tx_id << ", table_id=" << table_id << ", key=" << key);
//     KVTError result = kvt_manager().ext_del(tx_id, table_id, key, func, error_msg);
//     if (result != KVTError::SUCCESS) {
//         VERBOSE(std::cout << " -> ERROR: " << error_msg << std::endl);
//     } else {
//         VERBOSE(std::cout << " -> SUCCESS" << std::endl);
//     }
//     return result;
// }

// KVTError kvt_ext_scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
//               const std::string& key_end, size_t num_item_limit, 
//               KVTExtScanFunc & func, std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
//     VERBOSE(std::cout << "kvt_ext_scan: tx_id=" << tx_id << ", table_id=" << table_id << ", key_start=" << key_start << ", key_end=" << key_end << ", limit=" << num_item_limit);
//     KVTError result = kvt_manager().ext_scan(tx_id, table_id, key_start, key_end, num_item_limit, func, results, error_msg);
//     if (result != KVTError::SUCCESS) {
//         VERBOSE(std::cout << " -> ERROR: " << error_msg << std::endl);
//     } else {
//         VERBOSE(std::cout << " -> SUCCESS, count=" << results.size() << std::endl);
//     }
//     return result;
// }

KVTError kvt_commit_transaction(uint64_t tx_id, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_commit_transaction: tx_id=" << tx_id);
    KVTError result = kvt_manager().commit_transaction(tx_id, error_msg);
    if (result != KVTError::SUCCESS) {
        VERBOSE(std::cout << " -> ERROR: " << error_msg << std::endl);
    } else {
        VERBOSE(std::cout << " -> SUCCESS" << std::endl);
    }
    return result;
}

KVTError kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_rollback_transaction: tx_id=" << tx_id);
    KVTError result = kvt_manager().rollback_transaction(tx_id, error_msg);
    if (result != KVTError::SUCCESS) {
        VERBOSE(std::cout << " -> ERROR: " << error_msg << std::endl);
    } else {
        VERBOSE(std::cout << " -> SUCCESS" << std::endl);
    }
    return result;
}

KVTError kvt_batch_execute(uint64_t tx_id, const KVTBatchOps& batch_ops, 
                          KVTBatchResults& batch_results, std::string& error_msg) {
    VERBOSE(std::cout << "kvt_batch_execute: tx_id=" << tx_id << ", ops_count=" << batch_ops.size());
    KVTError result = kvt_manager().batch_execute(tx_id, batch_ops, batch_results, error_msg);
    if (result != KVTError::SUCCESS) {
        VERBOSE(std::cout << " -> ERROR: " << error_msg << std::endl);
    } else {
        VERBOSE(std::cout << " -> SUCCESS, results_count=" << batch_results.size() << std::endl);
    }
    return result;
}


//=============================================================================================

// Table management
KVTError KVTMemManagerNoCC::create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (table_to_id.find(table_name) != table_to_id.end()) {
        error_msg = "Table " + table_name + " already exists";
        return KVTError::TABLE_ALREADY_EXISTS;
    }
    table_to_id[table_name] = next_table_id;
    std::cout << "create_table " << table_name << " as TableID" << next_table_id << std::endl;
    next_table_id += 1;
    table_id = next_table_id - 1;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerNoCC::drop_table(uint64_t table_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    std::string table_name;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_name = pair.first;
            break;
        }
    }
    if (table_name.empty()) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    // Remove all data associated with this table
    auto it = table_data.begin();
    while (it != table_data.end()) {
        std::pair<uint64_t, std::string> parsed = parse_table_key(it->first);
        if (parsed.first == table_id) {
            it = table_data.erase(it);
        } else {
            ++it;
        }
    }
    
    // Remove table from table_to_id map
    table_to_id.erase(table_name);
    std::cout << "drop_table " << table_name << std::endl;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerNoCC::get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_name = pair.first;
            return KVTError::SUCCESS;
        }
    }
    error_msg = "Table with ID " + std::to_string(table_id) + " not found";
    return KVTError::TABLE_NOT_FOUND;
}

KVTError KVTMemManagerNoCC::get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    auto it = table_to_id.find(table_name);
    if (it == table_to_id.end()) {
        error_msg = "Table " + table_name + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    table_id = it->second;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerNoCC::list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    results.clear();
    for (const auto& pair : table_to_id) {
        results.emplace_back(pair.first, pair.second);
    }
    return KVTError::SUCCESS;
}
KVTError KVTMemManagerNoCC::start_transaction(uint64_t& tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    std::cout << "start_transaction " << next_tx_id << std::endl;
    next_tx_id += 1;
    tx_id = next_tx_id - 1;
    return KVTError::SUCCESS;
}
KVTError KVTMemManagerNoCC::commit_transaction(uint64_t tx_id, std::string& error_msg) 
{
    std::cout << "commit_transaction " << tx_id << std::endl;
    return KVTError::SUCCESS;
}
KVTError KVTMemManagerNoCC::rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    std::cout << "rollback_transaction " << tx_id << std::endl;
    return KVTError::SUCCESS;
}
// Data operations  
KVTError KVTMemManagerNoCC::get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
            std::string& value, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (tx_id >= next_tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }

    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key);
    auto it = table_data.find(table_key);
    if (it == table_data.end()) {
        error_msg = "Key " + key + " not found";
        return KVTError::KEY_NOT_FOUND;
    }
    value = it->second;
    std::cout << "get " << table_id << ":" << key << " = " << value << std::endl;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerNoCC::set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
            const std::string& value, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (tx_id >= next_tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key);
    table_data[table_key] = value;
    std::cout << "set " << table_id << ":" << key << " = " << value << std::endl;
    return KVTError::SUCCESS;

}
KVTError KVTMemManagerNoCC::del(uint64_t tx_id, uint64_t table_id, 
        const std::string& key, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (tx_id >= next_tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key);
    if (table_data.find(table_key) == table_data.end()) {
        std::cout << "del " << table_id << ":" << key << " not found" << std::endl;
        error_msg = "Key " + key + " not found";
        return KVTError::KEY_NOT_FOUND;
    }
    else {
        table_data.erase(table_key);
        std::cout << "del " << table_id << ":" << key << std::endl;
        return KVTError::SUCCESS;
    }
}

KVTError KVTMemManagerNoCC::scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
            const std::string& key_end, size_t num_item_limit, 
            std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (tx_id >= next_tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key_start);
    std::string table_key_end = make_table_key(table_id, key_end);
    auto itr = table_data.lower_bound(table_key);
    auto end_itr = table_data.upper_bound(table_key_end);
    KVTError result = KVTError::SUCCESS;
    while (itr != end_itr) { 
        results.emplace_back(parse_table_key(itr->first).second, itr->second);
        ++itr;
        if (results.size() >= num_item_limit) {
            result = KVTError::SCAN_LIMIT_REACHED;
            break;
        }
    }
    std::cout << "scan " << table_id << ":" << key_start << " to " << key_end << " = " << results.size() << " items" << std::endl;
    return result;
}

//==================================KVT ManagerWrapperSimple ===========================================================

KVTError KVTMemManagerSimple::create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    if (table_to_id.find(table_name) != table_to_id.end()) {
        error_msg = "Table " + table_name + " already exists";
        return KVTError::TABLE_ALREADY_EXISTS;
    }
    table_to_id[table_name] = next_table_id;
    next_table_id += 1;
    table_id = next_table_id - 1;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::drop_table(uint64_t table_id, std::string& error_msg)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    std::string table_name;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_name = pair.first;
            break;
        }
    }
    if (table_name.empty()) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    // Remove all data associated with this table
    auto it = table_data.begin();
    while (it != table_data.end()) {
        std::pair<uint64_t, std::string> parsed = parse_table_key(it->first);
        if (parsed.first == table_id) {
            it = table_data.erase(it);
        } else {
            ++it;
        }
    }
    
    // Remove table from table_to_id map
    table_to_id.erase(table_name);
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_name = pair.first;
            return KVTError::SUCCESS;
        }
    }
    error_msg = "Table with ID " + std::to_string(table_id) + " not found";
    return KVTError::TABLE_NOT_FOUND;
}

KVTError KVTMemManagerSimple::get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    auto it = table_to_id.find(table_name);
    if (it == table_to_id.end()) {
        error_msg = "Table " + table_name + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    table_id = it->second;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    results.clear();
    for (const auto& pair : table_to_id) {
        results.emplace_back(pair.first, pair.second);
    }
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::start_transaction(uint64_t& tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (current_tx_id != 0) {
        error_msg = "A transaction is already running";
        return KVTError::TRANSACTION_ALREADY_RUNNING;
    }
    current_tx_id = next_tx_id;
    next_tx_id += 1;
    tx_id = current_tx_id;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::commit_transaction(uint64_t tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (current_tx_id != tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Apply changes
    table_data.insert(write_set.begin(), write_set.end());
    for (const auto& key : delete_set) {
        //when inserting into delete set, we removed it from write set.
        assert (write_set.find(key) == write_set.end());
        //when inserting into delete set, we checked table has it.
        assert (table_data.find(key) != table_data.end());
        table_data.erase(key);
    }
    
    write_set.clear();
    delete_set.clear();
    current_tx_id = 0;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::rollback_transaction(uint64_t tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    if (current_tx_id != tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    write_set.clear();
    delete_set.clear();
    current_tx_id = 0;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
            std::string& value, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }

    //if tx_id is 0, it is a one-shot read operation, so we just get the data from the table.
    //otherwise, we only allow a single transaction at a time, so we check if the transaction id is the current one.
    if (tx_id != 0 && current_tx_id != tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    std::string table_key = make_table_key(table_id, key);
    auto itr = write_set.find(table_key);
    if (itr != write_set.end()) {
        value = itr->second;
        return KVTError::SUCCESS;
    }
    auto itr1 = delete_set.find(table_key);
    if (itr1 != delete_set.end()) {
        error_msg = "Key " + key + " is deleted";
        return KVTError::KEY_IS_DELETED;
    }
    auto it = table_data.find(table_key);
    if (it == table_data.end()) {
        error_msg = "Key " + key + " not found";
        return KVTError::KEY_NOT_FOUND;
    }
    value = it->second;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
            const std::string& value, std::string& error_msg)
{
    std::lock_guard<std::mutex> lock(global_mutex);
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    //we cannot allow one shot mutation operations when another transaction is running.
    //When current_tx_id is 0, (i.e. no ongoing transaction) we allow one shot mutation (which also have tx_id 0).
    //we only allow a single transaction at a time, so we check if the transaction id is the current one.
    if (current_tx_id != tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    //the invariant is that when a key is deleted, it must not be in the write set. vice versa.
    std::string table_key = make_table_key(table_id, key);
    auto itr = delete_set.find(table_key);
    if (itr != delete_set.end()) 
        delete_set.erase(itr);
    write_set[table_key] = value;
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::del(uint64_t tx_id, uint64_t table_id, const std::string& key, 
    std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    //we cannot allow one shot mutation operations when another transaction is running.
    //When current_tx_id is 0, (i.e. no ongoing transaction) we allow one shot mutation (which also have tx_id 0).
    //we only allow a single transaction at a time, so we check if the transaction id is the current one.
    if (current_tx_id != tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    std::string table_key = make_table_key(table_id, key);
    //the invariant is that when a key is deleted, it must not be in the write set. vice versa.
    auto itr = write_set.find(table_key);
    if (itr != write_set.end()) {
        write_set.erase(itr);
        return KVTError::SUCCESS;
    }
    auto itr1 = table_data.find(table_key);
    if (itr1 == table_data.end()) {
        error_msg = "Key " + key + " not found, cannot be deleted";
        return KVTError::KEY_NOT_FOUND;
    }
    delete_set.insert(table_key);
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerSimple::scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
            const std::string& key_end, size_t num_item_limit, 
            std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    // Check if table_id exists
    bool table_exists = false;
    for (const auto& pair : table_to_id) {
        if (pair.second == table_id) {
            table_exists = true;
            break;
        }
    }
    if (!table_exists) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    //if tx_id is 0, it is a one-shot read operation, so we just get the data from the table.
    //otherwise, we only allow a single transaction at a time, so we check if the transaction id is the current one.
    if (tx_id != 0 && current_tx_id != tx_id) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    results.clear();
    std::string table_key = make_table_key(table_id, key_start);
    std::string table_key_end = make_table_key(table_id, key_end);
    
    // First, scan table_data for existing keys
    std::map<std::string, std::string> result_map; 
    auto itr = table_data.lower_bound(table_key);
    auto end_itr = table_data.upper_bound(table_key_end);
    result_map.insert(itr, end_itr);
    for (const auto& key :delete_set)
        result_map.erase(key);
    itr = write_set.lower_bound(table_key);
    end_itr = write_set.upper_bound(table_key_end);
    result_map.insert(itr, end_itr);    
    KVTError r = KVTError::SUCCESS;
    for (auto & [table_key, value] : result_map) {
        auto [table_id_parsed, key] = parse_table_key(table_key);
        results.emplace_back(key, value);
        if (results.size() >= num_item_limit) {
            r = KVTError::SCAN_LIMIT_REACHED;
            break;
        }
    }
    return r;
}


//==================================KVT KVTMemManager2PL ===========================================================

KVTError KVTMemManager2PL::commit_transaction(uint64_t tx_id, std::string& error_msg) {
    std::lock_guard<std::mutex> lock(global_mutex);
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Apply deletes first
    for (const auto& delete_key : tx->delete_set) {
        auto [table_id_parsed, key] = parse_table_key(delete_key);
        Table* table = get_table_by_id(table_id_parsed);
        if (table) {
            auto it = table->data.find(key);
            if (it != table->data.end()) {
                // Verify we hold the lock
                assert(it->second.metadata == static_cast<int32_t>(tx_id));
                table->data.erase(it);
            }
        }
    }
    
    // Apply writes
    for (const auto& [write_key, entry] : tx->write_set) {
        auto [table_id_parsed, key] = parse_table_key(write_key);
        Table* table = get_table_by_id(table_id_parsed);
        if (table) {
            auto it = table->data.find(key);
            if (it != table->data.end()) {
                // Verify we hold the lock
                assert(it->second.metadata == static_cast<int32_t>(tx_id));
                std::string old_value = it->second.data;
                // Install the write and release the lock
                it->second.data = entry.data;
                it->second.metadata = 0;  // Release lock
            } else {
                // This shouldn't happen - we should have created a placeholder
                // But handle it just in case
                std::cout << "  VERBOSE: Creating new entry key=" << key << " value=" << entry.data << std::endl;
                table->data[key] = Entry(entry.data, 0);
            }
        }
    }
    
    for (const auto& [read_key, entry] : tx->read_set) {
        // Skip if this key was also written or deleted
        if (tx->write_set.find(read_key) != tx->write_set.end() ||
            tx->delete_set.find(read_key) != tx->delete_set.end()) {
            continue;
        }
        
        auto [table_id_parsed, key] = parse_table_key(read_key);
        Table* table = get_table_by_id(table_id_parsed);
        if (table) {
            auto it = table->data.find(key);
            if (it != table->data.end()) {
                assert(it->second.metadata == static_cast<int32_t>(tx_id));
                it->second.metadata = 0;  // Release the lock
            }
        }
    }
    
    transactions.erase(tx_id);
    return KVTError::SUCCESS;
}

KVTError KVTMemManager2PL::rollback_transaction(uint64_t tx_id, std::string& error_msg)  {
    std::lock_guard<std::mutex> lock(global_mutex);
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Release all locks held by this transaction
    // Release write locks (for keys that were to be written but not yet)
    for (const auto& [write_key, entry] : tx->write_set) {
        auto [table_id_parsed, key] = parse_table_key(write_key);
        Table* table = get_table_by_id(table_id_parsed);
        if (table) {
            auto it = table->data.find(key);
            if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                // If this was a new key (metadata=1 in write_set entry), remove the placeholder
                if (entry.metadata == 1) {
                    table->data.erase(it);
                } else {
                    it->second.metadata = 0;  // Release the lock
                }
            }
        }
    }
    
    // Release read locks
    for (const auto& [read_key, entry] : tx->read_set) {
        auto [table_id_parsed, key] = parse_table_key(read_key);
        Table* table = get_table_by_id(table_id_parsed);
        if (table) {
            auto it = table->data.find(key);
            if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                it->second.metadata = 0;  // Release the lock
            }
        }
    }
    
    // Release delete locks
    for (const auto& delete_key : tx->delete_set) {
        auto [table_id_parsed, key] = parse_table_key(delete_key);
        Table* table = get_table_by_id(table_id_parsed);
        if (table) {
            auto it = table->data.find(key);
            if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                it->second.metadata = 0;  // Release the lock
            }
        }
    }
    
    transactions.erase(tx_id);
    return KVTError::SUCCESS;
}

KVTError KVTMemManager2PL::get(uint64_t tx_id, uint64_t table_id, const std::string& key,
            std::string& value, std::string& error_msg)  {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    // One-shot read
    if (tx_id == 0) {
        Table* table = get_table_by_id(table_id);
        if (!table) {
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }
        auto it = table->data.find(key);
        if (it == table->data.end()) {
            error_msg = "Key " + key + " not found";
            return KVTError::KEY_NOT_FOUND;
        }
        if (it->second.metadata != 0) {
            error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
            return KVTError::KEY_IS_LOCKED;
        }
        value = it->second.data;
        return KVTError::SUCCESS;
    }
    
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key);
    
    // Check if deleted in this transaction
    if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
        error_msg = "Key " + key + " is deleted";
        return KVTError::KEY_IS_DELETED;
    }
    
    // Check write set
    auto write_it = tx->write_set.find(table_key);
    if (write_it != tx->write_set.end()) {
        value = write_it->second.data;
        return KVTError::SUCCESS;
    }
    
    // Check read set
    auto read_it = tx->read_set.find(table_key);
    if (read_it != tx->read_set.end()) {
        value = read_it->second.data;
        return KVTError::SUCCESS;
    }
    
    // Need to read from table and acquire lock
    Table* table = get_table_by_id(table_id);
    if (!table) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    auto it = table->data.find(key);
    if (it == table->data.end()) {
        error_msg = "Key " + key + " not found";
        return KVTError::KEY_NOT_FOUND;
    }
    
    // Check if locked by another transaction
    if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
        error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
        return KVTError::KEY_IS_LOCKED;
    }
    
    // Acquire lock
    it->second.metadata = static_cast<int32_t>(tx_id);
    tx->read_set[table_key] = it->second;
    value = it->second.data;
    return KVTError::SUCCESS;
}

KVTError KVTMemManager2PL::set(uint64_t tx_id, uint64_t table_id, const std::string& key,
            const std::string& value, std::string& error_msg)  {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    if (tx_id == 0) {
        Table* table = get_table_by_id(table_id);
        if (!table) {
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }
        auto itr = table->data.find(key);
        if (itr == table->data.end()) {
            table->data[key] = Entry(value, 0); //not locked
            return KVTError::SUCCESS;
        }
        if (itr->second.metadata == 0) { //not locked
            itr->second.data = value; 
            return KVTError::SUCCESS;
        }
        else {
            error_msg = "Key " + key + " is locked by transaction " + std::to_string(itr->second.metadata);
            return KVTError::KEY_IS_LOCKED; 
        }
    }
    
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key);
    
    // Remove from delete set if it was there
    tx->delete_set.erase(table_key);
    
    // Check if we already have it in write set
    if (tx->write_set.find(table_key) != tx->write_set.end()) {
        tx->write_set[table_key].data = value;
        return KVTError::SUCCESS;
    }
    
    // Need to acquire lock if we don't already have it
    Table* table = get_table_by_id(table_id);
    if (!table) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    auto it = table->data.find(key);
    bool key_exists = (it != table->data.end());
    
    // If key exists, check if it's locked
    if (key_exists) {
        if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
            error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
            return KVTError::KEY_IS_LOCKED;
        }
        // Acquire or maintain lock
        it->second.metadata = static_cast<int32_t>(tx_id);
        // Track original value in read set if not already there
        if (tx->read_set.find(table_key) == tx->read_set.end()) {
            tx->read_set[table_key] = it->second;
        }
    } else {
        // New key - create placeholder with lock
        table->data[key] = Entry("", static_cast<int32_t>(tx_id));
    }
    
    // Add to write set
    tx->write_set[table_key] = Entry(value, key_exists ? 0 : 1);  // metadata tracks if new
    return KVTError::SUCCESS;
}

KVTError KVTMemManager2PL::del(uint64_t tx_id, uint64_t table_id, const std::string& key,
            std::string& error_msg)  {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    if (tx_id == 0) {
        Table* table = get_table_by_id(table_id);
        if (!table) {
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }
        auto itr = table->data.find(key);
        if (itr == table->data.end()) {
            error_msg = "Key " + key + " not found";
            return KVTError::KEY_NOT_FOUND;
        }
        if (itr->second.metadata == 0) { //not locked
            table->data.erase(itr);
            return KVTError::SUCCESS;
        }
        else {
            error_msg = "Key " + key + " is locked by transaction " + std::to_string(itr->second.metadata);
            return KVTError::KEY_IS_LOCKED; 
        }
    }
    
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    std::string table_key = make_table_key(table_id, key);
    
    // Remove from write set if it was there
    auto write_it = tx->write_set.find(table_key);
    if (write_it != tx->write_set.end()) {
        // If it was a new key we were going to add, just remove it
        if (write_it->second.metadata == 1) {
            // Release the lock on the placeholder
            Table* table = get_table_by_id(table_id);
            if (table) {
                auto it = table->data.find(key);
                if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                    table->data.erase(it);
                }
            }
            tx->write_set.erase(write_it);
            return KVTError::SUCCESS;
        }
        tx->write_set.erase(write_it);
    }
    
    // Need to acquire lock on the key to delete
    Table* table = get_table_by_id(table_id);
    if (!table) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    auto it = table->data.find(key);
    if (it == table->data.end()) {
        error_msg = "Key " + key + " not found";
        return KVTError::KEY_NOT_FOUND;
    }
    
    // Check if locked by another transaction
    if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
        error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
        return KVTError::KEY_IS_LOCKED;
    }
    
    // Acquire lock
    it->second.metadata = static_cast<int32_t>(tx_id);
    
    // Add to read set (to track the lock) and delete set
    if (tx->read_set.find(table_key) == tx->read_set.end()) {
        tx->read_set[table_key] = it->second;
    }
    tx->delete_set.insert(table_key);
    return KVTError::SUCCESS;
}

KVTError KVTMemManager2PL::scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start,
            const std::string& key_end, size_t num_item_limit,
            std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg)  {
    std::lock_guard<std::mutex> lock(global_mutex);
    
    Table* table = get_table_by_id(table_id);
    if (!table) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    
    results.clear();
    
    // One-shot scan
    if (tx_id == 0) {
        for (auto it = table->data.lower_bound(key_start); 
                it != table->data.end() && it->first < key_end && results.size() < num_item_limit;
                ++it) {
            //read operation just go through all keys, as locks are supposed to be "read lock"
            results.emplace_back(it->first, it->second.data);
        }
        return KVTError::SUCCESS;
    }
    
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    // Collect results from write set and table
    std::map<std::string, std::string> temp_results;
    
    // First add from write set
    std::string table_key_start = make_table_key(table_id, key_start);
    std::string table_key_end = make_table_key(table_id, key_end);

    for (auto it = tx->write_set.lower_bound(table_key_start);
            it != tx->write_set.end() && it->first < table_key_end;
            ++it) {
        auto [tbl_id, key] = parse_table_key(it->first);
        assert (tbl_id == table_id); 
        temp_results[key] = it->second.data;
    }
    
    // Then scan table, we do not lock the range or the read keys, 
    // so we are "read committed" i.e. allow phantom
    for (auto it = table->data.lower_bound(key_start);
            it != table->data.end() && it->first < key_end;
            ++it) {
        std::string table_key = make_table_key(table_id, it->first);
        // Skip if deleted
        if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
            continue;
        }
        // Skip if already in write set
        if (temp_results.find(it->first) != temp_results.end()) {
            continue;
        }
        temp_results[it->first] = it->second.data;
    }
    
    // Convert to results vector
    KVTError r = KVTError::SUCCESS;
    for (const auto& [key, value] : temp_results) {
        results.emplace_back(key, value);
        if (results.size() >= num_item_limit) {
            r = KVTError::SCAN_LIMIT_REACHED;
            break;
        }
    }
    
    return r;
}

//==================================KVT KVTMemManagerOCC ===========================================================

KVTError KVTMemManagerOCC::commit_transaction(uint64_t tx_id, std::string& error_msg) 
{
    std::lock_guard<std::mutex> lock(global_mutex);
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    
    //first check if the readset versions are still valid
    for (const auto& read_pair : tx->read_set) {
        auto [table_id_parsed, key] = parse_table_key(read_pair.first);
        Table* table = get_table_by_id(table_id_parsed);
        assert(table);

        uint64_t local_version = read_pair.second.metadata;

        if (table->data.find(key) == table->data.end() || //being deleted by another transaction
            table->data[key].metadata > local_version) {  //being written by another transaction
            error_msg = "Transaction " + std::to_string(tx_id) + " has stale data";
            transactions.erase(tx_id);
            return KVTError::TRANSACTION_HAS_STALE_DATA;
        }
    }
    //now all readset versions are valid, so we can install the new values
    for (const auto& delete_item : tx->delete_set) {
        auto [table_id_parsed, key] = parse_table_key(delete_item);
        Table* table = get_table_by_id(table_id_parsed);
        assert(table);
        auto itr = table->data.find(key);
        if (itr != table->data.end()) //not found could happen, if the key is first written then delete and never hit the table.
            table->data.erase(itr);
    }
    for (const auto& write_pair : tx->write_set) {
        auto [table_id_parsed, key] = parse_table_key(write_pair.first);
        Table* table = get_table_by_id(table_id_parsed);
        assert(table);
        
        std::string old_value = (table->data.find(key) != table->data.end()) ? table->data[key].data : "NEW";
        uint64_t new_version = (table->data.find(key) != table->data.end()) ? table->data[key].metadata + 1 : 1;
        table->data[key] = Entry(write_pair.second.data, static_cast<int32_t>(new_version));
        
    }
    
    transactions.erase(tx_id);
    return KVTError::SUCCESS;
}
        
KVTError KVTMemManagerOCC::rollback_transaction(uint64_t tx_id, std::string& error_msg) 
{
    std::lock_guard<std::mutex> lock(global_mutex);
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    transactions.erase(tx_id);
    return KVTError::SUCCESS;
}

KVTError KVTMemManagerOCC::get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
        std::string& value, std::string& error_msg) 
{
    std::lock_guard<std::mutex> lock(global_mutex);
    //one shot transaction is only allowed for read only transaction.
    if (tx_id == 0) {
        Table* table = get_table_by_id(table_id);
        if (!table) {
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }
        auto it = table->data.find(key);
        if (it == table->data.end()) {
            error_msg = "Key " + key + " not found";
            return KVTError::KEY_NOT_FOUND;
        }
        value = it->second.data;
        return KVTError::SUCCESS;
    }
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    std::string table_key = make_table_key(table_id, key);
    if (tx->write_set.find(table_key) != tx->write_set.end()) {
        value = tx->write_set[table_key].data;
        return KVTError::SUCCESS;
    }
    if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
        error_msg = "Key " + key + " is deleted";
        return KVTError::KEY_IS_DELETED;
    }
    if (tx->read_set.find(table_key) != tx->read_set.end()) {
        value = tx->read_set[table_key].data;
        return KVTError::SUCCESS;
    }
    Table* table = get_table_by_id(table_id);
    if (!table) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    if (table->data.find(key) == table->data.end()) {
        error_msg = "Key " + key + " not found";
        return KVTError::KEY_NOT_FOUND;
    }
    tx->read_set[table_key] = table->data[key];
    value = table->data[key].data;
    return KVTError::SUCCESS;
}

  KVTError KVTMemManagerOCC::set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
           const std::string& value, std::string& error_msg) 
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        if (tx_id == 0) {
            Table* table = get_table_by_id(table_id);
            if (!table) {
                error_msg = "Table with ID " + std::to_string(table_id) + " not found";
                return KVTError::TABLE_NOT_FOUND;
            }
            if (table->data.find(key) == table->data.end()) {
                table->data[key] = Entry(value,1);
            }
            else {
                table->data[key] = Entry(value,table->data[key].metadata + 1);
            }
            return KVTError::SUCCESS;
        }
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return KVTError::TRANSACTION_NOT_FOUND;
        }
        std::string table_key = make_table_key(table_id, key);
        tx->write_set[table_key] = Entry(value, 0); //no need to track metadata for write set
        auto itr = tx->delete_set.find(table_key);
        if (itr != tx->delete_set.end()) {
            tx->delete_set.erase(itr); //this is opitonal, as we install deletes first then writes
        }
        return KVTError::SUCCESS;
    }

KVTError KVTMemManagerOCC::del(uint64_t tx_id, uint64_t table_id, const std::string& key, std::string& error_msg) 
{
    std::lock_guard<std::mutex> lock(global_mutex);
    if (tx_id == 0) {
        Table* table = get_table_by_id(table_id);
        if (!table) {
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }
        if (table->data.find(key) == table->data.end()) {
            error_msg = "Key " + key + " not found";
            return KVTError::KEY_NOT_FOUND;
        }
        table->data.erase(key); 
        return KVTError::SUCCESS;
    }
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    std::string table_key = make_table_key(table_id, key);
    auto itr = tx->write_set.find(table_key);
    if (itr != tx->write_set.end()) {
        tx->write_set.erase(itr); //delete after write, so not necessarily read from table. 
    }
    else {
        if (tx->read_set.find(table_key) == tx->read_set.end()) { //not in the read set, so need to read from table.
            Table* table = get_table_by_id(table_id);
            if (!table) {
                error_msg = "Table with ID " + std::to_string(table_id) + " not found";
                return KVTError::TABLE_NOT_FOUND;
            }
            if (table->data.find(key) == table->data.end()) {
                error_msg = "Key " + key + " not found, cannot be deleted";
                return KVTError::KEY_NOT_FOUND;
            }
            tx->read_set[table_key] = table->data[key];
        }
    }
    tx->delete_set.insert(table_key);
    return KVTError::SUCCESS;
}


KVTError KVTMemManagerOCC::scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
        const std::string& key_end, size_t num_item_limit, 
        std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) 
{
    std::lock_guard<std::mutex> lock(global_mutex);
    Table* table = get_table_by_id(table_id);
    if (!table) {
        error_msg = "Table with ID " + std::to_string(table_id) + " not found";
        return KVTError::TABLE_NOT_FOUND;
    }
    if (tx_id == 0) {
        for (auto itr = table->data.lower_bound(key_start); itr != table->data.end() && itr->first < key_end; ++itr) {
            results.emplace_back(itr->first, itr->second.data);
            if (results.size() >= num_item_limit) {
                break;
            }
        }
        return KVTError::SUCCESS;
    }
    Transaction* tx = get_transaction(tx_id);
    if (!tx) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return KVTError::TRANSACTION_NOT_FOUND;
    }
    std::map<std::string, std::string> results_writes;
    {
        std::string table_key_start = make_table_key(table_id, key_start);
        std::string table_key_end = make_table_key(table_id, key_end);
        //first put all write_set into results
        for (auto itr = tx->write_set.lower_bound(table_key_start); itr != tx->write_set.end() && itr->first < table_key_end; ++itr) {
            auto [table_id_parsed, key] = parse_table_key(itr->first);
            results_writes[key] = itr->second.data;
            if (results_writes.size() >= num_item_limit) {
                break;
            }
        }
    }
    std::map<std::string, std::string> results_table;
    //now collect from table, put into read_set if necessary
    for (auto itr = table->data.lower_bound(key_start); itr != table->data.end() && itr->first < key_end; ++itr) {
        if (results_writes.find(itr->first) != results_writes.end()) //already in write set, skip
            continue;
        std::string table_key = make_table_key(table_id, itr->first);
        if (tx->delete_set.find(table_key) != tx->delete_set.end()) //being deleted, skip
            continue;
        if (tx->read_set.find(table_key) == tx->read_set.end()) { //not in the read set, so need to read from table.
            tx->read_set[table_key] = itr->second;
            results_table[itr->first] = itr->second.data;
        } else {
            if (tx->read_set[table_key].data != itr->second.data) {
                //if my value is not the same as the table, table value is newer, so my version must be older
                //actually we should abort here, but we just assert for now. since we are not supposed to
                //read from table in real implementations. 
                assert (tx->read_set[table_key].metadata < itr->second.metadata);
            }
            results_table[itr->first] = tx->read_set[table_key].data; //should be the same, if not, then we will abort anyway.
        }
        if (results_table.size() >= num_item_limit) {
            break;
        }
    }
    //now merge the results
    KVTError r = KVTError::SUCCESS;
    results_table.insert(results_writes.begin(), results_writes.end());
    for (const auto& scan_pair : results_table) {
        results.emplace_back(scan_pair);
        if (results.size() >= num_item_limit) {
            r = KVTError::SCAN_LIMIT_REACHED;
            break;
        }
    }
    return r;
}
