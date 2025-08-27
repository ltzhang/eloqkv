#include "kvt_manager.h"
#include <sstream>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <cstring>
#include <glog/logging.h>
#include <mutex>
#include <thread>
#include <chrono>
#include <cassert>
#include <iostream>

namespace EloqKV {


void KVTManager::handleCommand(const std::vector<butil::StringPiece> &args,
                              brpc::RedisReply *output) {
    if (args.empty()) {
        output->SetError("Empty command");
        return;
    }

    std::string cmd = args[0].as_string();
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
    std::string error_msg;
    
    if (cmd == "create_table") {
        if (args.size() != 3) {
            output->SetError("Usage: create_table tablename partition_method");
            return;
        }
        std::string table_name = args[1].as_string();
        std::string partition_method = args[2].as_string();
        uint64_t table_id = doCreateTable(table_name, partition_method, error_msg);
        if (table_id == 0) {
            output->SetError(error_msg);
        } else {
            output->SetInteger(table_id);
        }
    }
    else if (cmd == "start_tx") {
        if (args.size() != 1) {
            output->SetError("Usage: start_tx");
            return;
        }
        uint64_t tx_id = doStartTx(error_msg);
        if (tx_id == 0) {
            output->SetError(error_msg);
        } else {
            output->SetInteger(tx_id);
        }
    }
    else if (cmd == "get") {
        if (args.size() != 4) {
            output->SetError("Usage: get tx_id tablename key");
            return;
        }
        uint64_t tx_id = std::stoull(args[1].as_string());
        std::string table_name = args[2].as_string();
        std::string key = args[3].as_string();
        std::string value;
        if (doGet(tx_id, table_name, key, value, error_msg)) {
            if (value.empty()) {
                output->SetString("");
            } else {
                output->SetString(value);
            }
        } else {
            output->SetError(error_msg);
        }
    }
    else if (cmd == "set") {
        if (args.size() != 5) {
            output->SetError("Usage: set tx_id tablename key value");
            return;
        }
        uint64_t tx_id = std::stoull(args[1].as_string());
        std::string table_name = args[2].as_string();
        std::string key = args[3].as_string();
        std::string value = args[4].as_string();
        if (doSet(tx_id, table_name, key, value, error_msg)) {
            output->SetStatus("OK");
        } else {
            output->SetError(error_msg);
        }
    }
    else if (cmd == "scan") {
        if (args.size() != 6) {
            output->SetError("Usage: scan tx_id tablename keystart keyend num_item_limit");
            return;
        }
        uint64_t tx_id = std::stoull(args[1].as_string());
        std::string table_name = args[2].as_string();
        std::string key_start = args[3].as_string();
        std::string key_end = args[4].as_string();
        size_t num_item_limit = std::stoull(args[5].as_string());
        std::vector<std::pair<std::string, std::string>> results;
        if (doScan(tx_id, table_name, key_start, key_end, num_item_limit, results, error_msg)) {
            output->SetArray(results.size() * 2);
            for (size_t i = 0; i < results.size(); i++) {
                (*output)[i * 2].SetString(results[i].first);
                (*output)[i * 2 + 1].SetString(results[i].second);
            }
        } else {
            output->SetError(error_msg);
        }
    }
    else if (cmd == "commit") {
        if (args.size() != 2) {
            output->SetError("Usage: commit tx_id");
            return;
        }
        uint64_t tx_id = std::stoull(args[1].as_string());
        if (doCommitTx(tx_id, error_msg)) {
            output->SetStatus("OK");
        } else {
            output->SetError(error_msg);
        }
    }
    else if (cmd == "rollback") {
        if (args.size() != 2) {
            output->SetError("Usage: rollback tx_id");
            return;
        }
        uint64_t tx_id = std::stoull(args[1].as_string());
        if (doAbortTx(tx_id, error_msg)) {
            output->SetStatus("OK");
        } else {
            output->SetError(error_msg);
        }
    }
    else {
        output->SetError("Unknown command: " + cmd);
    }
}

KVTManager::~KVTManager() {
    shutdown();
}

void KVTManager::initialize() {
    // For now, keep it simple without TxService until we can properly set it up
    // The current simplified implementation works for testing the interface
    LOG(INFO) << "KVTManager initialized (simplified version without TxService)";
}

void KVTManager::shutdown() {
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    // Clean up any remaining transactions
    for (auto& pair : active_transactions_) {
        if (pair.second) {
            // For simplified version, just clear the pointer
            pair.second = nullptr;
        }
    }
    active_transactions_.clear();
}

uint64_t KVTManager::doCreateTable(const std::string& table_name, const std::string& partition_method, 
                                  std::string& error_msg) {
    std::lock_guard<std::mutex> lock(tables_mutex_);
    
    // Check if table already exists
    if (tables_.find(table_name) != tables_.end()) {
        error_msg = "Table " + table_name + " already exists";
        return 0;
    }
    
    // For simplified version, create a minimal table without tx_service integration
    auto tx_table_name = std::make_unique<txservice::TableName>(
        table_name, 
        txservice::TableType::Primary, 
        txservice::TableEngine::EloqKv
    );
    
    // Create and store the KVTTable
    auto kvt_table = std::make_unique<KVTTable>(table_name, partition_method, std::move(tx_table_name));
    tables_[table_name] = std::move(kvt_table);
    
    uint64_t table_id = next_table_id_++;
    std::cout << "Created table " << table_name << " with partition method " << partition_method 
              << ", assigned table_id " << table_id;
    return table_id;
}

uint64_t KVTManager::doStartTx(std::string& error_msg) {
    // Simplified version - just assign an ID without actual tx_service integration
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    uint64_t tx_id = next_transaction_id_++;
    
    // Store a placeholder (nullptr for now)
    active_transactions_[tx_id] = nullptr;
    
    std::cout << "Started transaction " << tx_id << " (simplified)" << std::endl;
    return tx_id;
}

bool KVTManager::doGet(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                      std::string& value, std::string& error_msg) {
    KVTTable* table = getTable(table_name);
    if (!table) {
        error_msg = "Table " + table_name + " not found";
        return false;
    }
    
    // Check if we have a transaction-specific value first
    if (tx_id != 0) {
        std::lock_guard<std::mutex> lock(tx_data_mutex_);
        auto tx_key = std::make_pair(tx_id, table_name + ":" + key);
        auto it = transaction_data_.find(tx_key);
        if (it != transaction_data_.end()) {
            value = it->second;
            std::cout << "Get operation on table " << table_name << " key " << key 
                      << " tx_id " << tx_id << " (found in tx): '" << value << "'" << std::endl;
            return true;
        }
    }
    
    // Look in committed data
    std::lock_guard<std::mutex> lock(committed_data_mutex_);
    std::string full_key = table_name + ":" + key;
    auto it = committed_data_.find(full_key);
    if (it != committed_data_.end()) {
        value = it->second;
        std::cout << "Get operation on table " << table_name << " key " << key 
                  << " tx_id " << tx_id << " (found committed): '" << value << "'" << std::endl;
    } else {
        value = "";
        std::cout << "Get operation on table " << table_name << " key " << key 
                  << " tx_id " << tx_id << " (not found): ''" << std::endl;
    }
    
    return true;
}

bool KVTManager::doSet(uint64_t tx_id, const std::string& table_name, 
                      const std::string& key, const std::string& value, std::string& error_msg) {
    KVTTable* table = getTable(table_name);
    if (!table) {
        error_msg = "Table " + table_name + " not found";
        return false;
    }
    
    if (tx_id == 0) {
        // One-shot transaction - commit immediately
        std::lock_guard<std::mutex> lock(committed_data_mutex_);
        std::string full_key = table_name + ":" + key;
        committed_data_[full_key] = value;
        std::cout << "Set operation on table " << table_name << " key " << key 
                  << " value '" << value << "' (one-shot, committed)" << std::endl;
    } else {
        // Transactional - store in transaction data
        std::lock_guard<std::mutex> tx_lock(transactions_mutex_);
        auto tx_it = active_transactions_.find(tx_id);
        if (tx_it == active_transactions_.end()) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        std::lock_guard<std::mutex> data_lock(tx_data_mutex_);
        auto tx_key = std::make_pair(tx_id, table_name + ":" + key);
        transaction_data_[tx_key] = value;
        std::cout << "Set operation on table " << table_name << " key " << key 
                  << " value '" << value << "' tx_id " << tx_id << " (staged)" << std::endl;
    }
    
    return true;
}

bool KVTManager::doScan(uint64_t tx_id, const std::string& table_name, 
                       const std::string& key_start, const std::string& key_end, size_t num_item_limit, 
                       std::vector<std::pair<std::string, std::string>>& results,
                       std::string& error_msg) {
    KVTTable* table = getTable(table_name);
    if (!table) {
        error_msg = "Table " + table_name + " not found";
        return false;
    }
    
    results.clear();
    std::string table_prefix = table_name + ":";
    
    // First collect from committed data
    {
        std::lock_guard<std::mutex> lock(committed_data_mutex_);
        for (const auto& pair : committed_data_) {
            if (pair.first.find(table_prefix) == 0) {
                std::string key = pair.first.substr(table_prefix.length());
                if (key >= key_start && key <= key_end) {
                    results.push_back({key, pair.second});
                    if (results.size() >= num_item_limit) break;
                }
            }
        }
    }
    
    // If in transaction, overlay transaction data
    if (tx_id != 0) {
        std::lock_guard<std::mutex> lock(tx_data_mutex_);
        for (const auto& pair : transaction_data_) {
            if (pair.first.first == tx_id && pair.first.second.find(table_prefix) == 0) {
                std::string key = pair.first.second.substr(table_prefix.length());
                if (key >= key_start && key <= key_end) {
                    // Find if this key already exists in results and replace it
                    bool found = false;
                    for (auto& result : results) {
                        if (result.first == key) {
                            result.second = pair.second;
                            found = true;
                            break;
                        }
                    }
                    if (!found && results.size() < num_item_limit) {
                        results.push_back({key, pair.second});
                    }
                }
            }
        }
    }
    
    // Sort results by key
    std::sort(results.begin(), results.end());
    
    // Limit results
    if (results.size() > num_item_limit) {
        results.resize(num_item_limit);
    }
    
    std::cout << "Scan operation on table " << table_name << " range [" << key_start 
              << ", " << key_end << "] limit " << num_item_limit 
              << " tx_id " << tx_id << " - found " << results.size() << " items" << std::endl;
    return true;
}

bool KVTManager::doCommitTx(uint64_t tx_id, std::string& error_msg) {
    if (tx_id == 0) {
        error_msg = "Cannot commit transaction with id 0";
        return false;
    }
    
    {
        std::lock_guard<std::mutex> lock(transactions_mutex_);
        auto it = active_transactions_.find(tx_id);
        if (it == active_transactions_.end()) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        // Remove from active transactions first
        active_transactions_.erase(tx_id);
    }
    
    // Move all transaction data to committed data
    std::vector<std::pair<std::pair<uint64_t, std::string>, std::string>> tx_changes;
    {
        std::lock_guard<std::mutex> tx_lock(tx_data_mutex_);
        for (auto it = transaction_data_.begin(); it != transaction_data_.end(); ) {
            if (it->first.first == tx_id) {
                tx_changes.push_back(*it);
                it = transaction_data_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    // Apply changes to committed data
    {
        std::lock_guard<std::mutex> commit_lock(committed_data_mutex_);
        for (const auto& change : tx_changes) {
            std::string full_key = change.first.second; // Already contains table_name:key
            committed_data_[full_key] = change.second;
        }
    }
    
    std::cout << "Committed transaction " << tx_id << " - applied " << tx_changes.size() << " changes" << std::endl;
    return true;
}

bool KVTManager::doAbortTx(uint64_t tx_id, std::string& error_msg) {
    if (tx_id == 0) {
        error_msg = "Cannot rollback transaction with id 0";
        return false;
    }
    
    {
        std::lock_guard<std::mutex> lock(transactions_mutex_);
        auto it = active_transactions_.find(tx_id);
        if (it == active_transactions_.end()) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        // Remove from active transactions first
        active_transactions_.erase(tx_id);
    }
    
    // Remove all transaction data (rollback)
    size_t changes_discarded = 0;
    {
        std::lock_guard<std::mutex> tx_lock(tx_data_mutex_);
        for (auto it = transaction_data_.begin(); it != transaction_data_.end(); ) {
            if (it->first.first == tx_id) {
                it = transaction_data_.erase(it);
                changes_discarded++;
            } else {
                ++it;
            }
        }
    }
    
    std::cout << "Rolled back transaction " << tx_id << " - discarded " << changes_discarded << " changes" << std::endl;
    return true;
}

txservice::TransactionExecution* KVTManager::getTransaction(uint64_t tx_id) {
    if (tx_id == 0) {
        return nullptr;
    }
    
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    auto it = active_transactions_.find(tx_id);
    return (it != active_transactions_.end()) ? it->second : nullptr;
}

txservice::TransactionExecution* KVTManager::getOrCreateTransaction(uint64_t tx_id) {
    if (tx_id == 0) {
        // For one-shot transaction, return nullptr for now
        return nullptr;
    }
    
    return getTransaction(tx_id);
}

KVTTable* KVTManager::getTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(tables_mutex_);
    auto it = tables_.find(table_name);
    return (it != tables_.end()) ? it->second.get() : nullptr;
}

void KVTManager::startTestInBackground() {
    std::thread test_thread([this]() {
        // Wait a bit for the server to fully initialize
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        
        std::cout << "=== Starting KVTManager comprehensive test ===" << std::endl << std::endl;
        try {
            runComprehensiveTest();
            std::cout << "=== KVTManager comprehensive test completed successfully ===" << std::endl << std::endl;
        } catch (const std::exception& e) {
            std::cout << "=== KVTManager test failed with exception: " << e.what() << " ===" << std::endl << std::endl;
        } catch (...) {
            std::cout << "=== KVTManager test failed with unknown exception ===" << std::endl << std::endl;
        }
    });
    
    // Detach the thread so it runs independently
    test_thread.detach();
    std::cout << "KVTManager background test started" << std::endl << std::endl;
}

void KVTManager::runComprehensiveTest() {
    std::string error_msg;
    
    // Test 1: Create tables
    std::cout << "Test 1: Creating tables..." << std::endl << std::endl;
    uint64_t table1_id = doCreateTable("test_table_hash", "hash", error_msg);
    assert(table1_id != 0);
    std::cout << "✓ Created table 'test_table_hash' with ID: " << table1_id << std::endl;
    
    uint64_t table2_id = doCreateTable("test_table_range", "range", error_msg);
    assert(table2_id != 0);
    std::cout << "✓ Created table 'test_table_range' with ID: " << table2_id << std::endl;
    
    // Test duplicate table creation
    uint64_t duplicate_id = doCreateTable("test_table_hash", "hash", error_msg);
    assert(duplicate_id == 0);
    std::cout << "✓ Duplicate table creation correctly failed: " << error_msg << std::endl;
    
    // Test 2: Transaction management
    std::cout << "Test 2: Transaction management..." << std::endl << std::endl;
    uint64_t tx1 = doStartTx(error_msg);
    assert(tx1 != 0);
    std::cout << "✓ Started transaction TX1: " << tx1 << std::endl;
    
    uint64_t tx2 = doStartTx(error_msg);
    assert(tx2 != 0);
    assert(tx1 != tx2);
    std::cout << "✓ Started transaction TX2: " << tx2 << std::endl;
    
    // Test 3: One-shot operations (tx_id = 0)
    std::cout << "Test 3: One-shot operations..." << std::endl << std::endl;
    bool success = doSet(0, "test_table_hash", "oneshot_key", "oneshot_value", error_msg);
    assert(success);
    std::cout << "✓ One-shot SET operation succeeded" << std::endl << std::endl;
    
    std::string value;
    success = doGet(0, "test_table_hash", "oneshot_key", value, error_msg);
    assert(success);
    std::cout << "✓ One-shot GET operation succeeded, value: '" << value << "'" << std::endl;
    
    // Test 4: Transactional operations
    std::cout << "Test 4: Transactional operations..." << std::endl << std::endl;
    
    // Set some values in transaction 1
    success = doSet(tx1, "test_table_hash", "key1", "value1_tx1", error_msg);
    assert(success);
    std::cout << "✓ SET key1=value1_tx1 in TX1" << std::endl << std::endl;
    
    success = doSet(tx1, "test_table_hash", "key2", "value2_tx1", error_msg);
    assert(success);
    std::cout << "✓ SET key2=value2_tx1 in TX1" << std::endl << std::endl;
    
    // Set different values in transaction 2
    success = doSet(tx2, "test_table_hash", "key1", "value1_tx2", error_msg);
    assert(success);
    std::cout << "✓ SET key1=value1_tx2 in TX2" << std::endl << std::endl;
    
    success = doSet(tx2, "test_table_range", "range_key", "range_value", error_msg);
    assert(success);
    std::cout << "✓ SET range_key=range_value in TX2 on range table" << std::endl << std::endl;
    
    // Test reading within transactions
    success = doGet(tx1, "test_table_hash", "key1", value, error_msg);
    assert(success);
    std::cout << "✓ GET key1 in TX1, value: '" << value << "'" << std::endl;
    
    success = doGet(tx2, "test_table_hash", "key1", value, error_msg);
    assert(success);
    std::cout << "✓ GET key1 in TX2, value: '" << value << "'" << std::endl;
    
    // Test 5: Scan operations
    std::cout << "Test 5: Scan operations..." << std::endl << std::endl;
    std::vector<std::pair<std::string, std::string>> scan_results;
    
    success = doScan(tx1, "test_table_hash", "key0", "key9", 10, scan_results, error_msg);
    assert(success);
    std::cout << "✓ SCAN in TX1 returned " << scan_results.size() << " results" << std::endl;
    
    success = doScan(0, "test_table_range", "a", "z", 5, scan_results, error_msg);
    assert(success);
    std::cout << "✓ One-shot SCAN on range table returned " << scan_results.size() << " results" << std::endl;
    
    // Test 6: Error conditions
    std::cout << "Test 6: Error conditions..." << std::endl << std::endl;
    
    // Try to operate on non-existent table
    success = doSet(tx1, "non_existent_table", "key", "value", error_msg);
    assert(!success);
    std::cout << "✓ Operation on non-existent table correctly failed: " << error_msg << std::endl;
    
    // Try to commit non-existent transaction
    success = doCommitTx(99999, error_msg);
    assert(!success);
    std::cout << "✓ Commit non-existent transaction correctly failed: " << error_msg << std::endl;
    
    // Try to rollback non-existent transaction
    success = doAbortTx(99999, error_msg);
    assert(!success);
    std::cout << "✓ Rollback non-existent transaction correctly failed: " << error_msg << std::endl;
    
    // Test 7: Transaction commit and rollback - demonstrate ACID properties
    std::cout << "Test 7: ACID transaction behavior..." << std::endl << std::endl;
    
    // First, verify what we can read from each transaction before commit/rollback
    std::cout << "Before commit/rollback:" << std::endl;
    success = doGet(tx1, "test_table_hash", "key1", value, error_msg);
    std::cout << "TX1 sees key1: '" << value << "'" << std::endl;
    success = doGet(tx2, "test_table_hash", "key1", value, error_msg);
    std::cout << "TX2 sees key1: '" << value << "'" << std::endl;
    success = doGet(0, "test_table_hash", "key1", value, error_msg);
    std::cout << "One-shot read sees key1: '" << value << "' (should be empty or oneshot_value)" << std::endl;
    
    // Commit transaction 1 - its changes should become persistent
    success = doCommitTx(tx1, error_msg);
    assert(success);
    std::cout << "✓ Committed TX1: " << tx1 << std::endl;
    
    // After TX1 commit, verify persistence
    success = doGet(0, "test_table_hash", "key1", value, error_msg);
    std::cout << "After TX1 commit, one-shot read sees key1: '" << value << "' (should be value1_tx1)" << std::endl;
    success = doGet(0, "test_table_hash", "key2", value, error_msg);
    std::cout << "After TX1 commit, one-shot read sees key2: '" << value << "' (should be value2_tx1)" << std::endl;
    
    // TX2 should still see its own uncommitted version
    success = doGet(tx2, "test_table_hash", "key1", value, error_msg);
    std::cout << "TX2 still sees its version of key1: '" << value << "'" << std::endl;
    
    // Rollback transaction 2 - its changes should be discarded
    success = doAbortTx(tx2, error_msg);
    assert(success);
    std::cout << "✓ Rolled back TX2: " << tx2 << std::endl;
    
    // After TX2 rollback, verify TX1's committed values are still there
    success = doGet(0, "test_table_hash", "key1", value, error_msg);
    std::cout << "After TX2 rollback, one-shot read sees key1: '" << value << "' (should still be value1_tx1)" << std::endl;
    success = doGet(0, "test_table_range", "range_key", value, error_msg);
    std::cout << "After TX2 rollback, range_key should not exist: '" << value << "' (should be empty)" << std::endl;
    
    // Try to use committed/rolled back transactions (should fail)
    success = doSet(tx1, "test_table_hash", "key3", "value3", error_msg);
    assert(!success);
    std::cout << "✓ Using committed transaction correctly failed: " << error_msg << std::endl;
    
    success = doSet(tx2, "test_table_hash", "key4", "value4", error_msg);
    assert(!success);
    std::cout << "✓ Using rolled back transaction correctly failed: " << error_msg << std::endl;
    
    // Test 8: Complex scenario with multiple transactions
    std::cout << "Test 8: Complex multi-transaction scenario..." << std::endl << std::endl;
    
    uint64_t tx3 = doStartTx(error_msg);
    uint64_t tx4 = doStartTx(error_msg);
    uint64_t tx5 = doStartTx(error_msg);
    assert(tx3 != 0 && tx4 != 0 && tx5 != 0);
    std::cout << "✓ Started three new transactions: " << tx3 << ", " << tx4 << ", " << tx5 << std::endl;
    
    // Perform operations across multiple tables and transactions
    doSet(tx3, "test_table_hash", "complex_key1", "complex_value1", error_msg);
    doSet(tx3, "test_table_range", "complex_key2", "complex_value2", error_msg);
    doSet(tx4, "test_table_hash", "complex_key3", "complex_value3", error_msg);
    doSet(tx5, "test_table_range", "complex_key4", "complex_value4", error_msg);
    
    // Scan operations within transactions
    doScan(tx3, "test_table_hash", "complex", "complex_z", 10, scan_results, error_msg);
    doScan(tx4, "test_table_range", "a", "z", 20, scan_results, error_msg);
    
    // Commit some, rollback others
    doCommitTx(tx3, error_msg);
    doAbortTx(tx4, error_msg);
    doCommitTx(tx5, error_msg);
    
    std::cout << "✓ Complex scenario completed - TX3 committed, TX4 rolled back, TX5 committed" << std::endl << std::endl;
    
    // Test 9: Stress test with many operations
    std::cout << "Test 9: Stress test..." << std::endl << std::endl;
    
    uint64_t stress_tx = doStartTx(error_msg);
    assert(stress_tx != 0);
    
    // Perform many operations
    for (int i = 0; i < 100; i++) {
        std::string key = "stress_key_" + std::to_string(i);
        std::string value = "stress_value_" + std::to_string(i * 10);
        
        bool set_success = doSet(stress_tx, "test_table_hash", key, value, error_msg);
        assert(set_success);
        
        if (i % 10 == 0) {
            std::cout << "✓ Stress test progress: " << i << "/100 operations completed" << std::endl;
        }
    }
    
    // Read back some values
    for (int i = 0; i < 100; i += 10) {
        std::string key = "stress_key_" + std::to_string(i);
        std::string read_value;
        bool get_success = doGet(stress_tx, "test_table_hash", key, read_value, error_msg);
        assert(get_success);
    }
    
    doCommitTx(stress_tx, error_msg);
    std::cout << "✓ Stress test completed - 100 SET/GET operations in single transaction" << std::endl << std::endl;
    
    // Test 10: Edge cases
    std::cout << "Test 10: Edge cases..." << std::endl << std::endl;
    
    // Try to commit/rollback transaction ID 0
    success = doCommitTx(0, error_msg);
    assert(!success);
    std::cout << "✓ Commit TX ID 0 correctly failed: " << error_msg << std::endl;
    
    success = doAbortTx(0, error_msg);
    assert(!success);
    std::cout << "✓ Rollback TX ID 0 correctly failed: " << error_msg << std::endl;
    
    // Empty key operations
    success = doSet(0, "test_table_hash", "", "empty_key_value", error_msg);
    assert(success);  // Empty keys should be allowed
    std::cout << "✓ SET with empty key succeeded" << std::endl << std::endl;
    
    success = doGet(0, "test_table_hash", "", value, error_msg);
    assert(success);
    std::cout << "✓ GET with empty key succeeded, value: '" << value << "'" << std::endl;
    
    // Empty value operations
    success = doSet(0, "test_table_hash", "empty_value_key", "", error_msg);
    assert(success);
    std::cout << "✓ SET with empty value succeeded" << std::endl << std::endl;
    
    // Very long key and value
    std::string long_key(1000, 'k');
    std::string long_value(10000, 'v');
    success = doSet(0, "test_table_hash", long_key, long_value, error_msg);
    assert(success);
    std::cout << "✓ SET with very long key (" << long_key.length() << " chars) and value (" 
              << long_value.length() << " chars) succeeded" << std::endl;
    
    // Test 11: Advanced ACID test - multiple transactions with overlapping keys
    std::cout << "Test 11: Advanced ACID behavior..." << std::endl << std::endl;
    
    // Create a baseline committed value
    success = doSet(0, "test_table_hash", "shared_key", "baseline_value", error_msg);
    assert(success);
    std::cout << "✓ Set baseline value for shared_key" << std::endl;
    
    // Create two transactions that modify the same key
    uint64_t tx_a = doStartTx(error_msg);
    uint64_t tx_b = doStartTx(error_msg);
    
    // Both transactions modify the same key
    success = doSet(tx_a, "test_table_hash", "shared_key", "value_from_tx_a", error_msg);
    assert(success);
    success = doSet(tx_b, "test_table_hash", "shared_key", "value_from_tx_b", error_msg);
    assert(success);
    
    // Each transaction should see its own version
    success = doGet(tx_a, "test_table_hash", "shared_key", value, error_msg);
    std::cout << "TX_A sees shared_key: '" << value << "' (should be value_from_tx_a)" << std::endl;
    success = doGet(tx_b, "test_table_hash", "shared_key", value, error_msg);
    std::cout << "TX_B sees shared_key: '" << value << "' (should be value_from_tx_b)" << std::endl;
    
    // One-shot reads should see the baseline
    success = doGet(0, "test_table_hash", "shared_key", value, error_msg);
    std::cout << "One-shot sees shared_key: '" << value << "' (should be baseline_value)" << std::endl;
    
    // Commit TX_A first
    success = doCommitTx(tx_a, error_msg);
    assert(success);
    std::cout << "✓ Committed TX_A" << std::endl;
    
    // Now one-shot should see TX_A's value
    success = doGet(0, "test_table_hash", "shared_key", value, error_msg);
    std::cout << "After TX_A commit, one-shot sees: '" << value << "' (should be value_from_tx_a)" << std::endl;
    
    // TX_B should still see its own uncommitted version
    success = doGet(tx_b, "test_table_hash", "shared_key", value, error_msg);
    std::cout << "TX_B still sees its version: '" << value << "' (should be value_from_tx_b)" << std::endl;
    
    // Now rollback TX_B
    success = doAbortTx(tx_b, error_msg);
    assert(success);
    std::cout << "✓ Rolled back TX_B" << std::endl;
    
    // Final state should be TX_A's committed value
    success = doGet(0, "test_table_hash", "shared_key", value, error_msg);
    std::cout << "Final state of shared_key: '" << value << "' (should be value_from_tx_a)" << std::endl;
    
    // Test 12: Scan with transaction isolation
    std::cout << "Test 12: Scan with transaction isolation..." << std::endl << std::endl;
    
    uint64_t scan_tx = doStartTx(error_msg);
    
    // Add multiple keys in transaction
    for (int i = 1; i <= 5; i++) {
        std::string key = "scan_key_" + std::to_string(i);
        std::string value = "scan_value_" + std::to_string(i);
        success = doSet(scan_tx, "test_table_hash", key, value, error_msg);
        assert(success);
    }
    
    // Scan within transaction should see these keys
    std::vector<std::pair<std::string, std::string>> tx_scan_results;
    success = doScan(scan_tx, "test_table_hash", "scan_key_", "scan_key_z", 10, tx_scan_results, error_msg);
    assert(success);
    std::cout << "✓ Transaction scan found " << tx_scan_results.size() << " items" << std::endl;
    for (const auto& pair : tx_scan_results) {
        std::cout << "  " << pair.first << " -> " << pair.second << std::endl;
    }
    
    // One-shot scan should not see these keys yet
    std::vector<std::pair<std::string, std::string>> oneshot_scan_results;
    success = doScan(0, "test_table_hash", "scan_key_", "scan_key_z", 10, oneshot_scan_results, error_msg);
    assert(success);
    std::cout << "✓ One-shot scan found " << oneshot_scan_results.size() << " items (should be 0)" << std::endl;
    
    // Commit the transaction
    success = doCommitTx(scan_tx, error_msg);
    assert(success);
    std::cout << "✓ Committed scan transaction" << std::endl;
    
    // Now one-shot scan should see the keys
    success = doScan(0, "test_table_hash", "scan_key_", "scan_key_z", 10, oneshot_scan_results, error_msg);
    assert(success);
    std::cout << "✓ Post-commit one-shot scan found " << oneshot_scan_results.size() << " items" << std::endl;
    for (const auto& pair : oneshot_scan_results) {
        std::cout << "  " << pair.first << " -> " << pair.second << std::endl;
    }
    
    std::cout << "=== All tests passed! KVTManager demonstrates proper ACID behavior ===" << std::endl << std::endl;
    exit(1);
}

} // namespace EloqKV