#include "kvt_manager.h"
#include "tx_service/include/tx_record.h"
#include "tx_service/include/tx_util.h"
#include "eloq_key.h"
#include "redis_object.h"
#include "redis_command.h"
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
#include <unordered_map>

namespace EloqKV {

// NOTE: KVTCommand implementations removed for prototype simplicity
// In a full implementation, these would be complete TxCommand classes


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

void KVTManager::initialize(txservice::TxService* tx_service, 
                          txservice::CatalogFactory* catalog_factory) {
    tx_service_ = tx_service;
    catalog_factory_ = catalog_factory;
    
    // NOTE: In a full implementation, we would:
    // 1. Use catalog_factory to create table schemas dynamically
    // 2. Register tables with tx_service
    // 3. Use tx_service for all storage operations
    // For this prototype, we use test_storage_ for verification
    // while demonstrating the proper tx_service integration points
    
    LOG(INFO) << "KVTManager initialized with tx_service and catalog factory";
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
    
    if (!catalog_factory_) {
        error_msg = "CatalogFactory not initialized";
        return 0;
    }
    
    // Create table using catalog factory - following RedisServiceImpl pattern
    auto tx_table_name = std::make_unique<txservice::TableName>(
        table_name, 
        txservice::TableType::Primary, 
        txservice::TableEngine::EloqKv
    );
    
    // Create table schema using catalog factory
    uint64_t version = 1;
    std::string catalog_image = "kvt_simple_string_kv"; // Simple catalog image for KV table
    auto table_schema = catalog_factory_->CreateTableSchema(*tx_table_name, catalog_image, version);
    
    if (!table_schema) {
        error_msg = "Failed to create table schema for " + table_name;
        return 0;
    }
    
    // Create and store the KVTTable
    auto kvt_table = std::make_unique<KVTTable>(table_name, partition_method, std::move(tx_table_name));
    tables_[table_name] = std::move(kvt_table);
    
    uint64_t table_id = next_table_id_++;
    //std::cout << "Created table " << table_name << " with partition method " << partition_method 
    //          << ", assigned table_id " << table_id << std::endl;
    return table_id;
}

uint64_t KVTManager::doStartTx(std::string& error_msg) {
    if (!tx_service_) {
        error_msg = "TxService not initialized";
        return 0;
    }
    
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    uint64_t tx_id = next_transaction_id_++;
    
    // Create a new transaction execution object using tx_service
    // Use default isolation level and concurrency control protocol
    txservice::TransactionExecution* txm = newTxm(
        txservice::IsolationLevel::ReadCommitted,
        txservice::CcProtocol::Locking
    );
    
    if (!txm) {
        error_msg = "Failed to create transaction execution";
        return 0;
    }
    
    active_transactions_[tx_id] = txm;
    
    //std::cout << "Started transaction " << tx_id << " with txm: " << txm << std::endl;
    return tx_id;
}

bool KVTManager::doGet(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                      std::string& value, std::string& error_msg) {
    KVTTable* table = getTable(table_name);
    if (!table) {
        error_msg = "Table " + table_name + " not found";
        return false;
    }
    
    // For prototype: demonstrate tx_service integration points without complex TxCommand
    // In a full implementation:
    // 1. Create EloqKey from string key
    // 2. Create custom KVTGetCommand extending TxCommand
    // 3. Create ObjectCommandTxRequest with the command
    // 4. Execute via tx_service and extract results
    
    // Get or create transaction
    txservice::TransactionExecution* txm = nullptr;
    bool auto_commit = (tx_id == 0);
    
    if (auto_commit) {
        // Create one-shot transaction for auto-commit
        txm = newTxm(txservice::IsolationLevel::ReadCommitted, txservice::CcProtocol::Locking);
        if (!txm) {
            error_msg = "Failed to create transaction for GET";
            return false;
        }
        //std::cout << "GET auto-commit transaction created" << std::endl;
    } else {
        txm = getTransaction(tx_id);
        if (!txm) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        //std::cout << "GET validated against transaction " << tx_id << " (txm: " << txm << ")" << std::endl;
    }
    
    try {
        // Create TxKey for the key
        txservice::TxKey tx_key = txservice::TxKeyFactory::CreateTxKey(key.data(), key.size());
        
        // Create a TxRecord to receive the result
        auto result_record = std::make_unique<txservice::BlobTxRecord>();
        
        // Create ReadTxRequest for GET operation
        txservice::ReadTxRequest read_req(
            table->table_name(),           // table_name
            0,                             // schema_version
            &tx_key,                       // key
            result_record.get(),           // record to store result
            false,                         // is_for_write
            false,                         // is_for_share
            false,                         // read_local
            0,                             // ts
            false,                         // is_covering_keys
            false,                         // is_recovering
            false,                         // point_read_on_cache_miss
            nullptr,                       // yield_fptr
            nullptr,                       // resume_fptr
            txm                            // txm
        );
        
        // Execute the request through tx_service
        txm->ProcessTxRequest(read_req);
        
        // Wait for result
        read_req.Wait();
        
        // Check for errors
        if (read_req.IsError()) {
            error_msg = "GET operation failed: " + read_req.ErrorMsg();
            if (auto_commit) {
                txservice::AbortTx(txm);
            }
            return false;
        } else {
            // Extract result from the request
            auto result_pair = read_req.Result();
            txservice::RecordStatus status = result_pair.first;
            
            if (status == txservice::RecordStatus::Normal) {
                // Key exists, get value from record
                value = result_record->ToString();
                //std::cout << "GET " << table_name << ":" << key << " tx_id=" << tx_id << " found: '" << value << "'" << std::endl;
            } else {
                // Key doesn't exist or other status
                value = "";
                //std::cout << "GET " << table_name << ":" << key << " tx_id=" << tx_id << " not found" << std::endl;
            }
        }
        
        // Auto-commit if needed
        if (auto_commit) {
            auto commit_result = txservice::CommitTx(txm);
            if (!commit_result.first) {
                error_msg = "Failed to commit GET transaction: " + std::to_string(static_cast<int>(commit_result.second));
                return false;
            }
            //std::cout << "GET auto-commit transaction committed" << std::endl;
        }
        
    } catch (const std::exception& e) {
        error_msg = "GET operation failed: " + std::string(e.what());
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
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
    
    // For prototype: demonstrate tx_service integration points without complex TxCommand
    // In a full implementation:
    // 1. Create EloqKey from string key  
    // 2. Create custom KVTSetCommand extending TxCommand with the value
    // 3. Create ObjectCommandTxRequest with the command
    // 4. Execute via tx_service and handle results
    
    // Get or create transaction
    txservice::TransactionExecution* txm = nullptr;
    bool auto_commit = (tx_id == 0);
    
    if (auto_commit) {
        // Create one-shot transaction for auto-commit
        txm = newTxm(txservice::IsolationLevel::ReadCommitted, txservice::CcProtocol::Locking);
        if (!txm) {
            error_msg = "Failed to create transaction for SET";
            return false;
        }
        //std::cout << "SET auto-commit transaction created" << std::endl;
    } else {
        txm = getTransaction(tx_id);
        if (!txm) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        //std::cout << "SET validated against transaction " << tx_id << " (txm: " << txm << ")" << std::endl;
    }
    
    try {
        // Create EloqKey for the key
        auto eloq_key = std::make_unique<EloqKey>(key.data(), key.size());
        
        // Create SetCommand with the value
        auto set_command = std::make_unique<SetCommand>(value);
        
        // Create ObjectCommandTxRequest
        txservice::ObjectCommandTxRequest tx_req(
            table->table_name(),           // table_name
            eloq_key.get(),                // key 
            set_command.get(),             // command
            auto_commit,                   // auto_commit
            false,                         // always_redirect  
            txm                            // txm
        );
        
        // Execute the request through tx_service
        txm->ProcessTxRequest(tx_req);
        
        // Check for errors
        if (tx_req.IsError()) {
            error_msg = "SET operation failed: " + std::to_string(static_cast<int>(tx_req.ErrorCode()));
            if (auto_commit) {
                txservice::AbortTx(txm);
            }
            return false;
        }
        
        // Auto-commit if needed
        if (auto_commit) {
            auto commit_result = txservice::CommitTx(txm);
            if (!commit_result.first) {
                error_msg = "Failed to commit SET transaction: " + std::to_string(static_cast<int>(commit_result.second));
                return false;
            }
            //std::cout << "SET auto-commit transaction committed" << std::endl;
        }
        
    } catch (const std::exception& e) {
        error_msg = "SET operation failed: " + std::string(e.what());
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }
    
    //std::cout << "SET " << table_name << ":" << key << "='" << value << "' tx_id=" << tx_id << std::endl;
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
    
    // Get or create transaction
    txservice::TransactionExecution* txm = nullptr;
    bool auto_commit = (tx_id == 0);
    
    if (auto_commit) {
        // Create one-shot transaction for auto-commit
        txm = newTxm(txservice::IsolationLevel::ReadCommitted, txservice::CcProtocol::Locking);
        if (!txm) {
            error_msg = "Failed to create transaction for scan";
            return false;
        }
        //std::cout << "SCAN auto-commit transaction created for " << table_name << std::endl;
    } else {
        txm = getTransaction(tx_id);
        if (!txm) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        //std::cout << "SCAN validated against transaction " << tx_id << " (txm: " << txm << ")" << std::endl;
    }
    
    try {
        // Create EloqKeys for start and end
        EloqKey start_eloq_key(key_start.data(), key_start.size());
        EloqKey end_eloq_key(key_end.data(), key_end.size());
        
        // Create TxKeys from EloqKeys
        txservice::TxKey start_tx_key(&start_eloq_key);
        txservice::TxKey end_tx_key(&end_eloq_key);
        
        // Get schema version (simplified for prototype)
        uint64_t schema_version = 1;
        
        // Create ScanOpenTxRequest
        txservice::ScanOpenTxRequest scan_open(
            table->table_name(),                    // table_name
            schema_version,                         // schema_version 
            txservice::ScanIndexType::Primary,     // index_type
            &start_tx_key,                          // start_key
            true,                                   // start_inclusive
            &end_tx_key,                            // end_key
            true,                                   // end_inclusive
            txservice::ScanDirection::Forward,     // direction
            false,                                  // is_ckpt
            false,                                  // is_for_write
            false,                                  // is_for_share
            false,                                  // is_covering_keys
            true,                                   // is_require_keys
            true,                                   // is_require_recs
            true,                                   // is_require_sort
            false,                                  // is_read_local
            nullptr,                                // yield_fptr
            nullptr,                                // resume_fptr
            txm                                     // txm
        );
        
        // Execute the scan open request through ProcessTxRequest
        txm->ProcessTxRequest(scan_open);
        
        // Get scan alias from result
        uint64_t scan_alias = scan_open.Result();
        if (scan_alias == UINT64_MAX) {
            error_msg = "Failed to open scan - invalid alias";
            if (auto_commit) {
                txservice::AbortTx(txm);
            }
            return false;
        }
        
        // Fetch results using ScanBatchTxRequest
        std::vector<txservice::ScanBatchTuple> batch_results;
        txservice::ScanBatchTxRequest scan_batch(
            scan_alias,                             // alias
            *table->table_name(),                   // table_name
            &batch_results,                         // batch_vec
            nullptr,                                // yield_fptr
            nullptr,                                // resume_fptr
            txm                                     // txm
        );
        
        size_t total_fetched = 0;
        while (total_fetched < num_item_limit) {
            batch_results.clear();
            
            // Execute scan batch request
            txm->ProcessTxRequest(scan_batch);
            
            if (batch_results.empty()) {
                // No more results
                break;
            }
            
            // Process batch results
            for (const auto& tuple : batch_results) {
                if (total_fetched >= num_item_limit) break;
                
                // Extract key and value from ScanBatchTuple
                if (tuple.record_) {
                    // Get key using GetKey template method
                    const EloqKey* eloq_key = tuple.key_.GetKey<EloqKey>();
                    std::string key = eloq_key ? eloq_key->ToString() : "";
                    
                    // Get value from RedisEloqObject (simplified for prototype)
                    std::string value;
                    const RedisEloqObject* redis_obj = static_cast<const RedisEloqObject*>(tuple.record_);
                    if (redis_obj) {
                        value = redis_obj->ToString();
                    }
                    
                    results.push_back({key, value});
                    total_fetched++;
                }
            }
        }
        
        // Auto-commit if needed
        if (auto_commit) {
            auto commit_result = txservice::CommitTx(txm);
            if (!commit_result.first) {
                error_msg = "Failed to commit scan transaction: " + std::to_string(static_cast<int>(commit_result.second));
                return false;
            }
            //std::cout << "SCAN auto-commit transaction committed" << std::endl;
        }
        
    } catch (const std::exception& e) {
        error_msg = "Scan operation failed: " + std::string(e.what());
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }
    
/*    std::cout << "SCAN " << table_name << " from '" << key_start << "' to '" << key_end 
              << "' limit=" << num_item_limit << " tx_id=" << tx_id 
              << " found " << results.size() << " items" << std::endl;
*/
    return true;
}

bool KVTManager::doCommitTx(uint64_t tx_id, std::string& error_msg) {
    if (tx_id == 0) {
        error_msg = "Cannot commit transaction with id 0";
        return false;
    }
    
    if (!tx_service_) {
        error_msg = "TxService not initialized";
        return false;
    }
    
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    auto it = active_transactions_.find(tx_id);
    if (it == active_transactions_.end()) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    
    txservice::TransactionExecution* txm = it->second;
    if (!txm) {
        error_msg = "Transaction execution object is null";
        return false;
    }
    
    // Use tx_service CommitTx utility function for proper transaction commit
    auto commit_result = txservice::CommitTx(txm);
    
    // Handle commit result
    if (!commit_result.first) {
        error_msg = "Transaction commit failed: " + 
                   std::to_string(static_cast<int>(commit_result.second));
        // Keep transaction in active list on failure
        return false;
    }
    
    // Remove from active transactions only on successful commit
    active_transactions_.erase(tx_id);
    
    //std::cout << "Committed transaction " << tx_id << std::endl;
    return true;
}

bool KVTManager::doAbortTx(uint64_t tx_id, std::string& error_msg) {
    if (tx_id == 0) {
        error_msg = "Cannot rollback transaction with id 0";
        return false;
    }
    
    if (!tx_service_) {
        error_msg = "TxService not initialized";
        return false;
    }
    
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    auto it = active_transactions_.find(tx_id);
    if (it == active_transactions_.end()) {
        error_msg = "Transaction " + std::to_string(tx_id) + " not found";
        return false;
    }
    
    txservice::TransactionExecution* txm = it->second;
    if (!txm) {
        error_msg = "Transaction execution object is null";
        return false;
    }
    
    // Use tx_service AbortTx utility function for proper transaction rollback
    txservice::AbortTx(txm);
    
    // AbortTx is a void function that always succeeds in rolling back the transaction
    // All changes made during the transaction will be properly rolled back by tx_service
    
    // Remove from active transactions after successful abort
    active_transactions_.erase(tx_id);
    
    //std::cout << "Rolled back transaction " << tx_id << std::endl;
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

txservice::TransactionExecution* KVTManager::newTxm(txservice::IsolationLevel iso_level,
                                                   txservice::CcProtocol protocol) {
    if (!tx_service_) {
        LOG(ERROR) << "TxService not initialized";
        return nullptr;
    }
    
    auto* txm = tx_service_->NewTx();
    if (txm) {
        txm->InitTx(iso_level, protocol);
    }
    return txm;
}

bool KVTManager::executeTxRequest(txservice::TransactionExecution* txm,
                                 txservice::ObjectCommandTxRequest* tx_req,
                                 std::string& error_msg) {
    // NOTE: This method is kept for interface compatibility but simplified for prototype
    // In a full implementation, this would:
    // 1. Process the request through the transaction: tx_req->Process(txm)
    // 2. Wait for completion: while (!tx_req->IsFinished()) { wait... }
    // 3. Check for errors: if (tx_req->IsError()) { handle... }
    // 4. Extract results from the command
    
    if (!txm && tx_req) {
        error_msg = "Auto-commit transaction processing not implemented in prototype";
        return false;
    }
    
    //std::cout << "TxRequest execution (prototype): txm=" << txm << std::endl;
    return true;
}

void KVTManager::startTestInBackground() {
    std::thread test_thread([this]() {
        // Wait a bit for the server to fully initialize
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        
        try {
            // Run comprehensive test first to test basic functionality
            std::cout << "=== Running Comprehensive Test First ===" << std::endl;
            runComprehensiveTest();
            
            // Then run stress and performance tests
            std::cout << "=== Running Stress Tests ===" << std::endl;
            runStressTest();
            
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

} // namespace EloqKV