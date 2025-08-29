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
#include "tx_service/include/eloq_string_key_record.h"
#include <unordered_map>

namespace EloqKV {

// NOTE: KVTCommand implementations removed for prototype simplicity
// In a full implementation, these would be complete TxCommand classes
using namespace txservice;

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
    
    // Create and store the KVTTable
    auto kvt_table = std::make_unique<KVTTable>(table_name, partition_method);
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
    
    std::lock_guard<std::mutex> lock(transactions_mutex_);
    uint64_t tx_id = next_transaction_id_++;
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

    bool is_hash_table = table->partition_method() == "hash";

    // First put a read lock on the table
    TableName table_name_obj(table_name, TableType::Primary, is_hash_table ? TableEngine::InternalHash : TableEngine::InternalRange);
    CatalogKey table_key(table_name_obj);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_record;


    ReadTxRequest read_tx_req(&catalog_ccm_name, 0, &tbl_tx_key,
        &catalog_record, false, false, true, 0, false,
        false, false, nullptr,
        nullptr, txm);
    txm->Execute(&read_tx_req);
    read_tx_req.Wait();
    if (read_tx_req.ErrorCode() != TxErrorCode::NO_ERROR) {
        error_msg = "GET operation failed: " + std::to_string(static_cast<int>(read_tx_req.ErrorCode()));
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }   
    RecordStatus rec_status= read_tx_req.Result().first;

    if (rec_status == RecordStatus::Deleted) {
        // Table not exist
        error_msg = "Table " + table_name + " not found";
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }

    uint64_t schema_version = catalog_record.SchemaTs();

    // Add table name as prefix to distinguish between different KVT tables
    std::string prefixed_key = table_name + ":" + key;
    EloqStringKey key_obj(prefixed_key.data(), prefixed_key.size());
    TxKey tx_key(&key_obj);
    EloqStringRecord record;
    ReadTxRequest read_req(
        &table_name_obj, schema_version, &tx_key,
        &record, false, false, false, 0, false,
        false, false, nullptr, nullptr, txm);
    txm->Execute(&read_req);
    read_req.Wait();
    if (read_req.ErrorCode() != TxErrorCode::NO_ERROR) {
        error_msg = "GET operation failed: " + std::to_string(static_cast<int>(read_req.ErrorCode()));
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }   
    rec_status= read_req.Result().first;
    if (rec_status == RecordStatus::Deleted) {
        error_msg = "Key " + key + " not found";
        return false;
    }
    value = record.ToString();

    if (auto_commit) {
        auto commit_result = txservice::CommitTx(txm);
        if (!commit_result.first) {
            error_msg = "Failed to commit GET transaction: " + std::to_string(static_cast<int>(commit_result.second));
            return false;
        }
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

    bool is_hash_table = table->partition_method() == "hash";

    // First put a read lock on the table
    TableName table_name_obj(table_name, TableType::Primary, is_hash_table ? TableEngine::InternalHash : TableEngine::InternalRange);
    CatalogKey table_key(table_name_obj);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_record;


    ReadTxRequest read_tx_req(&catalog_ccm_name, 0, &tbl_tx_key,
        &catalog_record, false, false, true, 0, false,
        false, false, nullptr,
        nullptr, txm);
    txm->Execute(&read_tx_req);
    read_tx_req.Wait();
    RecordStatus rec_status= read_tx_req.Result().first;

    if (rec_status == RecordStatus::Deleted) {
        // Table not exist
        error_msg = "Table " + table_name + " not found";
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }

    uint64_t schema_version = catalog_record.SchemaTs();

    // Add table name as prefix to distinguish between different KVT tables
    std::string prefixed_key = table_name + ":" + key;
    EloqStringKey key_obj(prefixed_key.data(), prefixed_key.size());
    TxKey tx_key(&key_obj);
    EloqStringRecord record;
    ReadTxRequest read_req(
        &table_name_obj, schema_version, &tx_key,
        &record, true, false, false, 0, false,
        false, false, nullptr, nullptr, txm);
    txm->Execute(&read_req);
    read_req.Wait();
    rec_status= read_req.Result().first;

    if (read_req.ErrorCode() != TxErrorCode::NO_ERROR) {
        error_msg = "SET operation failed: " + std::to_string(static_cast<int>(read_req.ErrorCode()));
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }

    // We need to pass in ownership of key and record to the txm
    EloqStringRecord::Uptr record_ptr = std::make_unique<EloqStringRecord>();
    std::unique_ptr<EloqStringKey> key_ptr = std::make_unique<EloqStringKey>(prefixed_key.data(), prefixed_key.size());
    record_ptr->SetEncodedBlob(reinterpret_cast<const unsigned char *>(value.data()), value.size());
    OperationType op_type = read_req.Result().first == RecordStatus::Deleted ? OperationType::Insert : OperationType::Update;

    txm->TxUpsert(table_name_obj, schema_version, TxKey(std::move(key_ptr)), std::move(record_ptr), op_type);

    if (auto_commit) {
        auto commit_result = txservice::CommitTx(txm);
        if (!commit_result.first) {
            error_msg = "Failed to commit SET transaction: " + std::to_string(static_cast<int>(commit_result.second));
            return false;
        }
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

    if (table->partition_method() == "hash") {
        error_msg = "SCAN is not supported for hash tables";
        return false;
    }


    // First put a read lock on the table
    TableName table_name_obj(table_name, TableType::Primary, TableEngine::InternalRange);
    CatalogKey table_key(table_name_obj);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_record;


    ReadTxRequest read_tx_req(&catalog_ccm_name, 0, &tbl_tx_key,
        &catalog_record, false, false, true, 0, false,
        false, false, nullptr,
        nullptr, txm);
    txm->Execute(&read_tx_req);
    read_tx_req.Wait();
    RecordStatus rec_status= read_tx_req.Result().first;

    if (rec_status == RecordStatus::Deleted) {
        // Table not exist
        error_msg = "Table " + table_name + " not found";
        if (auto_commit) {
            txservice::AbortTx(txm);
        }
        return false;
    }

    uint64_t schema_version = catalog_record.SchemaTs();

    // Add table name as prefix to distinguish between different KVT tables
    std::string prefixed_start_key = table_name + ":" + key_start;
    std::string prefixed_end_key = table_name + ":" + key_end;
    EloqStringKey start_key_obj(prefixed_start_key.data(), prefixed_start_key.size());
    EloqStringKey end_key_obj(prefixed_end_key.data(), prefixed_end_key.size());
    TxKey start_tx_key(&start_key_obj);
    TxKey end_tx_key(&end_key_obj);

    // Create ScanOpenTxRequest
    txservice::ScanOpenTxRequest scan_open(
        &table_name_obj,                    // table_name
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

    
    // Get scan alias from result
    uint64_t scan_alias = txm->OpenTxScan(scan_open);
    
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
        table_name_obj,                   // table_name
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
            if (tuple.record_ && tuple.status_ != RecordStatus::Deleted) {
                // Get key using GetKey template method
                const EloqStringKey* eloq_key = tuple.key_.GetKey<EloqStringKey>();
                std::string prefixed_key = eloq_key ? eloq_key->ToString() : "";
                
                // Strip table name prefix from the key
                std::string key = prefixed_key;
                size_t colon_pos = prefixed_key.find(table_name + ":");
                if (colon_pos != std::string::npos) {
                    key = prefixed_key.substr(colon_pos + 1);
                }

                // Get value from EloqStringRecord
                std::string value;
                const EloqStringRecord* eloq_record = static_cast<const EloqStringRecord*>(tuple.record_);
                if (eloq_record) {
                    value = eloq_record->ToString();
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