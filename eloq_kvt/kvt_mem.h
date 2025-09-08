#ifndef KVT_MEM_H
#define KVT_MEM_H
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <cassert>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include "kvt_inc.h"

extern int g_verbosity;
extern int g_sanity_check_level;

#if 1
    #define VERBOSE(x) {if (g_verbosity > 0) {x;}}
    #define VERBOSE1(x) {if (g_verbosity > 1) {x;}}
    #define VERBOSE2(x) {if (g_verbosity > 2) {x;}}
#else 
    #define VERBOSE(x) {}
    #define VERBOSE1(x) {}
    #define VERBOSE2(x) {}
#endif

#if 1
    #define CHECK(x) {if (g_sanity_check_level > 0) {x;}}
    #define CHECK1(x) {if (g_sanity_check_level > 1) {x;}}
    #define CHECK2(x) {if (g_sanity_check_level > 2) {x;}}
#else 
    #define CHECK(x) {}
    #define CHECK1(x) {}
    #define CHECK2(x) {}
#endif


class KVTWrapper
{
    private:
        size_t check_point_id_;
        std::ofstream log_ofs_;

        size_t total_mutations_;

        std::string data_path_;

        std::string check_point_name_base_ = "/kvt_checkpoint";
        std::string log_file_name_base_ = "/kvt_log";

        std::string get_checkpoint_name(size_t check_point_id) {
            return data_path_ + check_point_name_base_ + "_"  + std::to_string(check_point_id);
        }

        std::string get_logfile_name(size_t check_point_id) {
            return data_path_ + log_file_name_base_ + "_" + std::to_string(check_point_id);
        }

        size_t get_checkpoint_id_from_file_name(std::string file_name) {
            return std::stoull(file_name.substr(file_name.find_last_of('_') + 1));
        }


        //helper
        KVTKey make_table_key(uint64_t table_id, const KVTKey& key) {
            KVTKey result;
            if (key.empty()) {
                // Special case: empty key (maximum key) is encoded as table_id + 1
                // This ensures empty keys are treated as largest in standard string comparison
                result.resize(8);
                uint64_t encoded_table_id = table_id + 1;
                for (int i = 0; i < 8; ++i) {
                    result[i] = static_cast<char>((encoded_table_id >> (i * 8)) & 0xFF);
                }
            } else {
                result.resize(8 + key.size());
                // Write table_id as 8 bytes (little-endian)
                for (int i = 0; i < 8; ++i) {
                    result[i] = static_cast<char>((table_id >> (i * 8)) & 0xFF);
                }
                // Copy key after the table_id
                std::copy(key.begin(), key.end(), result.begin() + 8);
            }
            return result;
        }
        std::pair<uint64_t, KVTKey> parse_table_key(const KVTKey& table_key) {
            if (table_key.size() < 8) {
                return std::make_pair(0, KVTKey());
            }
            // Read table_id from first 8 bytes (little-endian)
            uint64_t encoded_table_id = 0;
            for (int i = 0; i < 8; ++i) {
                encoded_table_id |= static_cast<uint64_t>(static_cast<unsigned char>(table_key[i])) << (i * 8);
            }
            
            if (table_key.size() == 8) {
                // Special case: 8-byte key indicates empty key (maximum key)
                // Decode by subtracting 1 from the encoded table_id
                uint64_t table_id = encoded_table_id - 1;
                return std::make_pair(table_id, KVTKey());
            } else {
                // Normal case: extract key (everything after the 8-byte table_id)
                KVTKey key(table_key.begin() + 8, table_key.end());
                return std::make_pair(encoded_table_id, key);
            }
        }



    public:
        KVTWrapper(std::string data_path = "./") : data_path_(data_path) {
            check_point_id_ = 0;
            total_mutations_ = 0;
            startup();
        }
        // Virtual destructor to ensure proper cleanup of derived classes
        virtual ~KVTWrapper() = default;

        virtual void startup() {
            int64_t current_check_point_id = -1;
            std::filesystem::path check_point_path(data_path_ + check_point_name_base_);
            
            // Create checkpoint directory if it doesn't exist
            if (!std::filesystem::exists(check_point_path)) {
                std::filesystem::create_directories(check_point_path);
            }
            
            for (const auto& entry : std::filesystem::directory_iterator(check_point_path)) {
                size_t id = get_checkpoint_id_from_file_name(entry.path().string());
                if(id > current_check_point_id) {
                    current_check_point_id = id;
                }
            }
            if (current_check_point_id == -1) {
                std::cout << "No checkpoint found, Trying to Replay Log" << std::endl;
                check_point_id_ = 0;
            } else {
                std::cout << "KVT Store continue from checkpoint " << current_check_point_id << std::endl;
                if (!load_checkpoint(checkpoint_name(current_check_point_id))){
                    std::cout << "KVT Failed to load checkpoint " << current_check_point_id << std::endl;
                    exit(1);
                    return;
                }
            }
            int64_t current_log_id = -1;
            std::filesystem::path log_file_path(data_path_ + log_file_name_base_);
            
            // Create log directory if it doesn't exist
            if (!std::filesystem::exists(log_file_path)) {
                std::filesystem::create_directories(log_file_path);
            }
            
            for (const auto& entry : std::filesystem::directory_iterator(log_file_path)) {
                size_t id = get_checkpoint_id_from_file_name(entry.path().string());
                if(id > current_log_id) {
                    current_log_id = id;
                }
            }
            if (current_log_id >= current_check_point_id + 1) {
                std::cout << "Log file id is greater than checkpoint id, Corrupted Data" << std::endl;
                exit(1);
                return;
            }
            if (current_log_id == -1) {
                std::cout << "No log found, KVT Store start from scratch or clean checkpoint" << std::endl;
                check_point_id_ = 0;
            } else {
                if (std::filesystem::exists(log_file_name(current_check_point_id))) { //replay log
                    std::cout << "KVT Store replay log from checkpoint " << current_check_point_id << std::endl;
                    if (!replay_log(log_file_name(current_check_point_id))){
                        std::cout << "KVT Failed to replay log from checkpoint " << current_check_point_id << std::endl;
                        exit(1);
                        return;
                    }
                }
                check_point_id_ = current_check_point_id + 1;
            }
            
            // Always open log file for writing
            std::string log_file_name_str = log_file_name(check_point_id_);
            std::cout << "DEBUG: Opening log file: " << log_file_name_str << std::endl;
            log_ofs_.open(log_file_name_str, std::ios::out | std::ios::app);
            if (!log_ofs_.is_open()) {
                std::cout << "Failed to open log file: " << log_file_name_str << std::endl;
                exit(1);
            }
            std::cout << "DEBUG: Log file opened successfully" << std::endl;
        }

        virtual bool replay_log(std::string log_file_name)
        {
            std::ifstream log_file(log_file_name);
            if (!log_file.is_open()) {
                std::cout << "Failed to open log file: " << log_file_name << std::endl;
                return false;
            }
            
            std::string line;
            while (std::getline(log_file, line)) {
                if (line.empty()) continue;
                
                std::istringstream iss(line);
                std::string operation;
                iss >> operation;
                
                if (operation == "CREATE_TABLE") {
                    std::string table_name, partition_method;
                    uint64_t table_id;
                    iss >> table_name >> partition_method >> table_id;
                    std::string error_msg;
                    KVTError result = create_table(table_name, partition_method, table_id, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay CREATE_TABLE failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else if (operation == "DROP_TABLE") {
                    uint64_t table_id;
                    iss >> table_id;
                    std::string error_msg;
                    KVTError result = drop_table(table_id, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay DROP_TABLE failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else if (operation == "START_TRANSACTION") {
                    uint64_t tx_id;
                    iss >> tx_id;
                    std::string error_msg;
                    KVTError result = start_transaction(tx_id, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay START_TRANSACTION failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else if (operation == "COMMIT_TRANSACTION") {
                    uint64_t tx_id;
                    iss >> tx_id;
                    std::string error_msg;
                    KVTError result = commit_transaction(tx_id, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay COMMIT_TRANSACTION failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else if (operation == "ROLLBACK_TRANSACTION") {
                    uint64_t tx_id;
                    iss >> tx_id;
                    std::string error_msg;
                    KVTError result = rollback_transaction(tx_id, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay ROLLBACK_TRANSACTION failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else if (operation == "SET") {
                    uint64_t tx_id, table_id;
                    std::string key, value;
                    iss >> tx_id >> table_id >> key >> value;
                    std::string error_msg;
                    KVTError result = set(tx_id, table_id, key, value, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay SET failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else if (operation == "DEL") {
                    uint64_t tx_id, table_id;
                    std::string key;
                    iss >> tx_id >> table_id >> key;
                    std::string error_msg;
                    KVTError result = del(tx_id, table_id, key, error_msg);
                    if (result != KVTError::SUCCESS) {
                        std::cout << "Replay DEL failed: " << error_msg << std::endl;
                        return false;
                    }
                }
                else {
                    std::cout << "Unknown operation in log: " << operation << std::endl;
                    return false;
                }
            }
            
            log_file.close();
            return true;
        }

        virtual void try_check_point(std::string log_file_name)
        {
            const size_t CHECKPOINT_PERIOD = 1000;
            const size_t LOG_SIZE_LIMIT = 1024 * 1024 * 16; 
            const size_t KEEP_HISTORY = 10;
            if (total_mutations_ % CHECKPOINT_PERIOD != 0 || log_ofs_.tellp() < LOG_SIZE_LIMIT)
                return;
            save_checkpoint(get_checkpoint_name(check_point_id_));
            check_point_id_++;
            log_ofs_.close();
            log_ofs_.open(get_logfile_name(check_point_id_), std::ios::out | std::ios::app);
            if (!log_ofs_.is_open()) {
                std::cout << "Failed to open log file: " << log_file_name << std::endl;
                exit(1);
            }
            for (size_t i = 0; i< 10; ++i){
                int64_t id = check_point_id_ - i - KEEP_HISTORY;
                if (id < 0)
                    continue;
                if (std::filesystem::exists(get_checkpoint_name(id))) {
                    std::filesystem::remove(get_checkpoint_name(id));
                }   
                if (std::filesystem::exists(get_logfile_name(id))) {
                    std::filesystem::remove(get_logfile_name(id));
                }
            }
        }
            
        virtual KVTError do_create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) 
        {
            // Log the operation
            total_mutations_++;
            std::cout << "DEBUG: do_create_table called, logging to file" << std::endl;
            log_ofs_ << "CREATE_TABLE " << table_name << " " << partition_method << " " << table_id << std::endl;
            log_ofs_.flush();
            std::cout << "DEBUG: Logged CREATE_TABLE operation" << std::endl;
            
            // Call the actual implementation
            return create_table(table_name, partition_method, table_id, error_msg);
        }
        virtual KVTError do_drop_table(uint64_t table_id, std::string& error_msg)
        {
            total_mutations_++;
            // Log the operation
            log_ofs_ << "DROP_TABLE " << table_id << std::endl;
            log_ofs_.flush();
            
            // Call the actual implementation
            return drop_table(table_id, error_msg);
        }
        virtual KVTError do_start_transaction(uint64_t& tx_id, std::string& error_msg)
        {
            total_mutations_++;
            // Log the operation
            log_ofs_ << "START_TRANSACTION " << tx_id << std::endl;
            log_ofs_.flush();
            
            // Call the actual implementation
            return start_transaction(tx_id, error_msg);
        }
        virtual KVTError do_commit_transaction(uint64_t tx_id, std::string& error_msg) 
        {
            total_mutations_++;
            // Log the operation
            log_ofs_ << "COMMIT_TRANSACTION " << tx_id << std::endl;
            log_ofs_.flush();
            
            // Call the actual implementation
            return commit_transaction(tx_id, error_msg);
        }
        virtual KVTError do_rollback_transaction(uint64_t tx_id, std::string& error_msg)
        {
            total_mutations_++;
            // Log the operation
            log_ofs_ << "ROLLBACK_TRANSACTION " << tx_id << std::endl;
            log_ofs_.flush();
            
            // Call the actual implementation
            return rollback_transaction(tx_id, error_msg);
        }
        // Data operations  
        virtual KVTError do_set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) 
        {
            total_mutations_++;
            // Log the operation
            log_ofs_ << "SET " << tx_id << " " << table_id << " " << key << " " << value << std::endl;
            log_ofs_.flush();
            
            // Call the actual implementation
            return set(tx_id, table_id, key, value, error_msg);
        }
        virtual KVTError do_del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg)
        {
            total_mutations_++;
            // Log the operation
            log_ofs_ << "DEL " << tx_id << " " << table_id << " " << key << std::endl;
            log_ofs_.flush();
            
            // Call the actual implementation
            return del(tx_id, table_id, key, error_msg);
        }
        

        virtual bool save_checkpoint(std::string checkpoint_name) = 0;
        virtual bool load_checkpoint(std::string checkpoint_name) = 0;
        // Table management
        virtual KVTError create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) = 0;
        virtual KVTError drop_table(uint64_t table_id, std::string& error_msg) = 0;
        virtual KVTError get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) = 0;
        virtual KVTError get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) = 0;
        virtual KVTError list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) = 0;
        virtual KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) = 0;
        virtual KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) = 0;
        virtual KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) = 0;
        // Data operations  
        virtual KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 std::string& value, std::string& error_msg) = 0;
        virtual KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) = 0;
        virtual KVTError del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg) = 0;
        // Scan keys in range [key_start, key_end) - key_start inclusive, key_end exclusive
        virtual KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) = 0;

        virtual KVTError process(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const KVTProcessFunc& func, const std::string& parameter, std::string& result_value, std::string& error_msg)
                {
                    std::string orig_value;
                    KVTError r_get = get(tx_id, table_id, key, orig_value, error_msg);
                    if (r_get != KVTError::SUCCESS)
                        return r_get;
                    
                    KVTProcessInput input(&key, &orig_value, &parameter);
                    KVTProcessOutput output;
                    bool success = func(input, output);
                    
                    if (!success) {
                        if (output.return_value.has_value()) {
                            error_msg = output.return_value.value();
                        } else {
                            error_msg = "Process function failed";
                        }
                        return KVTError::EXT_FUNC_ERROR;
                    }
                    
                    if (output.update_value.has_value()) {
                        KVTError r_set = set(tx_id, table_id, key, output.update_value.value(), error_msg);
                        if (r_set != KVTError::SUCCESS) {
                            result_value.clear();
                            return r_set;
                        }
                    }
                    
                    if (output.delete_key) {
                        KVTError r_del = del(tx_id, table_id, key, error_msg);
                        if (r_del != KVTError::SUCCESS) {
                            result_value.clear();
                            return r_del;
                        }
                    }
                    
                    if (output.return_value.has_value()) {
                        result_value = output.return_value.value();
                    } else {
                        result_value.clear();
                    }
                    
                    return KVTError::SUCCESS;
                }
                
        virtual KVTError range_process(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  const KVTProcessFunc& func, const std::string& parameter, std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg)
                {
                    std::vector<std::pair<KVTKey, std::string>> temp_results;
                    KVTKey new_start_key = key_start;
                    KVTError r_scan = KVTError::UNKNOWN_ERROR;
                    bool first_item = true;
                    while (results.size() < num_item_limit) {
                        temp_results.clear();
                        r_scan = scan(tx_id, table_id, new_start_key, key_end, num_item_limit, temp_results, error_msg);
                        if (r_scan != KVTError::SUCCESS && r_scan != KVTError::SCAN_LIMIT_REACHED) {
                            results.clear();
                            return r_scan;
                        }
                        
                        for (auto& [key, orig_value] : temp_results) {
                            KVTProcessInput input(&key, &orig_value, &parameter);
                            input.range_first = first_item;
                            first_item = false;
                            KVTProcessOutput output;
                            bool success = func(input, output);
                            
                            if (!success) {
                                if (output.return_value.has_value()) {
                                    error_msg = output.return_value.value();
                                } else {
                                    error_msg = "Process function failed";
                                }
                                results.clear();
                                return KVTError::EXT_FUNC_ERROR;
                            }
                            
                            if (output.update_value.has_value()) {
                                KVTError r_set = set(tx_id, table_id, key, output.update_value.value(), error_msg);
                                if (r_set != KVTError::SUCCESS) {
                                    results.clear();
                                    return r_set;
                                }
                            }
                            
                            if (output.delete_key) {
                                KVTError r_del = del(tx_id, table_id, key, error_msg);
                                if (r_del != KVTError::SUCCESS) {
                                    results.clear();
                                    return r_del;
                                }
                            }
                            
                            if (output.return_value.has_value()) {
                                results.emplace_back(key, output.return_value.value());
                            }
                        }
                        
                        if (temp_results.empty()) break;
                        new_start_key = temp_results.back().first;
                        new_start_key += '\0'; // Move to next key
                    } 
                    std::string dummy; 
                    KVTProcessInput input(nullptr, nullptr, nullptr, false, true);
                    KVTProcessOutput output;
                    bool success = func(input, output);
                    if (!success) {
                        results.clear();
                        return KVTError::EXT_FUNC_ERROR;
                    }
                    if (output.return_value.has_value()) {
                        error_msg = output.return_value.value();
                    }
                    return r_scan;
                }
 
        // Default batch execute implementation - executes operations individually
        virtual KVTError batch_execute(uint64_t tx_id, const KVTBatchOps& batch_ops, 
                  KVTBatchResults& batch_results, std::string& error_msg) {
            batch_results.clear();
            batch_results.reserve(batch_ops.size());
            
            bool all_success = true;
            std::string concatenated_errors;
            
            for (size_t i = 0; i < batch_ops.size(); ++i) {
                const KVTOp& op = batch_ops[i];
                KVTOpResult result;
                std::string op_error;
                
                switch (op.op) {
                    case OP_GET:
                        result.error = get(tx_id, op.table_id, op.key, result.value, op_error);
                        break;
                    case OP_SET:
                        result.error = set(tx_id, op.table_id, op.key, op.value, op_error);
                        break;
                    case OP_DEL:
                        result.error = del(tx_id, op.table_id, op.key, op_error);
                        break;
                    default:
                        result.error = KVTError::UNKNOWN_ERROR;
                        op_error = "Unknown operation type";
                        break;
                }
                
                if (result.error != KVTError::SUCCESS) {
                    all_success = false;
                    if (!op_error.empty()) {
                        concatenated_errors += "op[" + std::to_string(i) + "]: " + op_error + "; ";
                    }
                }
                
                batch_results.push_back(result);
            }
            
            if (all_success) {
                return KVTError::SUCCESS;
            } else {
                error_msg = concatenated_errors;
                return KVTError::BATCH_NOT_FULLY_SUCCESS;
            }
        }
    
};

class KVTMemManagerBase : public KVTWrapper
{
    protected:
        struct Entry {
            std::string data;
            int32_t metadata; //for 2PL, it is the lock flag, for OCC, it is the version number. -1 means deleted. 
            
            Entry() : data(""), metadata(0) {}
            Entry(const std::string& d, int32_t m) : data(d), metadata(m) {}
        };

        struct Table {
            uint64_t id;
            std::string name;
            std::string partition_method;  // "hash" or "range"
            std::map<KVTKey, Entry> data;
            Table(const std::string& n, const std::string& pm, uint64_t i) : name(n), partition_method(pm), id(i) {}
        };

        struct Transaction {
            uint64_t tx_id;
            std::map<KVTKey, Entry> read_set;    // table_key -> Value (for reads)
            std::map<KVTKey, Entry> write_set;   // table_key -> Value (for writes)
            std::unordered_set<KVTKey> delete_set; // table_key -> deleted
            
            Transaction(uint64_t id) : tx_id(id) {}
        };

        std::unordered_map<std::string, std::unique_ptr<Table>> tables;
        std::unordered_map<std::string, uint64_t> tablename_to_id;
        std::mutex global_mutex;
        uint64_t next_table_id;
        uint64_t next_tx_id;

    public:
        KVTMemManagerBase()
        {
            next_table_id = 1;
            next_tx_id = 1;
        }
        ~KVTMemManagerBase()
        {
        }

        bool save_checkpoint(std::string checkpoint_name) override
        {
            
        }
        bool load_checkpoint(std::string checkpoint_name) override
        {

        }
        // Table management
        KVTError create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tables.find(table_name) != tables.end()) {
                error_msg = "Table '" + table_name + "' already exists";
                return KVTError::TABLE_ALREADY_EXISTS;
            }
            if (partition_method != "hash" && partition_method != "range") {
                error_msg = "Invalid partition method. Must be 'hash' or 'range'";
                return KVTError::INVALID_PARTITION_METHOD;
            }
            uint64_t table_id_val = next_table_id ++;
            tables[table_name] = std::make_unique<Table>(table_name, partition_method, table_id_val);
            tablename_to_id[table_name] = table_id_val;
            table_id = table_id_val;
            return KVTError::SUCCESS;
            
        }

        KVTError drop_table(uint64_t table_id, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            std::string table_name;
            for (const auto& pair : tablename_to_id) {
                if (pair.second == table_id) {
                    table_name = pair.first;
                    break;
                }
            }
            if (table_name.empty()) {
                error_msg = "Table with ID " + std::to_string(table_id) + " not found";
                return KVTError::TABLE_NOT_FOUND;
            }
            tables.erase(table_name);
            tablename_to_id.erase(table_name);
            return KVTError::SUCCESS;
        }

        KVTError get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            for (const auto& pair : tablename_to_id) {
                if (pair.second == table_id) {
                    table_name = pair.first;
                    return KVTError::SUCCESS;
                }
            }
            error_msg = "Table with ID " + std::to_string(table_id) + " not found";
            return KVTError::TABLE_NOT_FOUND;
        }

        KVTError get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            auto it = tablename_to_id.find(table_name);
            if (it == tablename_to_id.end()) {
                error_msg = "Table '" + table_name + "' not found";
                return KVTError::TABLE_NOT_FOUND;
            }
            table_id = it->second;
            return KVTError::SUCCESS;
        }

        KVTError list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) override
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            results.clear();
            for (const auto& pair : tablename_to_id) {
                results.emplace_back(pair.first, pair.second);
            }
            return KVTError::SUCCESS;
        }
};

class KVTMemManagerNoCC : public KVTMemManagerBase
{
    public:
        KVTMemManagerNoCC() : KVTMemManagerBase()
        {
        }
        ~KVTMemManagerNoCC()
        {
        }

        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override;
        KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
        KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
        // Data operations  
        KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 std::string& value, std::string& error_msg) override;
        KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) override;
        KVTError del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg) override;
        KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
    }                  
;

class KVTMemManagerSimple : public KVTMemManagerBase
{
    private:
        //Only process one at a time, and transaction that can be rolled back.
        Transaction current_tx;
        uint64_t current_tx_id;

    public:
        KVTMemManagerSimple() : KVTMemManagerBase(), current_tx(0)
        {
            current_tx_id = 0;
        }
        ~KVTMemManagerSimple()
        {
        }

        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override;
        KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
        KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
        // Data operations  
        KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 std::string& value, std::string& error_msg) override;
        KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                 const std::string& value, std::string& error_msg) override;
        KVTError del(uint64_t tx_id, uint64_t table_id, 
                const KVTKey& key, std::string& error_msg) override;
        KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                  const KVTKey& key_end, size_t num_item_limit, 
                  std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
};


class KVTMemManager : public KVTMemManagerBase
{
    protected:

        std::unordered_map<uint64_t, std::unique_ptr<Transaction>> transactions;

        Transaction* get_transaction(uint64_t tx_id) {
            auto it = transactions.find(tx_id);
            if (it == transactions.end()) {
                return nullptr;
            }
            return it->second.get();
        }

    public:
        KVTMemManager(std::string data_path = "./") : KVTMemManagerBase(), data_path(data_path)
        {
            next_table_id = 1;
            next_tx_id = 1;
        }

        ~KVTMemManager()
        {
        }
    
        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override {
            std::lock_guard<std::mutex> lock(global_mutex);
            uint64_t tx_id_val = next_tx_id ++;
            transactions[tx_id_val] = std::make_unique<Transaction>(tx_id_val);
            tx_id = tx_id_val;
            return KVTError::SUCCESS;
        }
    };

class KVTMemManager2PL: public KVTMemManager
{
    // For table entries: metadata field stores the locking transaction ID (0 = unlocked)
    // When a transaction acquires a lock, it sets metadata to its tx_id
public:
    // Transaction management
    KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
    KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
    // Data operations  
    KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                std::string& value, std::string& error_msg) override;
    KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                const std::string& value, std::string& error_msg) override;
    KVTError del(uint64_t tx_id, uint64_t table_id, 
            const KVTKey& key, std::string& error_msg) override;
    KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                const KVTKey& key_end, size_t num_item_limit, 
                std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
};


class KVTMemManagerOCC: public KVTMemManager
{
    //for OCC, the metadata in an entry is the version number.
    //delete must be put into the read set so that it can keep version.
    //in this case, when a delete happens, it needs to be removed from write

    //Important Invariance: 
    //1. a key cannot appear in both write set and delete set. 
    //2. a deleted key must be in read set, if it does not already in write set (and then need to be removed)

public:
    // Constructor
    KVTMemManagerOCC(std::string data_path = "./") : KVTMemManager(data_path) {}
    
  // Transaction management
    KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
    KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
    // Data operations  
    KVTError get(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                std::string& value, std::string& error_msg) override;
    KVTError set(uint64_t tx_id, uint64_t table_id, const KVTKey& key, 
                const std::string& value, std::string& error_msg) override;
    KVTError del(uint64_t tx_id, uint64_t table_id, 
            const KVTKey& key, std::string& error_msg) override;
    KVTError scan(uint64_t tx_id, uint64_t table_id, const KVTKey& key_start, 
                const KVTKey& key_end, size_t num_item_limit, 
                std::vector<std::pair<KVTKey, std::string>>& results, std::string& error_msg) override;
  };

#endif // KVT_MEM_H
