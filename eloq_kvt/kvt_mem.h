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
#include <iostream>
#include <algorithm>

#define DEBUG(x) {x;}
//#define DEBUG(x) {}

/**
 * KVT Error Codes
 * 
 * Enumeration of all possible error conditions in the KVT system.
 * SUCCESS indicates successful operation, all other values indicate errors.
 */
enum class KVTError {
    SUCCESS = 0,                           // Operation completed successfully
    KVT_NOT_INITIALIZED,                   // KVT system not initialized
    TABLE_ALREADY_EXISTS,                  // Table with given name already exists
    TABLE_NOT_FOUND,                       // Table with given name does not exist
    INVALID_PARTITION_METHOD,              // Partition method is not "hash" or "range"
    TRANSACTION_NOT_FOUND,                 // Transaction with given ID does not exist
    TRANSACTION_ALREADY_RUNNING,           // Another transaction is already running
    KEY_NOT_FOUND,                         // Key does not exist in the table
    KEY_IS_DELETED,                        // Key was deleted in the current transaction
    KEY_IS_LOCKED,                         // Key is locked by another transaction (2PL)
    TRANSACTION_HAS_STALE_DATA,            // OCC validation failed due to concurrent modifications
    ONE_SHOT_WRITE_NOT_ALLOWED,           // Write operations require an active transaction
    ONE_SHOT_DELETE_NOT_ALLOWED,          // Delete operations require an active transaction
    BATCH_NOT_FULLY_SUCCESS,               // Some operations succeeded, some failed
    UNKNOWN_ERROR                          // Unknown or unexpected error
};

enum KVT_OPType //for batch operations
{
    OP_UNKNOWN,
    OP_GET,
    OP_SET,
    OP_DEL,
};

struct KVTOp
{
    enum KVT_OPType op;
    uint64_t table_id;  //table ID instead of table name
    std::string key;
    std::string value;
}; 

struct KVTOpResult
{
    KVTError error;
    std::string value; //only valid for get operation
}; 

typedef std::vector<KVTOp> KVTBatchOps;
typedef std::vector<KVTOpResult> KVTBatchResults;

class KVTWrapper
{
    public:
        // Virtual destructor to ensure proper cleanup of derived classes
        virtual ~KVTWrapper() = default;
        
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
        virtual KVTError get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                 std::string& value, std::string& error_msg) = 0;
        virtual KVTError set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                 const std::string& value, std::string& error_msg) = 0;
        virtual KVTError del(uint64_t tx_id, uint64_t table_id, 
                const std::string& key, std::string& error_msg) = 0;
        // Scan keys in range [key_start, key_end) - key_start inclusive, key_end exclusive
        virtual KVTError scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
                  const std::string& key_end, size_t num_item_limit, 
                  std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) = 0;
        
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

class KVTMemManagerNoCC : public KVTWrapper
{
    private:
        std::map<std::string, std::string> table_data;
        std::unordered_map<std::string, uint32_t> table_to_id;
        uint64_t next_table_id;
        uint64_t next_tx_id;
        std::mutex global_mutex;

        std::string make_table_key(uint64_t table_id, const std::string& key) {
            std::string result;
            result.resize(8 + key.size());
            // Write table_id as 8 bytes (little-endian)
            for (int i = 0; i < 8; ++i) {
                result[i] = static_cast<char>((table_id >> (i * 8)) & 0xFF);
            }
            // Copy key after the table_id
            std::copy(key.begin(), key.end(), result.begin() + 8);
            return result;
        }
        std::pair<uint64_t, std::string> parse_table_key(const std::string& table_key) {
            if (table_key.size() < 8) {
                return std::make_pair(0, "");
            }
            // Read table_id from first 8 bytes (little-endian)
            uint64_t table_id = 0;
            for (int i = 0; i < 8; ++i) {
                table_id |= static_cast<uint64_t>(static_cast<unsigned char>(table_key[i])) << (i * 8);
            }
            // Extract key (everything after the 8-byte table_id)
            std::string key(table_key.begin() + 8, table_key.end());
            return std::make_pair(table_id, key);
        }
    public:
        KVTMemManagerNoCC()
        {
            next_table_id = 1;
            next_tx_id = 1;
        }
        ~KVTMemManagerNoCC()
        {
        }

        KVTError create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) override;
        KVTError drop_table(uint64_t table_id, std::string& error_msg) override;
        KVTError get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) override;
        KVTError get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) override;
        KVTError list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) override;
        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override;
        KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
        KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
        // Data operations  
        KVTError get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                 std::string& value, std::string& error_msg) override;
        KVTError set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                 const std::string& value, std::string& error_msg) override;
        KVTError del(uint64_t tx_id, uint64_t table_id, 
                const std::string& key, std::string& error_msg) override;
        KVTError scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
                  const std::string& key_end, size_t num_item_limit, 
                  std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) override;
    }                  
;

class KVTMemManagerSimple : public KVTWrapper
{
    private:
        std::map<std::string, std::string> table_data;
        std::unordered_map<std::string, uint32_t> table_to_id;
        uint64_t next_table_id;
        uint64_t next_tx_id;
        uint64_t current_tx_id;
        std::mutex global_mutex;
        //helper
        std::string make_table_key(uint64_t table_id, const std::string& key) {
            std::string result;
            result.resize(8 + key.size());
            // Write table_id as 8 bytes (little-endian)
            for (int i = 0; i < 8; ++i) {
                result[i] = static_cast<char>((table_id >> (i * 8)) & 0xFF);
            }
            // Copy key after the table_id
            std::copy(key.begin(), key.end(), result.begin() + 8);
            return result;
        }
        std::pair<uint64_t, std::string> parse_table_key(const std::string& table_key) {
            if (table_key.size() < 8) {
                return std::make_pair(0, "");
            }
            // Read table_id from first 8 bytes (little-endian)
            uint64_t table_id = 0;
            for (int i = 0; i < 8; ++i) {
                table_id |= static_cast<uint64_t>(static_cast<unsigned char>(table_key[i])) << (i * 8);
            }
            // Extract key (everything after the 8-byte table_id)
            std::string key(table_key.begin() + 8, table_key.end());
            return std::make_pair(table_id, key);
        }


        std::map<std::string, std::string> write_set; 
        std::unordered_set<std::string> delete_set;

    public:
        KVTMemManagerSimple()
        {
            next_table_id = 1;
            next_tx_id = 1;
            current_tx_id = 0;
        }
        ~KVTMemManagerSimple()
        {
        }

        KVTError create_table(const std::string& table_name, const std::string& partition_method, uint64_t& table_id, std::string& error_msg) override;
        KVTError drop_table(uint64_t table_id, std::string& error_msg) override;
        KVTError get_table_name(uint64_t table_id, std::string& table_name, std::string& error_msg) override;
        KVTError get_table_id(const std::string& table_name, uint64_t& table_id, std::string& error_msg) override;
        KVTError list_tables(std::vector<std::pair<std::string, uint64_t>>& results, std::string& error_msg) override;
        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override;
        KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
        KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
        // Data operations  
        KVTError get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                 std::string& value, std::string& error_msg) override;
        KVTError set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                 const std::string& value, std::string& error_msg) override;
        KVTError del(uint64_t tx_id, uint64_t table_id, 
                const std::string& key, std::string& error_msg) override;
        KVTError scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
                  const std::string& key_end, size_t num_item_limit, 
                  std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) override;
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
            std::map<std::string, Entry> data;
            
            Table(const std::string& n, const std::string& pm, uint64_t i) : name(n), partition_method(pm), id(i) {}
        };

        struct Transaction {
            uint64_t tx_id;
            std::map<std::string, Entry> read_set;    // table_key -> Value (for reads)
            std::map<std::string, Entry> write_set;   // table_key -> Value (for writes)
            std::unordered_set<std::string> delete_set; // table_key -> deleted
            
            Transaction(uint64_t id) : tx_id(id) {}
        };

        std::unordered_map<std::string, std::unique_ptr<Table>> tables;
        std::unordered_map<uint64_t, std::unique_ptr<Transaction>> transactions;
        std::unordered_map<std::string, uint64_t> tablename_to_id;

        std::mutex global_mutex;
        uint64_t next_table_id;
        uint64_t next_tx_id;

        //helper
        std::string make_table_key(uint64_t table_id, const std::string& key) {
            std::string result;
            result.resize(8 + key.size());
            // Write table_id as 8 bytes (little-endian)
            for (int i = 0; i < 8; ++i) {
                result[i] = static_cast<char>((table_id >> (i * 8)) & 0xFF);
            }
            // Copy key after the table_id
            std::copy(key.begin(), key.end(), result.begin() + 8);
            return result;
        }

        std::pair<uint64_t, std::string> parse_table_key(const std::string& table_key) {
            if (table_key.size() < 8) {
                return std::make_pair(0, "");
            }
            // Read table_id from first 8 bytes (little-endian)
            uint64_t table_id = 0;
            for (int i = 0; i < 8; ++i) {
                table_id |= static_cast<uint64_t>(static_cast<unsigned char>(table_key[i])) << (i * 8);
            }
            // Extract key (everything after the 8-byte table_id)
            std::string key(table_key.begin() + 8, table_key.end());
            return std::make_pair(table_id, key);
        }

        Table* get_table(const std::string& table_name) {
            auto it = tables.find(table_name);
            if (it == tables.end()) {
                return nullptr;
            }
            return it->second.get();
        }

        Table* get_table_by_id(uint64_t table_id) {
            for (const auto& pair : tablename_to_id) {
                if (pair.second == table_id) {
                    auto it = tables.find(pair.first);
                    if (it != tables.end()) {
                        return it->second.get();
                    }
                }
            }
            return nullptr;
        }

        Transaction* get_transaction(uint64_t tx_id) {
            auto it = transactions.find(tx_id);
            if (it == transactions.end()) {
                return nullptr;
            }
            return it->second.get();
        }

    public:
        KVTMemManagerBase()
        {
            next_table_id = 1;
            next_tx_id = 1;
        }

        ~KVTMemManagerBase()
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

        KVTError start_transaction(uint64_t& tx_id, std::string& error_msg) override {
            std::lock_guard<std::mutex> lock(global_mutex);
            uint64_t tx_id_val = next_tx_id ++;
            transactions[tx_id_val] = std::make_unique<Transaction>(tx_id_val);
            tx_id = tx_id_val;
            return KVTError::SUCCESS;
        }
    };

class KVTMemManager2PL: public KVTMemManagerBase
{
    // For table entries: metadata field stores the locking transaction ID (0 = unlocked)
    // When a transaction acquires a lock, it sets metadata to its tx_id
public:
    // Transaction management
    KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
    KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
    // Data operations  
    KVTError get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                std::string& value, std::string& error_msg) override;
    KVTError set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                const std::string& value, std::string& error_msg) override;
    KVTError del(uint64_t tx_id, uint64_t table_id, 
            const std::string& key, std::string& error_msg) override;
    KVTError scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
                const std::string& key_end, size_t num_item_limit, 
                std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) override;
};


class KVTMemManagerOCC: public KVTMemManagerBase
{
    //for OCC, the metadata in an entry is the version number.
    //delete must be put into the read set so that it can keep version.
    //in this case, when a delete happens, it needs to be removed from write

    //Important Invariance: 
    //1. a key cannot appear in both write set and delete set. 
    //2. a deleted key must be in read set, if it does not already in write set (and then need to be removed)

public:
  // Transaction management
    KVTError commit_transaction(uint64_t tx_id, std::string& error_msg) override;
    KVTError rollback_transaction(uint64_t tx_id, std::string& error_msg) override;
    // Data operations  
    KVTError get(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                std::string& value, std::string& error_msg) override;
    KVTError set(uint64_t tx_id, uint64_t table_id, const std::string& key, 
                const std::string& value, std::string& error_msg) override;
    KVTError del(uint64_t tx_id, uint64_t table_id, 
            const std::string& key, std::string& error_msg) override;
    KVTError scan(uint64_t tx_id, uint64_t table_id, const std::string& key_start, 
                const std::string& key_end, size_t num_item_limit, 
                std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) override;
  };


#endif // KVT_MEM_H
