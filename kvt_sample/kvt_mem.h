#ifndef KVT_MEM_H
#define KVT_MEM_H

#include <map>
#include <unordered_map>
#include <mutex>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
//#include <algorithm>

// Forward declarations
class Table;
class Transaction;
class Value;



class KVTManagerWrapperInterface
{
    public:
        // Table management
        virtual uint64_t create_table(const std::string& table_name, const std::string& partition_method, std::string& error_msg) = 0;
        virtual uint64_t start_transaction(std::string& error_msg) = 0;
        virtual bool commit_transaction(uint64_t tx_id, std::string& error_msg) = 0;
        virtual bool rollback_transaction(uint64_t tx_id, std::string& error_msg) = 0;
        // Data operations  
        virtual bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 std::string& value, std::string& error_msg) = 0;
        virtual bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 const std::string& value, std::string& error_msg) = 0;
        virtual bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
                  const std::string& key_end, size_t num_item_limit, 
                  std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) = 0;
};

//only allow a single transaction at a time, truely serializable.
class KVTManagerWrapperSimple : public KVTManagerWrapperInterface
{
    private:
        std::map<std::string, std::string> table_data;
        std::unordered_map<std::string, uint32_t> table_to_id;
        uint64_t next_table_id;
        uint64_t next_tx_id;
        uint64_t current_tx_id;
        std::mutex global_mutex;
        //helper
        std::string make_table_key(const std::string& table_name, const std::string& key) {
            return table_name + std::string(1, '\0') + key;
        }

        std::unordered_map<std::string, std::string> write_set; 

    public:
        KVTManagerWrapperSimple()
        {
            next_table_id = 1;
            next_tx_id = 1;
            current_tx_id = 0;
        }
        ~KVTManagerWrapperSimple()
        {
        }

        uint64_t create_table(const std::string& table_name, const std::string& partition_method, std::string& error_msg)
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (table_to_id.find(table_name) != table_to_id.end()) {
                error_msg = "Table " + table_name + " already exists";
                return 0;
            }
            table_to_id[table_name] = next_table_id;
            next_table_id += 1;
            return next_table_id - 1;
        }

        uint64_t start_transaction(std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (current_tx_id != 0) {
                error_msg = "A transaction is already running";
                return 0;
            }
            current_tx_id = next_tx_id;
            next_tx_id += 1;
            return current_tx_id;
        }

        bool commit_transaction(uint64_t tx_id, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            table_data.insert(write_set.begin(), write_set.end());
            write_set.clear();
            current_tx_id = 0;
            return true;
        }

        bool rollback_transaction(uint64_t tx_id, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            write_set.clear();
            current_tx_id = 0;
            return true;
        }

        bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 std::string& value, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            
            // Handle one-shot operations (auto-commit)
            if (tx_id != 0 && current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            auto it = table_data.find(make_table_key(table_name, key));
            if (it == table_data.end()) {
                error_msg = "Key " + key + " not found";
                return false;
            }
            value = it->second;
            return true;
        }

        bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 const std::string& value, std::string& error_msg)
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            
            // Handle one-shot operations (auto-commit)
            if (tx_id != 0 && current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            write_set[make_table_key(table_name, key)] = value;
            return true;
        }

        bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
                 const std::string& key_end, size_t num_item_limit, 
                 std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            
            // Handle one-shot operations (auto-commit)
            if (tx_id == 0 && current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            
            results.clear();
            std::string table_key = make_table_key(table_name, key_start);
            std::string table_key_end = make_table_key(table_name, key_end);
            auto itr = table_data.lower_bound(table_key);
            auto end_itr = table_data.upper_bound(table_key_end);
            while (itr != end_itr && results.size() < num_item_limit) {
                results.emplace_back(itr->first, itr->second);
                ++itr;
            }
            return true;
        }
};


// class KVTManagerWrapperBase : public KVTManagerWrapperInterface
// {
//     protected;
//         struct Entry {
//             std::string data;
//             int32_t metadata; //for 2PL, it is the lock flag, for OCC, it is the version number. -1 means deleted. 
//         };

//         sturct Table {
//             uint64_t id;
//             std::string name;
//             std::string partition_method;  // "hash" or "range"
//             std::map<std::string, Entry> entries;
//         };

//         struct Transaction {
//             uint64_t tx_id;
//             std::map<std::string, Entry> read_set;    // table_key -> Value (for reads)
//             std::map<std::string, Entry> write_set;   // table_key -> Value (for writes)
//         };

//         std::unordered_map<std::string, std::unique_ptr<Table>> tables;
//         std::unordered_map<uint64_t, std::unique_ptr<Transaction>> transactions;
//         std::unordered_map<std::string, uint64_t> tablename_to_id;

//         std::mutex global_mutex;
//         uint64_t next_table_id;
//         uint64_t next_tx_id;

//         //helper
//         std::string make_table_key(const std::string& table_name, const std::string& key) {
//             return table_name + std::string(1, '\0') + key;
//         }

//     public:
//         KVTManagerWrapperBase()
//         {
//             next_table_id = 1;
//             next_tx_id = 1;
//         }

//         ~KVTManagerWrapperBase()
//         {
//         }
    
//         // Table management
//         uint64_t create_table(const std::string& table_name, const std::string& partition_method, std::string& error_msg)
//         {
//             std::lock_guard<std::mutex> lock(global_mutex);
//             if (tables.find(table_name) != tables.end()) {
//                 error_msg = "Table '" + table_name + "' already exists";
//                 return 0;
//             }
//             if (partition_method != "hash" && partition_method != "range") {
//                 error_msg = "Invalid partition method. Must be 'hash' or 'range'";
//                 return 0;
//             }
//             uint64_t table_id = next_table_id ++;
//             tables[table_name] = std::make_unique<Table>(table_name, partition_method, table_id);
//             tablename_to_id[table_name] = table_id;
//             return table_id;
            
//         }

//         uint64_t start_transaction(std::string& error_msg) {
//             std::lock_guard<std::mutex> lock(global_mutex);
//             uint64_t tx_id = next_tx_id ++;
//             transactions[tx_id] = std::make_unique<Transaction>(tx_id);
//             return tx_id;
//         }
        
//     };

// class KVTManagerWrapper2pl: public KVTManagerWrapperBase
// {
// public:    
//     // Transaction management
//     override bool commit_transaction(uint64_t tx_id, std::string& error_msg);
//     override bool rollback_transaction(uint64_t tx_id, std::string& error_msg);
//     override bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//              std::string& value, std::string& error_msg);
//     override bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//              const std::string& value, std::string& error_msg);
//     override bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
//               const std::string& key_end, size_t num_item_limit, 
//               std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg);
// };

// class KVTManagerWrapperOCC: public KVTManagerWrapperBase
// {
//   // Transaction management
//   override bool commit_transaction(uint64_t tx_id, std::string& error_msg);
//   override bool rollback_transaction(uint64_t tx_id, std::string& error_msg);
//   override bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//            std::string& value, std::string& error_msg);
//   override bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//            const std::string& value, std::string& error_msg);
//   override bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
//             const std::string& key_end, size_t num_item_limit, 
//             std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg);
//   };


typedef KVTManagerWrapperSimple KVTManagerWrapper;

#endif // KVT_MEM_H
