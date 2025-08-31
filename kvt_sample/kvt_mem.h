#ifndef KVT_MEM_H
#define KVT_MEM_H

#include <brpc/redis.h>
#include <sys/types.h>
#include <csetjmp>
#include <map>
#include <unordered_map>
#include <unordered_set>
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
        virtual bool del(uint64_t tx_id, const std::string& table_name, 
                const std::string& key, std::string& error_msg) = 0;
        virtual bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
                  const std::string& key_end, size_t num_item_limit, 
                  std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) = 0;
};
//No concurrency control, so transactions just commit immediately and can not be rolled back
//No isolation, read uncommitted.
class KVTManagerWrapperNoCC : public KVTManagerWrapperInterface
{
    private:
        std::map<std::string, std::string> table_data;
        std::unordered_map<std::string, uint32_t> table_to_id;
        uint64_t next_table_id;
        uint64_t next_tx_id;
        std::mutex global_mutex;

        std::string make_table_key(const std::string& table_name, const std::string& key) {
            assert(!table_name.empty() && table_name.find('\0') == std::string::npos);
            assert(!key.empty() && key.find('\0') == std::string::npos);
            return table_name + std::string(1, '\0') + key;
        }
        std::pair<std::string, std::string> parse_table_key(const std::string& table_key) {
            size_t separator_pos = table_key.find('\0');
            assert(separator_pos != std::string::npos);
            return std::make_pair(table_key.substr(0, separator_pos), table_key.substr(separator_pos + 1));
        }
    public:
        KVTManagerWrapperNoCC()
        {
            next_table_id = 1;
            next_tx_id = 1;
        }
        ~KVTManagerWrapperNoCC()
        {
        }
        // Table management
        virtual uint64_t create_table(const std::string& table_name, const std::string& partition_method, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (table_to_id.find(table_name) != table_to_id.end()) {
                error_msg = "Table " + table_name + " already exists";
                return 0;
            }
            table_to_id[table_name] = next_table_id;
            std::cout << "create_table " << table_name << " as TableID" << next_table_id << std::endl;
            next_table_id += 1;
            return next_table_id - 1;
        }
        virtual uint64_t start_transaction(std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            std::cout << "start_transaction " << next_tx_id << std::endl;
            next_tx_id += 1;
            return next_tx_id - 1;
        }
        virtual bool commit_transaction(uint64_t tx_id, std::string& error_msg) 
        {
            std::cout << "commit_transaction " << tx_id << std::endl;
            return true;
        }
        virtual bool rollback_transaction(uint64_t tx_id, std::string& error_msg) {
            std::cout << "rollback_transaction " << tx_id << std::endl;
            return true;
        }
        // Data operations  
        virtual bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 std::string& value, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tx_id >= next_tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }

            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            std::string table_key = make_table_key(table_name, key);
            auto it = table_data.find(table_key);
            if (it == table_data.end()) {
                error_msg = "Key " + key + " not found";
                return false;
            }
            value = it->second;
            std::cout << "get " << table_name << ":" << key << " = " << value << std::endl;
            return true;
        }

        virtual bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 const std::string& value, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tx_id >= next_tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            std::string table_key = make_table_key(table_name, key);
            table_data[table_key] = value;
            std::cout << "set " << table_name << ":" << key << " = " << value << std::endl;
            return true;

        }
        virtual bool del(uint64_t tx_id, const std::string& table_name, 
                const std::string& key, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tx_id >= next_tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            std::string table_key = make_table_key(table_name, key);
            if (table_data.find(table_key) == table_data.end()) {
                std::cout << "del " << table_name << ":" << key << " not found" << std::endl;
                error_msg = "Key " + key + " not found";
                return false;
            }
            else {
                table_data.erase(table_key);
                std::cout << "del " << table_name << ":" << key << std::endl;
                return true;
            }
        }

        virtual bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
                  const std::string& key_end, size_t num_item_limit, 
                  std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tx_id >= next_tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            std::string table_key = make_table_key(table_name, key_start);
            std::string table_key_end = make_table_key(table_name, key_end);
            auto itr = table_data.lower_bound(table_key);
            auto end_itr = table_data.upper_bound(table_key_end);
            while (itr != end_itr && results.size() < num_item_limit) { 
                results.emplace_back(parse_table_key(itr->first).second, itr->second);
                ++itr;
            }
            std::cout << "scan " << table_name << ":" << key_start << " to " << key_end << " = " << results.size() << " items" << std::endl;
            return true;
        }
};

//only allow a single transaction at a time, truely serializable. can be rolled back.
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
        std::pair<std::string, std::string> parse_table_key(const std::string& table_key) {
            size_t separator_pos = table_key.find('\0');
            assert(separator_pos != std::string::npos);
            return std::make_pair(table_key.substr(0, separator_pos), table_key.substr(separator_pos + 1));
        }


        std::unordered_map<std::string, std::string> write_set; 
        std::unordered_set<std::string> delete_set;

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
            return true;
        }

        bool rollback_transaction(uint64_t tx_id, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            write_set.clear();
            delete_set.clear();
            current_tx_id = 0;
            return true;
        }

        bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
                 std::string& value, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }

            //if tx_id is 0, it is a one-shot read operation, so we just get the data from the table.
            //otherwise, we only allow a single transaction at a time, so we check if the transaction id is the current one.
            if (tx_id != 0 && current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            std::string table_key = make_table_key(table_name, key);
            auto itr = write_set.find(table_key);
            if (itr != write_set.end()) {
                value = itr->second;
                return true;
            }
            auto itr1 = delete_set.find(table_key);
            if (itr1 != delete_set.end()) {
                error_msg = "Key " + key + " is deleted";
                return false;
            }
            auto it = table_data.find(table_key);
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
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            //we cannot allow one shot mutation operations when another transaction is running.
            //When current_tx_id is 0, (i.e. no ongoing transaction) we allow one shot mutation (which also have tx_id 0).
            //we only allow a single transaction at a time, so we check if the transaction id is the current one.
            if (current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            //the invariant is that when a key is deleted, it must not be in the write set. vice versa.
            std::string table_key = make_table_key(table_name, key);
            auto itr = delete_set.find(table_key);
            if (itr != delete_set.end()) 
                delete_set.erase(itr);
            write_set[table_key] = value;
            return true;
        }

        bool del(uint64_t tx_id, const std::string& table_name, const std::string& key, 
            std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            //we cannot allow one shot mutation operations when another transaction is running.
            //When current_tx_id is 0, (i.e. no ongoing transaction) we allow one shot mutation (which also have tx_id 0).
            //we only allow a single transaction at a time, so we check if the transaction id is the current one.
            if (current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            std::string table_key = make_table_key(table_name, key);
            //the invariant is that when a key is deleted, it must not be in the write set. vice versa.
            auto itr = write_set.find(table_key);
            if (itr != write_set.end()) {
                write_set.erase(itr);
                return true;
            }
            auto itr1 = table_data.find(table_key);
            if (itr1 == table_data.end()) {
                error_msg = "Key " + key + " not found, cannot be deleted";
                return false;
            }
            delete_set.insert(table_key);
            return true;
        }

        bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
                 const std::string& key_end, size_t num_item_limit, 
                 std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (table_to_id.find(table_name) == table_to_id.end()) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            //if tx_id is 0, it is a one-shot read operation, so we just get the data from the table.
            //otherwise, we only allow a single transaction at a time, so we check if the transaction id is the current one.
            if (tx_id != 0 && current_tx_id != tx_id) {
                error_msg = "Transaction " + std::to_string(tx_id) + " not found";
                return false;
            }
            
            results.clear();
            std::string table_key = make_table_key(table_name, key_start);
            std::string table_key_end = make_table_key(table_name, key_end);
            auto itr = table_data.lower_bound(table_key);
            auto end_itr = table_data.upper_bound(table_key_end);
            while (itr != end_itr && results.size() < num_item_limit) {
                std::pair<std::string, std::string> table_name_key = parse_table_key(itr->first);
                std::string & key = table_name_key.second;
                if (delete_set.find(itr->first) == delete_set.end()) {
                    if (write_set.find(itr->first) != write_set.end()) {
                        results.emplace_back(key, write_set[itr->first]);
                    }
                    else {
                        results.emplace_back(key, itr->second);
                    }
                }
                ++itr;
            }
            return true;
        }
};


class KVTManagerWrapperBase : public KVTManagerWrapperInterface
{
    protected;
        struct Entry {
            std::string data;
            int32_t metadata; //for 2PL, it is the lock flag, for OCC, it is the version number. -1 means deleted. 
        };

        sturct Table {
            uint64_t id;
            std::string name;
            std::string partition_method;  // "hash" or "range"
            std::map<std::string, Entry> entries;
        };

        struct Transaction {
            uint64_t tx_id;
            std::map<std::string, Entry> read_set;    // table_key -> Value (for reads)
            std::map<std::string, Entry> write_set;   // table_key -> Value (for writes)
            std::unordered_set<std::string> delete_set; // table_key -> deleted
        };

        std::unordered_map<std::string, std::unique_ptr<Table>> tables;
        std::unordered_map<uint64_t, std::unique_ptr<Transaction>> transactions;
        std::unordered_map<std::string, uint64_t> tablename_to_id;

        std::mutex global_mutex;
        uint64_t next_table_id;
        uint64_t next_tx_id;

        //helper
        std::string make_table_key(const std::string& table_name, const std::string& key) {
            assert(!table_name.empty() && table_name.find('\0') == std::string::npos);
            assert(!key.empty() && key.find('\0') == std::string::npos);
            return table_name + std::string(1, '\0') + key;
        }

        std::pair<std::string, std::string> parse_table_key(const std::string& table_key) {
            size_t separator_pos = table_key.find('\0');
            assert(separator_pos != std::string::npos);
            return std::make_pair(table_key.substr(0, separator_pos), table_key.substr(separator_pos + 1));
        }

        Table* get_table(const std::string& table_name) {
            auto it = tables.find(table_name);
            if (it == tables.end()) {
                return nullptr;
            }
            return it->second.get();
        }

        Transaction* get_transaction(uint64_t tx_id) {
            auto it = transactions.find(tx_id);
            if (it == transactions.end()) {
                return nullptr;
            }
            return it->second.get();
        }

    public:
        KVTManagerWrapperBase()
        {
            next_table_id = 1;
            next_tx_id = 1;
        }

        ~KVTManagerWrapperBase()
        {
        }
    
        // Table management
        uint64_t create_table(const std::string& table_name, const std::string& partition_method, std::string& error_msg)
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            if (tables.find(table_name) != tables.end()) {
                error_msg = "Table '" + table_name + "' already exists";
                return 0;
            }
            if (partition_method != "hash" && partition_method != "range") {
                error_msg = "Invalid partition method. Must be 'hash' or 'range'";
                return 0;
            }
            uint64_t table_id = next_table_id ++;
            tables[table_name] = std::make_unique<Table>(table_name, partition_method, table_id);
            tablename_to_id[table_name] = table_id;
            return table_id;
            
        }

        uint64_t start_transaction(std::string& error_msg) {
            std::lock_guard<std::mutex> lock(global_mutex);
            uint64_t tx_id = next_tx_id ++;
            transactions[tx_id] = std::make_unique<Transaction>(tx_id);
            return tx_id;
        }
        
    };

// class KVTManagerWrapper2pl: public KVTManagerWrapperBase
// {
//     //For table, the meta data in an entry is locked (1) or not (0);
//     //For transaction context, the metadata in an entry is self-created (1) or alread existed (0)
// public:    
//     // Transaction management
//     override bool commit_transaction(uint64_t tx_id, std::string& error_msg) {
//         std::lock_guard<std::mutex> lock(global_mutex);
//         if (tx_id == 0) {
//             error_msg = "Transaction " + std::to_string(tx_id) + " not found";
//             return false;
//         }
//         Transaction* tx = get_transaction(tx_id);
//         if (!tx) {
//             error_msg = "Transaction " + std::to_string(tx_id) + " not found";
//             return false;
//         }

//         for (const auto& write_pair : tx->write_set) {
//             std::pair<std::string, std::string> table_name_key = parse_table_key(write_pair.first);
//             Table* table = get_table(table_name_key.first);
//             if (!table) {
//                 error_msg = "Table " + table_name_key.first + " not found";
//                 return false;
//             }
//             //I must already hold the lock. so I can directly update the table and release the lock
//             table->data[table_name_key.second] = write_pair.second.data;
//             assert(table->data[table_name_key.second].is_locked);
//             table->data[table_name_key.second].is_locked = false;
//         }
//         for (const auto& read_pair : tx->read_set) {
//             std::pair<std::string, std::string> table_name_key = parse_table_key(read_pair.first);
//             Table* table = get_table(table_name_key.first);
//             if (!table) {
//                 error_msg = "Table " + table_name_key.first + " not found";
//                 return false;
//             }
//             assert(table->data[table_name_key.second].is_locked);
//             table->data[table_name_key.second].is_locked = false;
//         }
//         for (const auto& delete_pair : tx->delete_set) {
//             std::pair<std::string, std::string> table_name_key = parse_table_key(delete_pair);
//             Table* table = get_table(table_name_key.first);
//             if (!table) {
//                 error_msg = "Table " + table_name_key.first + " not found";
//                 return false;
//             }
//             assert (table->data[table_name_key.second].is_locked); //I must have the lock when I delete
//             table->data.erase(table_name_key.second);
//         }
//         transactions.erase(tx_id);
//         return true;
//     }

//     override bool rollback_transaction(uint64_t tx_id, std::string& error_msg) 
//     {
//         std::lock_guard<std::mutex> lock(global_mutex);
//         if (tx_id == 0) {
//             error_msg = "Transaction " + std::to_string(tx_id) + " not found";
//             return false;
//         }
//         Transaction* tx = get_transaction(tx_id);
//         if (!tx) {
//             error_msg = "Transaction " + std::to_string(tx_id) + " not found";
//             return false;
//         }
//         for (const auto& read_pair : tx->read_set) {
//             std::pair<std::string, std::string> table_name_key = parse_table_key(read_pair.first);
//             Table* table = get_table(table_name_key.first);
//             if (!table) {
//                 error_msg = "Table " + table_name_key.first + " not found";
//                 return false;
//             }
//             assert(table->data[table_name_key.second].is_locked);
//             table->data[table_name_key.second].is_locked = false;
//         }
//         for (const auto& write_pair : tx->write_set) {
//             std::pair<std::string, std::string> table_name_key = parse_table_key(write_pair.first);
//             Table* table = get_table(table_name_key.first);
//             if (!table) {
//                 error_msg = "Table " + table_name_key.first + " not found";
//                 return false;
//             }
//             assert(table->data[table_name_key.second].is_locked);
//             table->data[table_name_key.second].is_locked = false;
//         }

//         for (const auto& delete_pair : tx->delete_set) {
//             std::pair<std::string, std::string> table_name_key = parse_table_key(delete_pair);
//             Table* table = get_table(table_name_key.first);
//             if (!table) {
//                 error_msg = "Table " + table_name_key.first + " not found";
//                 return false;
//             }
//             assert(table->data[table_name_key.second].is_locked);
//             table->data[table_name_key.second].is_locked = false;
//         }
//         transactions.erase(tx_id);
//         return true;
//     }

    
//     override bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//              std::string& value, std::string& error_msg)
//     {
//         std::lock_guard<std::mutex> lock(global_mutex);
//         if (tx_id == 0) {
//             Table* table = get_table(table_name);
//             if (!table) {
//                 error_msg = "Table " + table_name + " not found";
//                 return false;
//             }
//             auto it = table->data.find(key);
//             if (it == table->data.end()) {
//                 error_msg = "Key " + key + " not found";
//                 return false;
//             }
//             if (it->second.is_locked) {
//                 error_msg = "Key " + key + " is locked by another transaction";
//                 return false;
//             }
//             value = it->second.data;
//             return true;
//         }
//         Transaction* tx = get_transaction(tx_id);
//         if (!tx) {
//             error_msg = "Transaction " + std::to_string(tx_id) + " not found";
//             return false;
//         }
//         std::string table_key = make_table_key(table_name, key);
//         if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
//             error_msg = "Key " + key + " is deleted";
//             return false;
//         }
//         auto write_itr = tx->write_set.find(table_key);
//         if (write_itr != tx->write_set.end()) {
//             value = write_itr->second.data;
//             return true;
//         }
//         auto read_itr = tx->read_set.find(table_key);
//         if (read_itr != tx->read_set.end()) {
//             value = read_itr->second.data;
//             return true;
//         }
//         Table* table = get_table(table_name);
//         if (!table) {
//             error_msg = "Table " + table_name + " not found";
//             return false;
//         }
//         auto it = table->data.find(key);
//         if (it == table->data.end()) {
//             error_msg = "Key " + key + " not found";
//             return false;
//         }
//         if (it->second.is_locked) {
//             error_msg = "Key " + key + " is locked by another transaction";
//             return false;
//         }
//         it->second.is_locked = true;
//         tx->read_set[table_key] = it->second;
//         value = it->second.data;
//         return true;
//     }
    
//     override bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//              const std::string& value, std::string& error_msg)
//     {
//         std::lock_guard<std::mutex> lock(global_mutex);
//         if (tx_id == 0) {
//             Table* table = get_table(table_name);
//             if (!table) {
//                 error_msg = "Table " + table_name + " not found";
//                 return false;
//             }
//             auto it = table->data.find(key);
//             if (it != table->data.end() && it->second.is_locked) {
//                 error_msg = "Key " + key + " is locked by another transaction";
//                 return false;
//             }
//             table->data[key] = Value(value, false);
//             return true;
//         }
//         Transaction* tx = get_transaction(tx_id);
//         if (!tx) {
//             error_msg = "Transaction " + std::to_string(tx_id) + " not found";
//             return false;
//         }
//         std::string table_key = make_table_key(table_name, key);
//         if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
//             tx->delete_set.erase(table_key);
//         }
//         if (tx->write_set.find(table_key) != tx->write_set.end()) {
//             tx->write_set[table_key].data = value;
//         }
//         else {
//             Table *table = get_table(table_name);
//             if (!table) {
//                 error_msg = "Table " + table_name + " not found";
//                 return false;
//             }
//             if (table->data.find(key) != table->data.end() && table->data[key].is_locked) {
//                 error_msg = "Key " + key + " is locked by another transaction";
//                 return false;
//             }
//             table->data[key] = Value(value, false);
//             return true;
//         }
//     }
//     override bool del(uint64_t tx_id, const std::string& table_name, const std::string& key, 
//             std::string& error_msg);
//     override bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
//               const std::string& key_end, size_t num_item_limit, 
//               std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg);
// };

class KVTManagerWrapperOCC: public KVTManagerWrapperBase
{
    //for OCC, the metadata in an entry is the version number.
    //delete must be put into the read set so that it can keep version.
    //in this case, when a delete happens, it needs to be removed from write

    //Important Invariance: 
    //1. a key cannot appear in both write set and delete set. 
    //2. a deleted key must be in read set, if it does not already in write set (and then need to be removed)

public:
  // Transaction management
  override bool commit_transaction(uint64_t tx_id, std::string& error_msg)
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        //first check if the readset versions are still valid
        for (const auto& read_pair : tx->read_set) {
            auto [table_name, key] = parse_table_key(read_pair.first);
            Table* table = get_table(table_name);
            assert(table);

            uint64_t local_version = read_pair.second.metadata;

            if (table->data.find(key) == table->data.end() || //being deleted by another transaction
                table->data[key].version > local_version) {  //being written by another transaction
                error_msg = "Transaction " + std::to_string(tx_id) + " has stale data";
                transactions.erase(tx_id); //baskc
                return false;
            }
        }
        //now all readset versions are valid, so we can install the new values
        for (const auto& delete_item : tx->delete_set) {
            auto [table_name, key] = parse_table_key(delete_item);
            Table* table = get_table(table_name);
            assert(table);
            auto itr = table->data.find(key);
            assert (itr != table->data.end());
            table->data.erase(itr);
        }
        for (const auto& write_pair : tx->write_set) {
            auto [table_name, key] = parse_table_key(write_pair.first);
            Table* table = get_table(table_name);
            assert(table);
            uint64_t new_version = (table->data.find(key) != table->data.end()) ? table->data[key].version + 1 : 1;
            table->data[key] = Value(write_pair.second.data, new_version);
        }
        transactions.erase(tx_id);
        return true;
    }
            
  override bool rollback_transaction(uint64_t tx_id, std::string& error_msg)
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        transactions.erase(tx_id);
        return true;
    }
  override bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
           std::string& value, std::string& error_msg)
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        //one shot transaction is only allowed for read only transaction.
        if (tx_id == 0) {
            Table* table = get_table(table_name);
            if (!table) {
                error_msg = "Table " + table_name + " not found";
                return false;
            }
            auto it = table->data.find(key);
            if (it == table->data.end()) {
                error_msg = "Key " + key + " not found";
                return false;
            }
            value = it->second.data;
            return true;
        }
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        std::string table_key = make_table_key(table_name, key);
        if (tx->write_set.find(table_key) != tx->write_set.end()) {
            value = tx->write_set[table_key].data;
            return true;
        }
        if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
            error_msg = "Key " + key + " is deleted";
            return false;
        }
        if (tx->read_set.find(table_key) != tx->read_set.end()) {
            value = tx->read_set[table_key].data;
            return true;
        }
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table " + table_name + " not found";
            return false;
        }
        if (table->data.find(key) == table->data.end()) {
            error_msg = "Key " + key + " not found";
            return false;
        }
        tx->read_set[table_key] = table->data[key];
        value = table->data[key].data;
        return true;
    }

  override bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
           const std::string& value, std::string& error_msg)
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        if (tx_id == 0) {
            error_msg = "One shot transaction is not allowed for write operations";
            return false;
        }
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        std::string table_key = make_table_key(table_name, key);
        tx->write_set[table_key] = Value(value, 0); //no need to track metadata for write set
        auto itr = tx->delete_set.find(table_key);
        if (itr != tx->delete_set.end()) {
            tx->delete_set.erase(itr); //this is opitonal, as we install deletes first then writes
        }
        return true;
    }
    override bool del(uint64_t tx_id, const std::string& table_name, const std::string& key, std::string& error_msg)
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        if (tx_id == 0) {
            error_msg = "One shot transaction is not allowed for delete operations";
            return false;
        }
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        std::string table_key = make_table_key(table_name, key);
        auto itr = tx->write_set.find(table_key);
        if (itr != tx->write_set.end()) {
            tx->write_set.erase(itr); //delete after write, so not necessarily read from table. 
        }
        else {
            if (tx->read_set.find(table_key) == tx->read_set.end()) { //not in the read set, so need to read from table.
                Table* table = get_table(table_name);
                if (!table) {
                    error_msg = "Table " + table_name + " not found";
                    return false;
                }
                if (table->data.find(key) == table->data.end()) {
                    error_msg = "Key " + key + " not found, cannot be deleted";
                    return false;
                }
                tx->read_set[table_key] = table->data[key];
            }
        }
        tx->delete_set.insert(table_key);
        return true;
    }


  override bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
            const std::string& key_end, size_t num_item_limit, 
            std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg);
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table " + table_name + " not found";
            return false;
        }
        if (tx_id == 0) {
            for (auto itr = table->data.lower_bound(key_start); itr != table->data.end() && itr->first < key_end; ++itr) {
                results.emplace_back(itr->first, itr->second.data);
                if (results.size() >= num_item_limit) {
                    break;
                }
            }
            return true;
        }
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        std::map<std::string, std::string> results_writes;
        {
            std::string table_key_start = make_table_key(table_name, key_start);
            std::string table_key_end = make_table_key(table_name, key_end);
            //first put all write_set into results
            for (auto itr = tx->write_set.lower_bound(table_key_start); itr != tx->write_set.end() && itr->first < table_key_end; ++itr) {
                auto [table_name, key] = parse_table_key(itr->first);
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
            if (tx->delete_set.find(itr->first) != tx->delete_set.end()) //being deleted, skip
                continue;
            if (tx->read_set.find(itr->first) == tx->read_set.end()) { //not in the read set, so need to read from table.
                tx->read_set[itr->first] = itr->second;
                results[itr->first] = itr->second.data;
            } else {
                if (tx->read_set[itr->first].data != itr->second.data) {
                    assert (tx->read_set[itr->first].metadata < itr->second.metadata);
                }
                results[itr->first] = tx->read_set[itr->first].data; //should be the same, if not, then we will abort anyway.
            }
            if (results_table.size() >= num_item_limit) {
                break;
            }
        }
        //now merge the results
        results_table.insert(results_writes.begin(), results_writes.end());
        for (const auto& scan_pair : results_table) {
            results.emplace_back(scan_pair);
            if (results.size() >= num_item_limit) {
                break;
            }
        }
        return true;
    }
  };


typedef KVTManagerWrapperSimple KVTManagerWrapper;

#endif // KVT_MEM_H
