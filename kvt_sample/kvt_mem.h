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
            
            // First, scan table_data for existing keys
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
            
            // CRITICAL FIX: Also include NEW keys from write_set that aren't in table_data yet
            for (const auto& [ws_key, ws_value] : write_set) {
                if (ws_key >= table_key && ws_key < table_key_end) {
                    // Check if this key is not already in results (from table_data scan)
                    if (table_data.find(ws_key) == table_data.end()) {
                        auto [ws_table_name, ws_key_only] = parse_table_key(ws_key);
                        if (ws_table_name == table_name) {
                            results.emplace_back(ws_key_only, ws_value);
                            if (results.size() >= num_item_limit) break;
                        }
                    }
                }
            }
            
            // Sort results by key since we added write_set items separately
            std::sort(results.begin(), results.end(), 
                     [](const auto& a, const auto& b) { return a.first < b.first; });
            
            // Trim to limit if needed
            if (results.size() > num_item_limit) {
                results.resize(num_item_limit);
            }
            
            return true;
        }
};


class KVTManagerWrapperBase : public KVTManagerWrapperInterface
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
        
        // DEBUG: Intrusive consistency check - directly accesses internal data
        bool debug_check_consistency(const std::string& table_name, const std::string& context) {
            std::lock_guard<std::mutex> lock(global_mutex);
            std::cout << "\n=== DEBUG CONSISTENCY CHECK [" << context << "] ===" << std::endl;
            
            Table* table = get_table(table_name);
            if (!table) {
                std::cout << "Table not found: " << table_name << std::endl;
                return false;
            }
            
            // Group keys by range (0-99, 100-199, etc.)
            std::map<int, std::vector<std::pair<int, int>>> ranges;  // range_start -> [(key, value)]
            
            for (const auto& [key_str, entry] : table->data) {
                if (entry.metadata == -1) continue;  // Skip deleted entries
                
                try {
                    int key = std::stoi(key_str);
                    int value = std::stoi(entry.data);
                    int range_start = (key / 100) * 100;
                    ranges[range_start].emplace_back(key, value);
                    
                    // Debug: show first few entries
                    static int debug_count = 0;
                    if (debug_count++ < 10) {
                        std::cout << "  Key " << key << " -> Value " << value 
                                  << " (metadata=" << entry.metadata << ")" << std::endl;
                    }
                } catch (...) {
                    std::cout << "Non-numeric key/value: " << key_str << " -> " << entry.data << std::endl;
                }
            }
            
            bool all_valid = true;
            for (const auto& [range_start, kvs] : ranges) {
                int sum = 0;
                for (const auto& [k, v] : kvs) {
                    sum += v;
                }
                
                std::cout << "Range [" << range_start << "-" << (range_start + 99) << "]: "
                          << kvs.size() << " keys, sum=" << sum;
                
                if (sum % 100 != 0) {
                    std::cout << " *** VIOLATION! Not divisible by 100 ***" << std::endl;
                    all_valid = false;
                    
                    // Show all keys in this range for debugging
                    std::cout << "  Keys in range: ";
                    for (const auto& [k, v] : kvs) {
                        std::cout << k << "=" << v << " ";
                    }
                    std::cout << std::endl;
                } else {
                    std::cout << " OK" << std::endl;
                }
            }
            
            std::cout << "Total ranges with data: " << ranges.size() << std::endl;
            std::cout << "Total keys in table: " << table->data.size() << std::endl;
            std::cout << "=== END CONSISTENCY CHECK ===" << std::endl << std::endl;
            
            return all_valid;
        }
        
    };

class KVTManagerWrapper2PL: public KVTManagerWrapperBase
{
    // For table entries: metadata field stores the locking transaction ID (0 = unlocked)
    // When a transaction acquires a lock, it sets metadata to its tx_id
public:
    // Transaction management
    bool commit_transaction(uint64_t tx_id, std::string& error_msg) override {
        std::lock_guard<std::mutex> lock(global_mutex);
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        // DEBUG: Show what we're committing
        std::cout << "\n==== DEBUG 2PL TX" << tx_id << " COMMIT START ====" << std::endl;
        std::cout << "Write set size: " << tx->write_set.size() << std::endl;
        std::cout << "Delete set size: " << tx->delete_set.size() << std::endl;
        std::cout << "Read set size: " << tx->read_set.size() << std::endl;
        
        // Show a sample of writes
        int write_count = 0;
        for (const auto& [write_key, entry] : tx->write_set) {
            if (write_count++ < 10) {
                auto [table_name, key] = parse_table_key(write_key);
                std::cout << "  Write: key=" << key << " value=" << entry.data 
                          << " metadata=" << entry.metadata << std::endl;
            }
        }
        
        // Apply deletes first
        for (const auto& delete_key : tx->delete_set) {
            auto [table_name, key] = parse_table_key(delete_key);
            Table* table = get_table(table_name);
            if (table) {
                auto it = table->data.find(key);
                if (it != table->data.end()) {
                    // Verify we hold the lock
                    assert(it->second.metadata == static_cast<int32_t>(tx_id));
                    std::cout << "  DEBUG: Deleting key=" << key << " old_value=" << it->second.data << std::endl;
                    table->data.erase(it);
                }
            }
        }
        
        // Apply writes
        for (const auto& [write_key, entry] : tx->write_set) {
            auto [table_name, key] = parse_table_key(write_key);
            Table* table = get_table(table_name);
            if (table) {
                auto it = table->data.find(key);
                if (it != table->data.end()) {
                    // Verify we hold the lock
                    assert(it->second.metadata == static_cast<int32_t>(tx_id));
                    std::string old_value = it->second.data;
                    // Install the write and release the lock
                    it->second.data = entry.data;
                    it->second.metadata = 0;  // Release lock
                    
                    // DEBUG
                    static int debug_write_count = 0;
                    if (debug_write_count++ < 20) {
                        std::cout << "  DEBUG: Writing key=" << key << " old=" << old_value 
                                  << " new=" << entry.data << std::endl;
                    }
                } else {
                    // This shouldn't happen - we should have created a placeholder
                    // But handle it just in case
                    std::cout << "  DEBUG: Creating new entry key=" << key << " value=" << entry.data << std::endl;
                    table->data[key] = Entry(entry.data, 0);
                }
            }
        }
        
        // Release read locks
        int read_lock_releases = 0;
        for (const auto& [read_key, entry] : tx->read_set) {
            // Skip if this key was also written or deleted
            if (tx->write_set.find(read_key) != tx->write_set.end() ||
                tx->delete_set.find(read_key) != tx->delete_set.end()) {
                continue;
            }
            
            auto [table_name, key] = parse_table_key(read_key);
            Table* table = get_table(table_name);
            if (table) {
                auto it = table->data.find(key);
                if (it != table->data.end()) {
                    assert(it->second.metadata == static_cast<int32_t>(tx_id));
                    it->second.metadata = 0;  // Release the lock
                    read_lock_releases++;
                }
            }
        }
        
        std::cout << "Released " << read_lock_releases << " read-only locks" << std::endl;
        std::cout << "==== DEBUG 2PL TX" << tx_id << " COMMIT END ====" << std::endl;
        
        transactions.erase(tx_id);
        return true;
    }
    
    bool rollback_transaction(uint64_t tx_id, std::string& error_msg) override {
        std::lock_guard<std::mutex> lock(global_mutex);
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        // Release all locks held by this transaction
        // Release write locks (for keys that were to be written but not yet)
        for (const auto& [write_key, entry] : tx->write_set) {
            auto [table_name, key] = parse_table_key(write_key);
            Table* table = get_table(table_name);
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
            auto [table_name, key] = parse_table_key(read_key);
            Table* table = get_table(table_name);
            if (table) {
                auto it = table->data.find(key);
                if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                    it->second.metadata = 0;  // Release the lock
                }
            }
        }
        
        // Release delete locks
        for (const auto& delete_key : tx->delete_set) {
            auto [table_name, key] = parse_table_key(delete_key);
            Table* table = get_table(table_name);
            if (table) {
                auto it = table->data.find(key);
                if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                    it->second.metadata = 0;  // Release the lock
                }
            }
        }
        
        transactions.erase(tx_id);
        return true;
    }
    
    bool get(uint64_t tx_id, const std::string& table_name, const std::string& key,
             std::string& value, std::string& error_msg) override {
        std::lock_guard<std::mutex> lock(global_mutex);
        
        // One-shot read
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
            if (it->second.metadata != 0) {
                error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
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
        
        // Check if deleted in this transaction
        if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
            error_msg = "Key " + key + " is deleted";
            return false;
        }
        
        // Check write set
        auto write_it = tx->write_set.find(table_key);
        if (write_it != tx->write_set.end()) {
            value = write_it->second.data;
            return true;
        }
        
        // Check read set
        auto read_it = tx->read_set.find(table_key);
        if (read_it != tx->read_set.end()) {
            value = read_it->second.data;
            return true;
        }
        
        // Need to read from table and acquire lock
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
        
        // Check if locked by another transaction
        if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
            error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
            return false;
        }
        
        // Acquire lock
        it->second.metadata = static_cast<int32_t>(tx_id);
        tx->read_set[table_key] = it->second;
        value = it->second.data;
        return true;
    }
    
    bool set(uint64_t tx_id, const std::string& table_name, const std::string& key,
             const std::string& value, std::string& error_msg) override {
        std::lock_guard<std::mutex> lock(global_mutex);
        
        if (tx_id == 0) {
            error_msg = "One-shot transactions not allowed for write operations";
            return false;
        }
        
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        std::string table_key = make_table_key(table_name, key);
        
        // Remove from delete set if it was there
        tx->delete_set.erase(table_key);
        
        // Check if we already have it in write set
        if (tx->write_set.find(table_key) != tx->write_set.end()) {
            tx->write_set[table_key].data = value;
            return true;
        }
        
        // Need to acquire lock if we don't already have it
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table " + table_name + " not found";
            return false;
        }
        
        auto it = table->data.find(key);
        bool key_exists = (it != table->data.end());
        
        // If key exists, check if it's locked
        if (key_exists) {
            if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
                error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
                return false;
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
        return true;
    }
    
    bool del(uint64_t tx_id, const std::string& table_name, const std::string& key,
             std::string& error_msg) override {
        std::lock_guard<std::mutex> lock(global_mutex);
        
        if (tx_id == 0) {
            error_msg = "One-shot transactions not allowed for delete operations";
            return false;
        }
        
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        std::string table_key = make_table_key(table_name, key);
        
        // Remove from write set if it was there
        auto write_it = tx->write_set.find(table_key);
        if (write_it != tx->write_set.end()) {
            // If it was a new key we were going to add, just remove it
            if (write_it->second.metadata == 1) {
                // Release the lock on the placeholder
                Table* table = get_table(table_name);
                if (table) {
                    auto it = table->data.find(key);
                    if (it != table->data.end() && it->second.metadata == static_cast<int32_t>(tx_id)) {
                        table->data.erase(it);
                    }
                }
                tx->write_set.erase(write_it);
                return true;
            }
            tx->write_set.erase(write_it);
        }
        
        // Need to acquire lock on the key to delete
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
        
        // Check if locked by another transaction
        if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
            error_msg = "Key " + key + " is locked by transaction " + std::to_string(it->second.metadata);
            return false;
        }
        
        // Acquire lock
        it->second.metadata = static_cast<int32_t>(tx_id);
        
        // Add to read set (to track the lock) and delete set
        if (tx->read_set.find(table_key) == tx->read_set.end()) {
            tx->read_set[table_key] = it->second;
        }
        tx->delete_set.insert(table_key);
        return true;
    }
    
    bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start,
              const std::string& key_end, size_t num_item_limit,
              std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) override {
        std::lock_guard<std::mutex> lock(global_mutex);
        
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table " + table_name + " not found";
            return false;
        }
        
        results.clear();
        
        // DEBUG
        static int scan_count = 0;
        if (scan_count++ < 5) {
            std::cout << "DEBUG SCAN TX" << tx_id << " range [" << key_start << "-" << key_end 
                      << "] limit=" << num_item_limit << std::endl;
            int count = 0;
            for (auto it = table->data.lower_bound(key_start);
                 it != table->data.end() && it->first <= key_end && count++ < 10;
                 ++it) {
                std::cout << "  Key=" << it->first << " metadata=" << it->second.metadata 
                          << " value=" << it->second.data << std::endl;
            }
        }
        
        // One-shot scan
        if (tx_id == 0) {
            for (auto it = table->data.lower_bound(key_start); 
                 it != table->data.end() && it->first <= key_end && results.size() < num_item_limit;
                 ++it) {
                if (it->second.metadata != 0) {
                    continue;  // Skip locked keys
                }
                results.emplace_back(it->first, it->second.data);
            }
            return true;
        }
        
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        // Collect results from write set and table
        std::map<std::string, std::string> temp_results;
        
        // First add from write set
        std::string table_key_start = make_table_key(table_name, key_start);
        std::string table_key_end = make_table_key(table_name, key_end);
        
        for (auto it = tx->write_set.lower_bound(table_key_start);
             it != tx->write_set.end() && it->first < table_key_end;
             ++it) {
            auto [tbl_name, key] = parse_table_key(it->first);
            if (tbl_name == table_name) {
                temp_results[key] = it->second.data;
            }
        }
        
        // Then scan table
        for (auto it = table->data.lower_bound(key_start);
             it != table->data.end() && it->first <= key_end;
             ++it) {
            std::string table_key = make_table_key(table_name, it->first);
            
            // Skip if deleted
            if (tx->delete_set.find(table_key) != tx->delete_set.end()) {
                continue;
            }
            
            // Skip if already in write set
            if (temp_results.find(it->first) != temp_results.end()) {
                continue;
            }
            
            // Check if locked by another transaction
            if (it->second.metadata != 0 && it->second.metadata != static_cast<int32_t>(tx_id)) {
                error_msg = "Key " + it->first + " is locked by transaction " + std::to_string(it->second.metadata);
                return false;
            }
            
            // Acquire lock if not already held
            if (it->second.metadata == 0) {
                it->second.metadata = static_cast<int32_t>(tx_id);
                tx->read_set[table_key] = it->second;
            }
            
            temp_results[it->first] = it->second.data;
        }
        
        // Convert to results vector
        for (const auto& [key, value] : temp_results) {
            results.emplace_back(key, value);
            if (results.size() >= num_item_limit) {
                break;
            }
        }
        
        return true;
    }
};




    
    

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
  bool commit_transaction(uint64_t tx_id, std::string& error_msg) override
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        Transaction* tx = get_transaction(tx_id);
        if (!tx) {
            error_msg = "Transaction " + std::to_string(tx_id) + " not found";
            return false;
        }
        
        // DEBUG: Show what we're committing
        std::cout << "\n==== DEBUG OCC TX" << tx_id << " COMMIT START ====" << std::endl;
        std::cout << "Write set size: " << tx->write_set.size() << std::endl;
        std::cout << "Delete set size: " << tx->delete_set.size() << std::endl;
        std::cout << "Read set size: " << tx->read_set.size() << std::endl;
        
        // Show a sample of writes
        int write_count = 0;
        for (const auto& [write_key, entry] : tx->write_set) {
            if (write_count++ < 10) {
                auto [table_name, key] = parse_table_key(write_key);
                std::cout << "  Write: key=" << key << " value=" << entry.data << std::endl;
            }
        }
        
        //first check if the readset versions are still valid
        for (const auto& read_pair : tx->read_set) {
            auto [table_name, key] = parse_table_key(read_pair.first);
            Table* table = get_table(table_name);
            assert(table);

            uint64_t local_version = read_pair.second.metadata;

            if (table->data.find(key) == table->data.end() || //being deleted by another transaction
                table->data[key].metadata > local_version) {  //being written by another transaction
                error_msg = "Transaction " + std::to_string(tx_id) + " has stale data";
                transactions.erase(tx_id);
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
            std::cout << "  DEBUG: Deleting key=" << key << " old_value=" << itr->second.data << std::endl;
            table->data.erase(itr);
        }
        for (const auto& write_pair : tx->write_set) {
            auto [table_name, key] = parse_table_key(write_pair.first);
            Table* table = get_table(table_name);
            assert(table);
            
            std::string old_value = (table->data.find(key) != table->data.end()) ? table->data[key].data : "NEW";
            uint64_t new_version = (table->data.find(key) != table->data.end()) ? table->data[key].metadata + 1 : 1;
            table->data[key] = Entry(write_pair.second.data, static_cast<int32_t>(new_version));
            
            // DEBUG
            static int debug_write_count = 0;
            if (debug_write_count++ < 20) {
                std::cout << "  DEBUG: Writing key=" << key << " old=" << old_value 
                          << " new=" << write_pair.second.data << " version=" << new_version << std::endl;
            }
        }
        
        std::cout << "==== DEBUG OCC TX" << tx_id << " COMMIT END ====" << std::endl;
        
        transactions.erase(tx_id);
        return true;
    }
            
  bool rollback_transaction(uint64_t tx_id, std::string& error_msg) override
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
  bool get(uint64_t tx_id, const std::string& table_name, const std::string& key, 
           std::string& value, std::string& error_msg) override
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

  bool set(uint64_t tx_id, const std::string& table_name, const std::string& key, 
           const std::string& value, std::string& error_msg) override
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
        tx->write_set[table_key] = Entry(value, 0); //no need to track metadata for write set
        auto itr = tx->delete_set.find(table_key);
        if (itr != tx->delete_set.end()) {
            tx->delete_set.erase(itr); //this is opitonal, as we install deletes first then writes
        }
        return true;
    }
    bool del(uint64_t tx_id, const std::string& table_name, const std::string& key, std::string& error_msg) override
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


  bool scan(uint64_t tx_id, const std::string& table_name, const std::string& key_start, 
            const std::string& key_end, size_t num_item_limit, 
            std::vector<std::pair<std::string, std::string>>& results, std::string& error_msg) override
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        Table* table = get_table(table_name);
        if (!table) {
            error_msg = "Table " + table_name + " not found";
            return false;
        }
        if (tx_id == 0) {
            for (auto itr = table->data.lower_bound(key_start); itr != table->data.end() && itr->first <= key_end; ++itr) {
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
            for (auto itr = tx->write_set.lower_bound(table_key_start); itr != tx->write_set.end() && itr->first <= table_key_end; ++itr) {
                auto [table_name, key] = parse_table_key(itr->first);
                results_writes[key] = itr->second.data;
                if (results_writes.size() >= num_item_limit) {
                    break;
                }
            }
        }
        std::map<std::string, std::string> results_table;
        //now collect from table, put into read_set if necessary
        for (auto itr = table->data.lower_bound(key_start); itr != table->data.end() && itr->first <= key_end; ++itr) {
            if (results_writes.find(itr->first) != results_writes.end()) //already in write set, skip
                continue;
            std::string table_key = make_table_key(table_name, itr->first);
            if (tx->delete_set.find(table_key) != tx->delete_set.end()) //being deleted, skip
                continue;
            if (tx->read_set.find(table_key) == tx->read_set.end()) { //not in the read set, so need to read from table.
                tx->read_set[table_key] = itr->second;
                results_table[itr->first] = itr->second.data;
            } else {
                if (tx->read_set[table_key].data != itr->second.data) {
                    assert (tx->read_set[table_key].metadata < itr->second.metadata);
                }
                results_table[itr->first] = tx->read_set[table_key].data; //should be the same, if not, then we will abort anyway.
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
