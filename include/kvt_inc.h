#ifndef KVT_INC_H
#define KVT_INC_H

#include <string>
#include <vector>
#include <utility>
#include <cstdint>
#include <functional>

class KVTKey : public std::string {
public:
    KVTKey() : std::string() {}
    KVTKey(const char* s) : std::string(s) {}
    KVTKey(const std::string& s) : std::string(s) {}
    KVTKey(std::string&& s) : std::string(std::move(s)) {}
    template<typename InputIterator> KVTKey(InputIterator first, InputIterator last) : std::string(first, last) {}
    KVTKey& operator=(const char* s) { std::string::operator=(s); return *this; }
    KVTKey& operator=(const std::string& s) { std::string::operator=(s); return *this; }
    KVTKey& operator=(std::string&& s) { std::string::operator=(std::move(s)); return *this; }
    bool operator<=(const KVTKey& other) const { return *this < other || *this == other; }
    bool operator>(const KVTKey& other) const { return other < *this; }
    bool operator>=(const KVTKey& other) const { return other < *this || *this == other; }
    bool operator==(const KVTKey& other) const { return static_cast<const std::string&>(*this) == static_cast<const std::string&>(other); }
    bool operator!=(const KVTKey& other) const { return !(*this == other); }

    //these are for scan operations
    //key cannot be null, so 0x00 is the smallest key possible
    //We do not allow empty string as key, so we use empty string as the largest key possible
    static KVTKey minimum_key() { return KVTKey(std::string(1, '\0'));}
    static KVTKey maximum_key() { return KVTKey("");}
    bool operator<(const KVTKey& other) const { 
        if (empty() && !other.empty()) return false;  // empty is larger
        if (!empty() && other.empty()) return true;   // other is larger
        return static_cast<const std::string&>(*this) < static_cast<const std::string&>(other); 
    }
};

namespace std {
    template<> struct hash<KVTKey> { std::size_t operator()(const KVTKey& key) const { return std::hash<std::string>()(static_cast<const std::string&>(key)); } };
}

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
    KEY_VERSION_MISMATCH,                  // Key version mismatch
    WRITE_CONFLICT,                        // Write conflict
    DELETE_CONFLICT,                       // Delete conflict
    UPDATE_CONFLICT,                       // Update conflict
    RANGE_UPDATE_CONFLICT,                 // Range update conflict
    RANGE_DELETE_CONFLICT,                 // Range delete conflict
    RANGE_INSERT_CONFLICT,                 // Range insert conflict
    TRANSACTION_HAS_STALE_DATA,            // OCC validation failed due to concurrent modifications
    ONE_SHOT_WRITE_NOT_ALLOWED,            // Write operations require an active transaction
    ONE_SHOT_DELETE_NOT_ALLOWED,           // Delete operations require an active transaction
    BATCH_NOT_FULLY_SUCCESS,               // Some operations succeeded, some failed
    EXT_FUNC_ERROR,                        // Error returned from external function
    SCAN_LIMIT_REACHED,                    // Scan limit reached: this is not an error,
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
    KVTKey key;
    std::string value;
}; 

struct KVTOpResult
{
    KVTError error;
    std::string value; //only valid for get operation
}; 

typedef std::vector<KVTOp> KVTBatchOps;
typedef std::vector<KVTOpResult> KVTBatchResults;


// Forward declaration - implementation hidden
class KVTManagerWrapper;

/**
 * KVT (Key-Value Transaction) API
 * 
 * This is a self-contained C++ API for transactional key-value operations.
 * It provides table management, transaction control, and CRUD operations
 * with full ACID properties.
 * 
 * Usage example:
 * ```cpp
 * #include "kvt_inc.h"
 * 
 * int main() {
 *     // Initialize the KVT system
 *     kvt_initialize();
 *     
 *     // Create a table
 *     std::string error;
 *     uint64_t table_id = kvt_create_table("my_table", "hash", error);
 *     
 *     // Start a transaction
 *     uint64_t tx_id = kvt_start_transaction(error);
 *     
 *     // Perform operations using table_id
 *     kvt_set(tx_id, table_id, "key1", "value1", error);
 *     
 *     // Commit the transaction
 *     kvt_commit_transaction(tx_id, error);
 *     
 *     // Cleanup
 *     kvt_shutdown();
 * }
 * ```
 */


 /**
 * Set the verbosity of the KVT system. 0 ~ 3: 0 (none), 1 (warnings), 2 (information), 3 (detailed tracing)
 * @return KVTError::SUCCESS
 */
KVTError kvt_set_verbosity(int verbosity); 

 /**
 * Set the sanity check level of the KVT system.
 * @param level Sanity check level: 0 (none), 1 (basic), 2 (detailed), 3 (very detailed)
 * @return KVTError::SUCCESS 
 */
KVTError kvt_set_sanity_check_level(int level); 

/**
 * Initialize the KVT system.
 * Must be called before any other KVT functions.
 * @return KVTError::SUCCESS if initialization succeeded, KVTError::UNKNOWN_ERROR otherwise
 */
KVTError kvt_initialize();

/**
 * Shutdown the KVT system and cleanup resources.
 * Should be called when done using KVT.
 */
void kvt_shutdown();

/**
 * Create a new table with the specified name and partition method.
 * @param table_name Name of the table to create
 * @param partition_method Partitioning method: "hash" or "range"
 * @param table_id Output parameter for table ID if successful (non-zero), 0 if failed
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_create_table(const std::string& table_name, 
                          const std::string& partition_method,
                          uint64_t& table_id,
                          std::string& error_msg);

/**
 * Drop/delete a table with the specified ID.
 * @param table_id ID of the table to drop
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_drop_table(uint64_t table_id, 
                        std::string& error_msg);

/**
 * Get the name of a table by its ID.
 * @param table_id ID of the table
 * @param table_name Output parameter for the table name
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_get_table_name(uint64_t table_id, 
                            std::string& table_name, 
                            std::string& error_msg);

/**
 * Get the ID of a table by its name.
 * @param table_name Name of the table
 * @param table_id Output parameter for the table ID
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_get_table_id(const std::string& table_name, 
                          uint64_t& table_id, 
                          std::string& error_msg);

/**
 * List all tables in the system.
 * @param results Output parameter for vector of table name and ID pairs
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_list_tables(std::vector<std::pair<std::string, uint64_t>>& results, 
                         std::string& error_msg);

/**
 * Start a new transaction.
 * @param tx_id Output parameter for transaction ID if successful (non-zero), 0 if failed
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_start_transaction(uint64_t& tx_id, 
                               std::string& error_msg);

/**
 * Get a value from a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_id ID of the table
 * @param key Key to retrieve
 * @param value Output parameter for the retrieved value
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_get(uint64_t tx_id, 
                uint64_t table_id,
                const KVTKey& key,
                std::string& value,
                std::string& error_msg);

/**
 * Set a key-value pair in a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_id ID of the table
 * @param key Key to set
 * @param value Value to set
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_set(uint64_t tx_id,
                uint64_t table_id,
                const KVTKey& key,
                const std::string& value,
                std::string& error_msg);

/**
 * Delete a key from a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_id ID of the table
 * @param key Key to delete
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_del(uint64_t tx_id, 
                uint64_t table_id,
                const KVTKey& key,
                std::string& error_msg);

/**
 * Scan a range of keys in a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_id ID of the table (must be range-partitioned)
 * @param key_start Start of key range (inclusive)
 * @param key_end End of key range (exclusive)
 * @param num_item_limit Maximum number of items to return
 * @param results Output parameter for key-value pairs found
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_scan(uint64_t tx_id,
                uint64_t table_id,
                const KVTKey& key_start,
                const KVTKey& key_end,
                size_t num_item_limit,
                std::vector<std::pair<KVTKey, std::string>>& results,
                std::string& error_msg);

/**
 * Execute a batch of operations within a transaction.
 * Operations are executed sequentially. If all operations succeed, returns SUCCESS.
 * If any operation fails, returns BATCH_NOT_FULLY_SUCCESS with error details.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operations)
 * @param batch_ops Vector of operations to execute
 * @param batch_results Output parameter for results of each operation
 * @param error_msg Output parameter for concatenated error messages with op indices
 *  "op[" + std::to_string(i) + "]: " + op_error + "; "
 * @return KVTError::SUCCESS if all operations successful, KVTError::BATCH_NOT_FULLY_SUCCESS if some failed
 */
KVTError kvt_batch_execute(uint64_t tx_id,
    const KVTBatchOps& batch_ops,
    KVTBatchResults& batch_results,
    std::string& error_msg);

//first return value mark success or failure 
//second return value mark if we need to update the value in the table
//third return value mark if we need to return the value to the user. 
typedef std::function<std::tuple<bool /*success*/, bool /*update value*/, bool /* return value*/ > (
    const KVTKey& /* key */, 
    const std::string& /* original value*/, 
    const std::string& /* parameter value*/, 
    std::string& /*new value to be set*/, 
    std::string& /*result value returned to user, or error message if not SUCCESS*/)> KVUpdateFunc;


 /**
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_id ID of the table
 * @param key Key to update
 * @param func Function to update the value
 * @param parameter Parameter to pass to the function
 * @param result_value  value returned back to the user
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_update(uint64_t tx_id, 
    uint64_t table_id,
    const KVTKey& key,
    KVUpdateFunc & func,
    const std::string& parameter,
    std::string& result_value,
    std::string& error_msg);


/**
* @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
* @param table_id ID of the table
* @param key_start Start of key range (inclusive)
* @param key_end End of key range (exclusive)
* @param num_item_limit Maximum number of items to return
* @param func Function to update the value
* @param parameter Parameter to pass to the function
* @param results Output parameter for key-value pairs found
* @param error_msg error message if fails, empty if success
* @return KVTError::SUCCESS if successful, appropriate error code otherwise
*/
KVTError kvt_range_update(uint64_t tx_id, 
        uint64_t table_id,
        const KVTKey& key_start,
        const KVTKey& key_end,
        size_t num_item_limit,
        KVUpdateFunc & func,
        const std::string& parameter,
        std::vector<std::pair<KVTKey, std::string>>& results,
        std::string& error_msg);


/**
 * Commit a transaction, making all changes permanent.
 * @param tx_id Transaction ID to commit
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */

KVTError kvt_commit_transaction(uint64_t tx_id, 
                                std::string& error_msg);

/**
 * Rollback/abort a transaction, discarding all changes.
 * @param tx_id Transaction ID to rollback
 * @param error_msg error message if fails, empty if success
 * @return KVTError::SUCCESS if successful, appropriate error code otherwise
 */
KVTError kvt_rollback_transaction(uint64_t tx_id, 
                                  std::string& error_msg);


#endif // KVT_INC_H