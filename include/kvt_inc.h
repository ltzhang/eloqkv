#ifndef KVT_INC_H
#define KVT_INC_H

#include <string>
#include <vector>
#include <utility>
#include <cstdint>
#include <memory>

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
 *     // Perform operations
 *     kvt_set(tx_id, "my_table", "key1", "value1", error);
 *     
 *     // Commit the transaction
 *     kvt_commit_transaction(tx_id, error);
 *     
 *     // Cleanup
 *     kvt_shutdown();
 * }
 * ```
 */

// Global KVT manager instance (singleton pattern)
extern std::unique_ptr<KVTManagerWrapper> g_kvt_manager;

/**
 * Initialize the KVT system.
 * Must be called before any other KVT functions.
 * @return true if initialization succeeded, false otherwise
 */
bool kvt_initialize();

/**
 * Shutdown the KVT system and cleanup resources.
 * Should be called when done using KVT.
 */
void kvt_shutdown();

/**
 * Create a new table with the specified name and partition method.
 * @param table_name Name of the table to create
 * @param partition_method Partitioning method: "hash" or "range"
 * @param error_msg Output parameter for error message if operation fails
 * @return Table ID if successful (non-zero), 0 if failed
 */
uint64_t kvt_create_table(const std::string& table_name, 
                          const std::string& partition_method,
                          std::string& error_msg);

/**
 * Start a new transaction.
 * @param error_msg Output parameter for error message if operation fails
 * @return Transaction ID if successful (non-zero), 0 if failed
 */
uint64_t kvt_start_transaction(std::string& error_msg);

/**
 * Get a value from a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_name Name of the table
 * @param key Key to retrieve
 * @param value Output parameter for the retrieved value
 * @param error_msg Output parameter for error message if operation fails
 * @return true if successful, false if failed
 */
bool kvt_get(uint64_t tx_id, 
            const std::string& table_name,
            const std::string& key,
            std::string& value,
            std::string& error_msg);

/**
 * Set a key-value pair in a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_name Name of the table
 * @param key Key to set
 * @param value Value to set
 * @param error_msg Output parameter for error message if operation fails
 * @return true if successful, false if failed
 */
bool kvt_set(uint64_t tx_id,
            const std::string& table_name,
            const std::string& key,
            const std::string& value,
            std::string& error_msg);

/**
 * Delete a key from a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_name Name of the table
 * @param key Key to delete
 * @param error_msg Output parameter for error message if operation fails
 * @return true if successful, false if failed
 */
 bool kvt_del(uint64_t tx_id, 
    const std::string& table_name,
    const std::string& key,
    std::string& error_msg);

/**
 * Scan a range of keys in a table within a transaction.
 * @param tx_id Transaction ID (0 for auto-commit/one-shot operation)
 * @param table_name Name of the table (must be range-partitioned)
 * @param key_start Start of key range (inclusive)
 * @param key_end End of key range (inclusive)
 * @param num_item_limit Maximum number of items to return
 * @param results Output parameter for key-value pairs found
 * @param error_msg Output parameter for error message if operation fails
 * @return true if successful, false if failed
 */
bool kvt_scan(uint64_t tx_id,
             const std::string& table_name,
             const std::string& key_start,
             const std::string& key_end,
             size_t num_item_limit,
             std::vector<std::pair<std::string, std::string>>& results,
             std::string& error_msg);

/**
 * Commit a transaction, making all changes permanent.
 * @param tx_id Transaction ID to commit
 * @param error_msg Output parameter for error message if operation fails
 * @return true if successful, false if failed
 */
bool kvt_commit_transaction(uint64_t tx_id, std::string& error_msg);

/**
 * Rollback/abort a transaction, discarding all changes.
 * @param tx_id Transaction ID to rollback
 * @param error_msg Output parameter for error message if operation fails
 * @return true if successful, false if failed
 */
bool kvt_rollback_transaction(uint64_t tx_id, std::string& error_msg);

#endif // KVT_INC_H