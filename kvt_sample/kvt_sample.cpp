/**
 * KVT API Sample Program
 * 
 * This program demonstrates the usage of the KVT (Key-Value Transaction) API
 * including table creation, transactions, and CRUD operations.
 */

#include "kvt_inc.h"
#include <iostream>
#include <cassert>
#include <vector>
#include <string>

void print_separator(const std::string& title) {
    std::cout << "\n========== " << title << " ==========" << std::endl;
}

void test_basic_operations() {
    print_separator("Basic Operations Test");
    
    std::string error;
    
    // Create a hash table
    uint64_t table_id = kvt_create_table("users", "hash", error);
    if (table_id == 0) {
        std::cerr << "Failed to create table: " << error << std::endl;
        return;
    }
    std::cout << "✓ Created table 'users' with ID: " << table_id << std::endl;
    
    // Test duplicate table creation (should fail)
    uint64_t dup_id = kvt_create_table("users", "hash", error);
    if (dup_id == 0) {
        std::cout << "✓ Duplicate table creation correctly failed: " << error << std::endl;
    }
    
    // One-shot SET operation
    if (kvt_set(0, "users", "user:1", "Alice", error)) {
        std::cout << "✓ Set user:1 = Alice" << std::endl;
    } else {
        std::cerr << "Failed to set: " << error << std::endl;
    }
    
    // One-shot GET operation
    std::string value;
    if (kvt_get(0, "users", "user:1", value, error)) {
        std::cout << "✓ Retrieved user:1 = " << value << std::endl;
        assert(value == "Alice");
    } else {
        std::cerr << "Failed to get: " << error << std::endl;
    }
    
    // Update the value
    if (kvt_set(0, "users", "user:1", "Alice Smith", error)) {
        std::cout << "✓ Updated user:1 = Alice Smith" << std::endl;
    }
    
    // Verify the update
    if (kvt_get(0, "users", "user:1", value, error)) {
        std::cout << "✓ Verified update: user:1 = " << value << std::endl;
        assert(value == "Alice Smith");
    }
}

void test_transactions() {
    print_separator("Transaction Test");
    
    std::string error;
    
    // Start a transaction
    uint64_t tx_id = kvt_start_transaction(error);
    if (tx_id == 0) {
        std::cerr << "Failed to start transaction: " << error << std::endl;
        return;
    }
    std::cout << "✓ Started transaction ID: " << tx_id << std::endl;
    
    // Perform multiple operations in the transaction
    if (kvt_set(tx_id, "users", "user:2", "Bob", error)) {
        std::cout << "✓ Set user:2 = Bob (in transaction)" << std::endl;
    }
    
    if (kvt_set(tx_id, "users", "user:3", "Charlie", error)) {
        std::cout << "✓ Set user:3 = Charlie (in transaction)" << std::endl;
    }
    
    // Read within the transaction
    std::string value;
    if (kvt_get(tx_id, "users", "user:2", value, error)) {
        std::cout << "✓ Read user:2 in transaction = " << value << std::endl;
    }
    
    // Commit the transaction
    if (kvt_commit_transaction(tx_id, error)) {
        std::cout << "✓ Transaction committed successfully" << std::endl;
    } else {
        std::cerr << "Failed to commit: " << error << std::endl;
    }
    
    // Verify data persisted after commit
    if (kvt_get(0, "users", "user:2", value, error)) {
        std::cout << "✓ Verified user:2 after commit = " << value << std::endl;
        assert(value == "Bob");
    }
}

void test_rollback() {
    print_separator("Rollback Test");
    
    std::string error;
    
    // Start a transaction
    uint64_t tx_id = kvt_start_transaction(error);
    if (tx_id == 0) {
        std::cerr << "Failed to start transaction: " << error << std::endl;
        return;
    }
    std::cout << "✓ Started transaction ID: " << tx_id << std::endl;
    
    // Set a value in the transaction
    if (kvt_set(tx_id, "users", "user:4", "David", error)) {
        std::cout << "✓ Set user:4 = David (in transaction)" << std::endl;
    }
    
    // Rollback the transaction
    if (kvt_rollback_transaction(tx_id, error)) {
        std::cout << "✓ Transaction rolled back successfully" << std::endl;
    } else {
        std::cerr << "Failed to rollback: " << error << std::endl;
    }
    
    // Verify data was not persisted
    std::string value;
    if (!kvt_get(0, "users", "user:4", value, error)) {
        std::cout << "✓ Verified user:4 does not exist after rollback" << std::endl;
    } else {
        std::cerr << "ERROR: user:4 should not exist after rollback!" << std::endl;
    }
}

void test_range_scan() {
    print_separator("Range Scan Test");
    
    std::string error;
    
    // Create a range-partitioned table
    uint64_t table_id = kvt_create_table("products", "range", error);
    if (table_id == 0) {
        std::cerr << "Failed to create range table: " << error << std::endl;
        return;
    }
    std::cout << "✓ Created range-partitioned table 'products' with ID: " << table_id << std::endl;
    
    // Insert some products
    kvt_set(0, "products", "prod:001", "Laptop", error);
    kvt_set(0, "products", "prod:002", "Mouse", error);
    kvt_set(0, "products", "prod:003", "Keyboard", error);
    kvt_set(0, "products", "prod:004", "Monitor", error);
    kvt_set(0, "products", "prod:005", "Headphones", error);
    std::cout << "✓ Inserted 5 products" << std::endl;
    
    // Scan a range
    std::vector<std::pair<std::string, std::string>> results;
    if (kvt_scan(0, "products", "prod:002", "prod:004", 10, results, error)) {
        std::cout << "✓ Scan from prod:002 to prod:004 returned " << results.size() << " items:" << std::endl;
        for (const auto& [key, value] : results) {
            std::cout << "  " << key << " = " << value << std::endl;
        }
    } else {
        std::cerr << "Scan failed: " << error << std::endl;
    }
}

void test_concurrent_transactions() {
    print_separator("Concurrent Transactions Test");
    
    std::string error;
    
    // Start two transactions
    uint64_t tx1 = kvt_start_transaction(error);
    uint64_t tx2 = kvt_start_transaction(error);
    
    if (tx1 == 0 || tx2 == 0) {
        std::cerr << "Failed to start transactions" << std::endl;
        return;
    }
    
    std::cout << "✓ Started transaction 1: " << tx1 << std::endl;
    std::cout << "✓ Started transaction 2: " << tx2 << std::endl;
    
    // Transaction 1 operations
    kvt_set(tx1, "users", "user:10", "Transaction1_User", error);
    std::cout << "✓ TX1: Set user:10" << std::endl;
    
    // Transaction 2 operations
    kvt_set(tx2, "users", "user:11", "Transaction2_User", error);
    std::cout << "✓ TX2: Set user:11" << std::endl;
    
    // Commit transaction 1
    if (kvt_commit_transaction(tx1, error)) {
        std::cout << "✓ TX1: Committed" << std::endl;
    }
    
    // Commit transaction 2
    if (kvt_commit_transaction(tx2, error)) {
        std::cout << "✓ TX2: Committed" << std::endl;
    }
    
    // Verify both changes persisted
    std::string value;
    kvt_get(0, "users", "user:10", value, error);
    std::cout << "✓ Verified user:10 = " << value << std::endl;
    
    kvt_get(0, "users", "user:11", value, error);
    std::cout << "✓ Verified user:11 = " << value << std::endl;
}

int main() {
    std::cout << "==================================" << std::endl;
    std::cout << "     KVT API Sample Program      " << std::endl;
    std::cout << "==================================" << std::endl;
    
    // Initialize the KVT system
    if (!kvt_initialize()) {
        std::cerr << "Failed to initialize KVT system!" << std::endl;
        return 1;
    }
    std::cout << "✓ KVT system initialized" << std::endl;
    
    try {
        // Run all tests
        test_basic_operations();
        test_transactions();
        test_rollback();
        test_range_scan();
        test_concurrent_transactions();
        
        print_separator("All Tests Completed Successfully");
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        kvt_shutdown();
        return 1;
    }
    
    // Cleanup
    kvt_shutdown();
    std::cout << "\n✓ KVT system shutdown" << std::endl;
    
    return 0;
}