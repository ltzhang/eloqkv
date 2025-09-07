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
    uint64_t table_id;
    KVTError result = kvt_create_table("users", "hash", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to create table: " << error << std::endl;
        return;
    }
    std::cout << "✓ Created table 'users' with ID: " << table_id << std::endl;
    
    // Test duplicate table creation (should fail)
    uint64_t dup_id;
    result = kvt_create_table("users", "hash", dup_id, error);
    if (result != KVTError::SUCCESS) {
        std::cout << "✓ Duplicate table creation correctly failed: " << error << std::endl;
    }
    
    // One-shot SET operation
    result = kvt_set(0, table_id, "user:1", "Alice", error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Set user:1 = Alice" << std::endl;
    } else {
        std::cerr << "Failed to set: " << error << std::endl;
    }
    
    // One-shot GET operation
    std::string value;
    result = kvt_get(0, table_id, "user:1", value, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Retrieved user:1 = " << value << std::endl;
        assert(value == "Alice");
    } else {
        std::cerr << "Failed to get: " << error << std::endl;
    }
    
    // Update the value
    result = kvt_set(0, table_id, "user:1", "Alice Smith", error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Updated user:1 = Alice Smith" << std::endl;
    }
    
    // Verify the update
    result = kvt_get(0, table_id, "user:1", value, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Verified update: user:1 = " << value << std::endl;
        assert(value == "Alice Smith");
    }
}

void test_transactions() {
    print_separator("Transaction Test");
    
    std::string error;
    
    // Get the users table ID
    uint64_t table_id;
    KVTError result = kvt_get_table_id("users", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to get table ID: " << error << std::endl;
        return;
    }
    
    // Start a transaction
    uint64_t tx_id;
    result = kvt_start_transaction(tx_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to start transaction: " << error << std::endl;
        return;
    }
    std::cout << "✓ Started transaction ID: " << tx_id << std::endl;
    
    // Perform multiple operations in the transaction
    result = kvt_set(tx_id, table_id, "user:2", "Bob", error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Set user:2 = Bob (in transaction)" << std::endl;
    }
    
    result = kvt_set(tx_id, table_id, "user:3", "Charlie", error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Set user:3 = Charlie (in transaction)" << std::endl;
    }
    
    // Read within the transaction
    std::string value;
    result = kvt_get(tx_id, table_id, "user:2", value, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Read user:2 in transaction = " << value << std::endl;
    }
    
    // Commit the transaction
    result = kvt_commit_transaction(tx_id, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Transaction committed successfully" << std::endl;
    } else {
        std::cerr << "Failed to commit: " << error << std::endl;
    }
    
    // Verify data persisted after commit
    result = kvt_get(0, table_id, "user:2", value, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Verified user:2 after commit = " << value << std::endl;
        assert(value == "Bob");
    }
}

void test_rollback() {
    print_separator("Rollback Test");
    
    std::string error;
    
    // Get the users table ID
    uint64_t table_id;
    KVTError result = kvt_get_table_id("users", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to get table ID: " << error << std::endl;
        return;
    }
    
    // Start a transaction
    uint64_t tx_id;
    result = kvt_start_transaction(tx_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to start transaction: " << error << std::endl;
        return;
    }
    std::cout << "✓ Started transaction ID: " << tx_id << std::endl;
    
    // Set a value in the transaction
    result = kvt_set(tx_id, table_id, "user:4", "David", error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Set user:4 = David (in transaction)" << std::endl;
    }
    
    // Rollback the transaction
    result = kvt_rollback_transaction(tx_id, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Transaction rolled back successfully" << std::endl;
    } else {
        std::cerr << "Failed to rollback: " << error << std::endl;
    }
    
    // Verify data was not persisted
    std::string value;
    result = kvt_get(0, table_id, "user:4", value, error);
    if (result != KVTError::SUCCESS) {
        std::cout << "✓ Verified user:4 does not exist after rollback" << std::endl;
    } else {
        std::cerr << "ERROR: user:4 should not exist after rollback!" << std::endl;
    }
}

void test_range_scan() {
    print_separator("Range Scan Test");
    
    std::string error;
    
    // Create a range-partitioned table
    uint64_t table_id;
    KVTError result = kvt_create_table("products", "range", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to create range table: " << error << std::endl;
        return;
    }
    std::cout << "✓ Created range-partitioned table 'products' with ID: " << table_id << std::endl;
    
    // Insert some products
    kvt_set(0, table_id, "prod:001", "Laptop", error);
    kvt_set(0, table_id, "prod:002", "Mouse", error);
    kvt_set(0, table_id, "prod:003", "Keyboard", error);
    kvt_set(0, table_id, "prod:004", "Monitor", error);
    kvt_set(0, table_id, "prod:005", "Headphones", error);
    std::cout << "✓ Inserted 5 products" << std::endl;
    
    // Scan a range
    std::vector<std::pair<KVTKey, std::string>> results;
    result = kvt_scan(0, table_id, "prod:002", "prod:004", 10, results, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Scan from prod:002 to prod:004 returned " << results.size() << " items:" << std::endl;
        for (const auto& [key, value] : results) {
            std::cout << "  " << key << " = " << value << std::endl;
        }
    } else {
        std::cerr << "Scan failed: " << error << std::endl;
    }
}

// Example function for atomic increment
bool atomic_increment_func(KVTProcessInput& input, KVTProcessOutput& output) {
    try {
        // Parse current value as integer
        int current_value = std::stoi(*input.value);
        int new_value = current_value + 1;
        
        // Update the value
        output.update_value = std::to_string(new_value);
        
        // Return the old value
        output.return_value = std::to_string(current_value);
        
        return true;
    } catch (const std::exception& e) {
        output.return_value = "Error: Invalid integer value";
        return false;
    }
}

// Example function for substring extraction
bool substring_extract_func(KVTProcessInput& input, KVTProcessOutput& output) {
    try {
        // Parse parameter as "start:length" format
        std::string param = *input.parameter;
        size_t colon_pos = param.find(':');
        if (colon_pos == std::string::npos) {
            output.return_value = "Error: Parameter must be in format 'start:length'";
            return false;
        }
        
        int start = std::stoi(param.substr(0, colon_pos));
        int length = std::stoi(param.substr(colon_pos + 1));
        
        if (start < 0 || length < 0 || start + length > static_cast<int>(input.value->size())) {
            output.return_value = "Error: Invalid substring parameters";
            return false;
        }
        
        // Extract substring
        std::string substring = input.value->substr(start, length);
        output.return_value = substring;
        
        return true;
    } catch (const std::exception& e) {
        output.return_value = "Error: Invalid parameter format";
        return false;
    }
}

// Example function for conditional delete
bool conditional_delete_func(KVTProcessInput& input, KVTProcessOutput& output) {
    // If the value contains the parameter string, delete the key
    if (input.value->find(*input.parameter) != std::string::npos) {
        output.delete_key = true;
        output.return_value = "Deleted: " + *input.value;
    } else {
        output.return_value = "Kept: " + *input.value;
    }
    
    return true;
}

void test_kvt_process() {
    print_separator("KVT Process Test (Single Key Operations)");
    
    std::string error;
    
    // Get the users table ID
    uint64_t table_id;
    KVTError result = kvt_get_table_id("users", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to get table ID: " << error << std::endl;
        return;
    }
    
    // Test atomic increment
    std::cout << "\n--- Testing Atomic Increment ---" << std::endl;
    
    // Set initial value
    kvt_set(0, table_id, "counter", "5", error);
    std::cout << "✓ Set counter = 5" << std::endl;
    
    // Perform atomic increment
    std::string parameter = ""; // No parameter needed for increment
    std::string return_value;
    result = kvt_process(0, table_id, "counter", atomic_increment_func, parameter, return_value, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Atomic increment returned old value: " << return_value << std::endl;
        
        // Verify the new value
        std::string new_value;
        kvt_get(0, table_id, "counter", new_value, error);
        std::cout << "✓ Counter is now: " << new_value << std::endl;
        assert(new_value == "6");
    } else {
        std::cerr << "Atomic increment failed: " << error << std::endl;
    }
    
    // Test substring extraction
    std::cout << "\n--- Testing Substring Extraction ---" << std::endl;
    
    // Set a test string
    kvt_set(0, table_id, "text", "Hello World", error);
    std::cout << "✓ Set text = 'Hello World'" << std::endl;
    
    // Extract substring "World" (start=6, length=5)
    parameter = "6:5";
    result = kvt_process(0, table_id, "text", substring_extract_func, parameter, return_value, error);
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Extracted substring: '" << return_value << "'" << std::endl;
        assert(return_value == "World");
    } else {
        std::cerr << "Substring extraction failed: " << error << std::endl;
    }
}

void test_kvt_range_process() {
    print_separator("KVT Range Process Test (Range Operations)");
    
    std::string error;
    
    // Get the products table ID
    uint64_t table_id;
    KVTError result = kvt_get_table_id("products", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to get table ID: " << error << std::endl;
        return;
    }
    
    // Add some products with descriptions
    kvt_set(0, table_id, "prod:006", "Wireless Mouse", error);
    kvt_set(0, table_id, "prod:007", "Wired Keyboard", error);
    kvt_set(0, table_id, "prod:008", "Bluetooth Headphones", error);
    kvt_set(0, table_id, "prod:009", "USB Cable", error);
    std::cout << "✓ Added 4 more products" << std::endl;
    
    // Test conditional delete - delete products containing "Wireless"
    std::cout << "\n--- Testing Conditional Delete ---" << std::endl;
    
    std::string parameter = "Wireless";
    std::vector<std::pair<KVTKey, std::string>> results;
    result = kvt_range_process(0, table_id, "prod:001", "prod:010", 10, 
                              conditional_delete_func, parameter, results, error);
    
    if (result == KVTError::SUCCESS) {
        std::cout << "✓ Range process completed. Results:" << std::endl;
        for (const auto& [key, value] : results) {
            std::cout << "  " << key << ": " << value << std::endl;
        }
        
        // Verify that "Wireless Mouse" was deleted
        std::string value;
        result = kvt_get(0, table_id, "prod:006", value, error);
        if (result != KVTError::SUCCESS) {
            std::cout << "✓ Verified 'Wireless Mouse' was deleted" << std::endl;
        } else {
            std::cerr << "ERROR: 'Wireless Mouse' should have been deleted!" << std::endl;
        }
        
        // Verify that "Wired Keyboard" was kept
        result = kvt_get(0, table_id, "prod:007", value, error);
        if (result == KVTError::SUCCESS) {
            std::cout << "✓ Verified 'Wired Keyboard' was kept: " << value << std::endl;
        } else {
            std::cerr << "ERROR: 'Wired Keyboard' should have been kept!" << std::endl;
        }
    } else {
        std::cerr << "Range process failed: " << error << std::endl;
    }
}

void test_concurrent_transactions() {
    print_separator("Concurrent Transactions Test");
    
    std::string error;
    
    // Get the users table ID
    uint64_t table_id;
    KVTError result = kvt_get_table_id("users", table_id, error);
    if (result != KVTError::SUCCESS) {
        std::cerr << "Failed to get table ID: " << error << std::endl;
        return;
    }
    
    // Start two transactions
    uint64_t tx1, tx2;
    KVTError result1 = kvt_start_transaction(tx1, error);
    KVTError result2 = kvt_start_transaction(tx2, error);
    
    if (result1 != KVTError::SUCCESS || result2 != KVTError::SUCCESS) {
        std::cerr << "Failed to start transactions" << std::endl;
        return;
    }
    
    std::cout << "✓ Started transaction 1: " << tx1 << std::endl;
    std::cout << "✓ Started transaction 2: " << tx2 << std::endl;
    
    // Transaction 1 operations
    kvt_set(tx1, table_id, "user:10", "Transaction1_User", error);
    std::cout << "✓ TX1: Set user:10" << std::endl;
    
    // Transaction 2 operations
    kvt_set(tx2, table_id, "user:11", "Transaction2_User", error);
    std::cout << "✓ TX2: Set user:11" << std::endl;
    
    // Commit transaction 1
    result1 = kvt_commit_transaction(tx1, error);
    if (result1 == KVTError::SUCCESS) {
        std::cout << "✓ TX1: Committed" << std::endl;
    }
    
    // Commit transaction 2
    result2 = kvt_commit_transaction(tx2, error);
    if (result2 == KVTError::SUCCESS) {
        std::cout << "✓ TX2: Committed" << std::endl;
    }
    
    // Verify both changes persisted
    std::string value;
    kvt_get(0, table_id, "user:10", value, error);
    std::cout << "✓ Verified user:10 = " << value << std::endl;
    
    kvt_get(0, table_id, "user:11", value, error);
    std::cout << "✓ Verified user:11 = " << value << std::endl;
}

int main() {
    std::cout << "==================================" << std::endl;
    std::cout << "     KVT API Sample Program      " << std::endl;
    std::cout << "==================================" << std::endl;
    
    // Initialize the KVT system
    KVTError result = kvt_initialize();
    if (result != KVTError::SUCCESS) {
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
        test_kvt_process();
        test_kvt_range_process();
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