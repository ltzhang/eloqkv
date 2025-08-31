#include "kvt_mem.h"
#include <iostream>
#include <vector>

int main() {
    // Test with OCC instead
    KVTManagerWrapperOCC kvt;
    
    std::string error_msg;
    
    // Create table
    uint64_t table_id = kvt.create_table("test_table", "range", error_msg);
    if (table_id == 0) {
        std::cerr << "Failed to create table: " << error_msg << std::endl;
        return 1;
    }
    
    // Start transaction
    uint64_t tx_id = kvt.start_transaction(error_msg);
    if (tx_id == 0) {
        std::cerr << "Failed to start transaction: " << error_msg << std::endl;
        return 1;
    }
    
    // Insert some test data in range 0 (keys 0-99)
    // We'll insert keys 0, 1, 2 with values that sum to 300 (divisible by 100)
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"0", "100"},
        {"1", "150"}, 
        {"2", "50"}
    };
    
    for (const auto& [key, value] : test_data) {
        if (!kvt.set(tx_id, "test_table", key, value, error_msg)) {
            std::cerr << "Failed to set " << key << "=" << value << ": " << error_msg << std::endl;
            return 1;
        }
        std::cout << "Set " << key << " = " << value << std::endl;
    }
    
    // Commit
    if (!kvt.commit_transaction(tx_id, error_msg)) {
        std::cerr << "Failed to commit: " << error_msg << std::endl;
        return 1;
    }
    
    // Now scan range 0-99 and check sum
    tx_id = kvt.start_transaction(error_msg);
    if (tx_id == 0) {
        std::cerr << "Failed to start scan transaction: " << error_msg << std::endl;
        return 1;
    }
    
    std::vector<std::pair<std::string, std::string>> results;
    if (!kvt.scan(tx_id, "test_table", "0", "99", 100, results, error_msg)) {
        std::cerr << "Failed to scan: " << error_msg << std::endl;
        return 1;
    }
    
    std::cout << "Scan results:" << std::endl;
    int sum = 0;
    for (const auto& [key, value] : results) {
        std::cout << "  Key: " << key << ", Value: " << value << std::endl;
        sum += std::stoi(value);
    }
    
    std::cout << "Total sum: " << sum << std::endl;
    std::cout << "Sum % 100: " << (sum % 100) << std::endl;
    
    if (sum % 100 == 0) {
        std::cout << "SUCCESS: Sum is divisible by 100!" << std::endl;
    } else {
        std::cout << "FAILURE: Sum is not divisible by 100!" << std::endl;
    }
    
    kvt.rollback_transaction(tx_id, error_msg);
    
    return 0;
}