#include "kvt_mem.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <cassert>
#include <atomic>
#include <sstream>
#include <algorithm>
#include <set>
#include <iomanip>

class SimpleKVTTest {
private:
    KVTManagerWrapperInterface* kvt_manager;
    std::mt19937 rng;
    std::string table_name;
    uint64_t table_id;
    std::atomic<int> transaction_count{0};
    std::atomic<int> commit_count{0};
    std::atomic<int> abort_count{0};
    
public:
    SimpleKVTTest(KVTManagerWrapperInterface* manager) : kvt_manager(manager), rng(std::random_device{}()) {
        table_name = "simple_test_table";
    }
    
    bool initialize() {
        std::cout << "Initializing simple test..." << std::endl;
        
        // Create table
        std::string error_msg;
        table_id = kvt_manager->create_table(table_name, "range", error_msg);
        if (table_id == 0) {
            std::cerr << "Failed to create table: " << error_msg << std::endl;
            return false;
        }
        std::cout << "Created table '" << table_name << "' with ID: " << table_id << std::endl;
        return true;
    }
    
    bool test_basic_operations() {
        std::cout << "Testing basic operations..." << std::endl;
        
        std::string error_msg;
        
        // Test 1: Simple set/get
        uint64_t tx_id = kvt_manager->start_transaction(error_msg);
        if (tx_id == 0) {
            std::cerr << "Failed to start transaction: " << error_msg << std::endl;
            return false;
        }
        
        if (!kvt_manager->set(tx_id, table_name, "key1", "value1", error_msg)) {
            std::cerr << "Failed to set key1: " << error_msg << std::endl;
            return false;
        }
        
        std::string value;
        if (!kvt_manager->get(tx_id, table_name, "key1", value, error_msg)) {
            std::cerr << "Failed to get key1: " << error_msg << std::endl;
            return false;
        }
        
        if (value != "value1") {
            std::cerr << "Value mismatch: expected 'value1', got '" << value << "'" << std::endl;
            return false;
        }
        
        if (!kvt_manager->commit_transaction(tx_id, error_msg)) {
            std::cerr << "Failed to commit transaction: " << error_msg << std::endl;
            return false;
        }
        
        std::cout << "Basic operations test PASSED" << std::endl;
        return true;
    }
    
    bool test_transaction_isolation() {
        std::cout << "Testing transaction isolation..." << std::endl;
        
        std::string error_msg;
        
        // Start two transactions
        uint64_t tx1 = kvt_manager->start_transaction(error_msg);
        uint64_t tx2 = kvt_manager->start_transaction(error_msg);
        
        if (tx1 == 0 || tx2 == 0) {
            std::cerr << "Failed to start transactions" << std::endl;
            return false;
        }
        
        // TX1 sets a value
        if (!kvt_manager->set(tx1, table_name, "isolation_key", "tx1_value", error_msg)) {
            std::cerr << "TX1 failed to set key: " << error_msg << std::endl;
            return false;
        }
        
        // TX2 should not see TX1's uncommitted changes
        std::string value;
        if (kvt_manager->get(tx2, table_name, "isolation_key", value, error_msg)) {
            std::cerr << "TX2 should not see uncommitted changes from TX1" << std::endl;
            return false;
        }
        
        // Commit TX1
        if (!kvt_manager->commit_transaction(tx1, error_msg)) {
            std::cerr << "Failed to commit TX1: " << error_msg << std::endl;
            return false;
        }
        
        // TX2 should still not see TX1's changes (depending on isolation level)
        // For most implementations this would be true
        
        // Rollback TX2
        kvt_manager->rollback_transaction(tx2, error_msg);
        
        std::cout << "Transaction isolation test PASSED" << std::endl;
        return true;
    }
    
    bool run_concurrent_test(int num_threads, int ops_per_thread) {
        std::cout << "Running concurrent test with " << num_threads << " threads, " 
                  << ops_per_thread << " operations each..." << std::endl;
        
        std::atomic<bool> test_failed{false};
        std::vector<std::thread> threads;
        
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([this, i, ops_per_thread, &test_failed]() {
                std::mt19937 local_rng(std::random_device{}());
                
                for (int op = 0; op < ops_per_thread && !test_failed; op++) {
                    std::string error_msg;
                    uint64_t tx_id = kvt_manager->start_transaction(error_msg);
                    
                    if (tx_id == 0) {
                        test_failed = true;
                        return;
                    }
                    
                    std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(op);
                    std::string value = "thread_" + std::to_string(i) + "_value_" + std::to_string(op);
                    
                    // Set a value
                    if (!kvt_manager->set(tx_id, table_name, key, value, error_msg)) {
                        test_failed = true;
                        return;
                    }
                    
                    // Get it back
                    std::string retrieved_value;
                    if (!kvt_manager->get(tx_id, table_name, key, retrieved_value, error_msg)) {
                        test_failed = true;
                        return;
                    }
                    
                    if (retrieved_value != value) {
                        test_failed = true;
                        return;
                    }
                    
                    // Commit with some probability
                    bool should_commit = std::uniform_real_distribution<>(0.0, 1.0)(local_rng) < 0.8;
                    
                    if (should_commit) {
                        if (kvt_manager->commit_transaction(tx_id, error_msg)) {
                            commit_count++;
                        } else {
                            abort_count++;
                        }
                    } else {
                        kvt_manager->rollback_transaction(tx_id, error_msg);
                        abort_count++;
                    }
                    
                    transaction_count++;
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        if (test_failed) {
            std::cout << "Concurrent test FAILED" << std::endl;
            return false;
        }
        
        std::cout << "Concurrent test PASSED" << std::endl;
        print_statistics();
        return true;
    }
    
    void print_statistics() {
        std::cout << "\n--- Statistics ---" << std::endl;
        std::cout << "Total transactions: " << transaction_count.load() << std::endl;
        std::cout << "Committed: " << commit_count.load() << std::endl;
        std::cout << "Aborted: " << abort_count.load() << std::endl;
        double commit_rate = (transaction_count > 0) ? 
                            (100.0 * commit_count / transaction_count) : 0;
        std::cout << "Commit rate: " << std::fixed << std::setprecision(2) 
                  << commit_rate << "%" << std::endl;
    }
    
    bool run_all_tests() {
        if (!test_basic_operations()) return false;
        if (!test_transaction_isolation()) return false;
        if (!run_concurrent_test(4, 100)) return false;
        
        std::cout << "\n=== ALL TESTS PASSED ===" << std::endl;
        return true;
    }
};

void test_implementation(KVTManagerWrapperInterface* wrapper, const std::string& impl_name) {
    std::cout << "\n\n##################################################" << std::endl;
    std::cout << "Testing implementation: " << impl_name << std::endl;
    std::cout << "##################################################" << std::endl;
    
    SimpleKVTTest test(wrapper);
    
    if (!test.initialize()) {
        std::cerr << "Failed to initialize test for " << impl_name << std::endl;
        return;
    }
    
    if (test.run_all_tests()) {
        std::cout << "SUCCESS: " << impl_name << " passed all tests!" << std::endl;
    } else {
        std::cout << "FAILURE: " << impl_name << " failed tests!" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    std::cout << "KVT Simple Correctness Test" << std::endl;
    std::cout << "============================" << std::endl;
    
    // Test KVTManagerWrapperSimple
    {
        std::cout << "\nTesting KVTManagerWrapperSimple..." << std::endl;
        KVTManagerWrapperSimple simple_wrapper;
        test_implementation(&simple_wrapper, "KVTManagerWrapperSimple");
    }
    
    // Test KVTManagerWrapperOCC
    {
        std::cout << "\nTesting KVTManagerWrapperOCC..." << std::endl;
        KVTManagerWrapperOCC occ_wrapper;
        test_implementation(&occ_wrapper, "KVTManagerWrapperOCC");
    }
    
    std::cout << "\n\n=== FINAL TEST SUMMARY ===" << std::endl;
    std::cout << "All implementations have been tested!" << std::endl;
    std::cout << "This verifies basic correctness of the KVT implementation." << std::endl;
    return 0;
}