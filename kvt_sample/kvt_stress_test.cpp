#include "kvt_mem.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <cassert>
#include <atomic>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <set>
#include <cstdlib>
#include <iomanip>

// Configuration constants
const int MAX_KEY = 10000;
const int MAX_VALUE = 1000;
const int RANGE_SIZE = 100;
const int MAX_RANGES = MAX_KEY / RANGE_SIZE;
const int MAX_OPS_PER_TX = 20;
const int MIN_OPS_PER_TX = 1;
const int MAX_RANGES_PER_TX = 4;
const int INITIAL_KEYS = 2000;  // Back to normal
const int CONSISTENCY_CHECK_INTERVAL = 100;
const int MAX_CONCURRENT_TXS = 10;
const double COMMIT_RATIO = 0.7; // 70% commit, 30% abort
const int CONSTRAINT_DIVISOR = 100; 

// Test modes
enum TestMode {
    SINGLE_NON_INTERLEAVED,
    SINGLE_INTERLEAVED,
    MULTI_THREADED
};

#define PANIC(x) {x; exit(1);}

// Transaction context for tracking operations
struct TransactionContext {
    struct Operation {
        enum {
            OP_GET,
            OP_SET,
            OP_DEL,
            OP_SCAN
        }; 
        int op_type;
        int key;
        int key2; 
        int value;
        int value2;
    };
    uint64_t tx_id;
    bool should_commit;
    int num_ops;
    std::vector<int> ranges; 
    std::vector<Operation> operations; // for replay/debug
};

class KVTStressTest {
private:
    KVTManagerWrapperInterface* kvt_manager;
    std::mt19937 rng;
    std::string table_name;
    uint64_t table_id;
    std::atomic<int> transaction_count{0};
    std::atomic<int> commit_count{0};
    std::atomic<int> abort_count{0};
    std::atomic<bool> test_running{true};
    
    // Helper functions
    int get_range_id(int key) {
        return key / RANGE_SIZE;
    }
    
    std::string key_to_string(int key) {
        // Zero-pad to 5 digits so string comparison matches numerical order
        // MAX_KEY is 10000, so we need 5 digits
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(5) << key;
        return ss.str();
    }
    
    std::string value_to_string(int value) {
        return std::to_string(value);
    }
    
    int string_to_value(const std::string& str) {
        return std::stoi(str);
    }
    
    int string_to_key(const std::string& str) {
        return std::stoi(str);
    }
    
public:
    KVTStressTest(KVTManagerWrapperInterface* manager) : kvt_manager(manager), rng(42) {  // Fixed seed for debugging
        table_name = "stress_test_table";
    }
    
    bool initialize() {
        std::cout << "Initializing stress test..." << std::endl;
        
        // Create table
        std::string error_msg;
        table_id = kvt_manager->create_table(table_name, "range", error_msg);
        if (table_id == 0)
            PANIC(std::cerr << "Failed to create table: " << error_msg << std::endl;);
        // Populate initial data
        populate_initial_data(); 
        check_consistency("initial"); 
        std::cout << "Initialization complete!" << std::endl;
        return true;
    }
    
    void populate_initial_data() {
        std::string error_msg;
        int keys_each_range = INITIAL_KEYS / MAX_RANGES;
        for (int range_id = 0; range_id < MAX_RANGES; range_id++) {
            int range_start = range_id * RANGE_SIZE;
            int range_sum = 0; 
            std::unordered_map<int, int> key_values; 
            for (int i = 0; i < keys_each_range; i++) {
                int key = rand() % RANGE_SIZE; 
                int value = rand() % MAX_VALUE; 
                if (key_values.find(key) != key_values.end()) {
                    i--; 
                    continue; 
                }
                key_values[key] = value;
                range_sum += value;
            }
            int diff = CONSTRAINT_DIVISOR - range_sum % CONSTRAINT_DIVISOR; 
            key_values.begin().second += diff;
            for (auto [key, value] : key_values) {
                kvt_manager->set(0, table_name, key_to_string(key), value_to_string(value), error_msg);
            }
        }
    }
    
    bool check_consistency() {
        std::string error_msg;
        uint64_t tx_id = 0;
        // Scan entire key range
        std::vector<std::pair<std::string, std::string>> all_results;
        if (!kvt_manager->scan(tx_id, table_name, key_to_string(0), 
                               key_to_string(MAX_KEY), MAX_KEY, all_results, error_msg))
            PANIC(std::cerr << "Failed to scan database: " << error_msg << std::endl;);
        // Check constraints - results are sorted
        bool all_consistent = true;
        int current_range = -1;
        int current_range_sum = 0;
        for (const auto& [key_str, value_str] : all_results) {
            int key = string_to_key(key_str);
            int value = string_to_value(value_str);
            int range_id = get_range_id(key);
            // If we moved to a new range, check the previous range's sum
            if (range_id != current_range) {
                if (current_range != -1 && current_range_sum > 0 && current_range_sum % 100 != 0)
                    PANIC(std::cerr << "Consistency violation in range " << current_range
                                 << ": sum=" << current_range_sum << " (not divisible by 100)" << std::endl;);
                // Start new range
                current_range = range_id;
                current_range_sum = 0;
            }
            current_range_sum += value;
        }
        // Check the last range
        if (current_range != -1 && current_range_sum > 0 && current_range_sum % 100 != 0) 
            PANIC(std::cerr << "Consistency violation in range " << current_range
                         << ": sum=" << current_range_sum << " (not divisible by 100)" << std::endl;);
        return all_consistent;
    }
    
    TransactionContext create_transaction_context() {
        TransactionContext ctx;
        std::string error_msg;
        ctx.tx_id = kvt_manager->start_transaction(error_msg);
        if (ctx.tx_id == 0) 
            PANIC(std::cerr << "Failed to start transaction: " << error_msg << std::endl;);
        std::uniform_real_distribution<> commit_dist(0.0, 1.0);
        ctx.should_commit = commit_dist(rng) < COMMIT_RATIO;
        ctx.num_ops = rand() % MAX_OPS_PER_TX + 1;
        int num_ranges = rand() % MAX_RANGES_PER_TX + 1;
        // Select random ranges
        for (int i = 0; i < num_ranges; i++) {
            int range_id = rand() % MAX_RANGES + 1; //can repeat
            ctx.ranges.push_back(range_id);
        }
        return ctx;
    }

    //return false if no more operations to execute
    void execute_single_operation(TransactionContext& ctx) {
        int rindex = rand() % ctx.ranges.size();
        int range_id = ctx.ranges[rindex];
        //randomly choose a op to execute according to the probability
        std::string error_msg;
        //first choose a range from ctx.ranges
        std::uniform_real_distribution<> prob_dist(0.0, 1.0);
        std::string value;
        if (prob_dist(rng) < 0.4) { //get
            //choose a key from the range
            int key = rand() % RANGE_SIZE + range_id * RANGE_SIZE;
            if (!kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), value, error_msg))
                PANIC(std::cerr << "Failed to get key: " << error_msg << std::endl;);
            int value_int = string_to_value(value);
            //append to the operations
            ctx.operations.push_back({TransactionContext::Operation::OP_GET, key, 0,  value_int, 0});
        } else if (prob_dist(rng) < 0.8) { //set
            //scan the ops to see whether we have already got a key's value. 
            int key = -1;
            if (ctx.operations.size() > 0 && ctx.operations.back().op_type == TransactionContext::Operation::OP_GET) {
                key = ctx.operations.back().key; //if previous op is a get, we can use the key
            } else {
                //otherwise, choose a new key, and get its value
                int key = rand() % RANGE_SIZE + range_id * RANGE_SIZE;
                if (!kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), value, error_msg))
                    PANIC(std::cerr << "Failed to get key: " << error_msg << std::endl;);
                int value_int = string_to_value(value);
                //append to the operations
                ctx.operations.push_back({TransactionContext::Operation::OP_GET, key, 0,  value_int, 0});
            }
            //choose a new value
            int new_value = rand() % MAX_VALUE;
            if (!kvt_manager->set(ctx.tx_id, table_name, key_to_string(key), value_to_string(new_value), error_msg))
                PANIC(std::cerr << "Failed to set key: " << error_msg << std::endl;);
            ctx.operations.push_back({TransactionContext::Operation::OP_SET, key, 0,  new_value, 0});
        } else if (prob_dist(rng) < 0.9) { //del
            int key = -1; 
            if (ctx.operations.size() > 0 && ctx.operations.back().op_type == TransactionContext::Operation::OP_GET) {
                key = ctx.operations.back().key; //if previous op is a get, we can use the key
            } else {
                //otherwise, choose a new key, and get its value
                int key = rand() % RANGE_SIZE + range_id * RANGE_SIZE;
                if (!kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), value, error_msg))
                    PANIC(std::cerr << "Failed to get key: " << error_msg << std::endl;);
                int value_int = string_to_value(value);
                //append to the operations
                ctx.operations.push_back({TransactionContext::Operation::OP_GET, key, 0,  value_int, 0});
            }
            if (!kvt_manager->del(ctx.tx_id, table_name, key_to_string(key), error_msg))
                PANIC(std::cerr << "Failed to delete key: " << error_msg << std::endl;);
            ctx.operations.push_back({TransactionContext::Operation::OP_DEL, key, 0, 0, 0});
        } else { //scan does not need to care about range
            int start_key = rand() % MAX_KEY;
            int end_key = rand() % MAX_KEY;
            if (start_key > end_key) {
                int tmp = start_key;
                start_key = end_key;
                end_key = tmp;
            }
            std::vector<std::pair<std::string, std::string>> results;
            if (!kvt_manager->scan(ctx.tx_id, table_name, key_to_string(start_key), key_to_string(end_key), RANGE_SIZE, results, error_msg))
                PANIC(std::cerr << "Failed to scan key: " << error_msg << std::endl;);
            //append to the operations
            ctx.operations.push_back({TransactionContext::Operation::OP_SCAN, start_key, end_key, 0, results.size()});
        }
        ctx.total_ops_remaining--;
    }
    
    bool run_single_transaction_normal(TransactionContext& ctx) {
        std::string error_msg;
        //total number of operations to execute will be larger than the predefined number
        //first execute the predefined number of operations, some may create more than 1 op.
        for (int i = 0; i < ctx.num_ops; i++) {
            execute_single_operation(ctx);
        }
        if (ctx.should_commit) {
            //now fix up the operations to make the constraint satisfied
            std::map<int, int> range_delta_sum;
            for (int range_id : ctx.ranges) {
                range_delta_sum[range_id] = 0;
            }
            //scan the ops to get the range_delta_sum, a mutation op(set, del) will always have a get before it. 
            for (int i = 0; i < ctx.operations.size(); i++) {
                if (ctx.operations[i].op_type == TransactionContext::Operation::OP_SET) {
                    assert (i > 0 && ctx.operations[i-1].op_type == TransactionContext::Operation::OP_GET);
                    int prev_value = ctx.operations[i-1].value;
                    int new_value = ctx.operations[i].value;
                    assert(range_delta_sum.find(ctx.operations[i].key / RANGE_SIZE) != range_delta_sum.end());
                    range_delta_sum[ctx.operations[i].key / RANGE_SIZE] += new_value - prev_value;
                } else if (ctx.operations[i].op_type == TransactionContext::Operation::OP_DEL) {
                    assert (i > 0 && ctx.operations[i-1].op_type == TransactionContext::Operation::OP_GET);
                    int prev_value = ctx.operations[i-1].value;
                    assert(range_delta_sum.find(ctx.operations[i].key / RANGE_SIZE) != range_delta_sum.end());
                    range_delta_sum[ctx.operations[i].key / RANGE_SIZE] -= prev_value;
                }
            }
            //now fix up the operations to make the constraint satisfied
            for (auto [range_id, delta_sum] : range_delta_sum) {
                int diff = CONSTRAINT_DIVISOR - delta_sum % CONSTRAINT_DIVISOR;
                if (diff != CONSTRAINT_DIVISOR) { //we need to get a new value to add to the range
                    //we get a key's value
                    int key = rand() % RANGE_SIZE + range_id * RANGE_SIZE;
                    if (!kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), value, error_msg))
                        PANIC(std::cerr << "Failed to get key: " << error_msg << std::endl;);
                    int value_int = string_to_value(value);
                    //append to the operations
                    ctx.operations.push_back({TransactionContext::Operation::OP_GET, key, 0,  value_int, 0});
                    kvt_manager->set(ctx.tx_id, table_name, key_to_string(key), value_to_string(value_int + diff), error_msg);
                    ctx.operations.push_back({TransactionContext::Operation::OP_SET, key, 0,  value_int + diff, 0});
                }
            }
            //commit
            bool success = kvt_manager->commit_transaction(ctx.tx_id, error_msg);
            if (success) {
                commit_count++;
            } else {
                std::cerr << "TX" << ctx.tx_id << " Commit failed: " << error_msg << std::endl;
                abort_count++;
            }
            return success;
        } else {
            //abort
            bool success = kvt_manager->rollback_transaction(ctx.tx_id, error_msg);
            abort_count++;
            return success;
        }
    }

    void run_single_non_interleaved_test(int num_transactions) {
        std::cout << "\n=== Running Single-Threaded Non-Interleaved Test ===" << std::endl;
        std::cout << "Executing " << num_transactions << " transactions sequentially..." << std::endl;
        
        auto start_time = std::chrono::steady_clock::now();
        
        for (int i = 0; i < num_transactions; i++) {
            TransactionContext ctx = create_transaction_context();
            run_single_transaction(ctx);
            
            // Periodic consistency check
            if ((i + 1) % CONSISTENCY_CHECK_INTERVAL == 0) {
                if (!check_consistency("transaction " + std::to_string(i + 1))) {
                    std::cerr << "Consistency check failed after transaction " << i + 1 << std::endl;
                    break;
                }
            }
        }
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "Test completed in " << duration.count() << " ms" << std::endl;
        print_statistics();
    }
    
    void run_single_interleaved_test(int num_transactions) {
        std::cout << "\n=== Running Single-Threaded Interleaved Test ===" << std::endl;
        std::cout << "Executing " << num_transactions << " transactions with interleaving..." << std::endl;
        
        auto start_time = std::chrono::steady_clock::now();
        
        std::vector<TransactionContext> active_contexts;
        int completed = 0;
        int started = 0;
        
        while (completed < num_transactions) {
            // Start new transactions if below limit
            std::uniform_real_distribution<> start_dist(0.0, 1.0);
            if (active_contexts.size() < MAX_CONCURRENT_TXS && 
                started < num_transactions && 
                start_dist(rng) < 0.7) { // 70% chance to start new transaction
                
                TransactionContext ctx = create_transaction_context();
                std::string error_msg;
                ctx.tx_id = kvt_manager->start_transaction(error_msg);
                if (ctx.tx_id != 0) {
                    active_contexts.push_back(ctx);
                    started++;
                }
            }
            
            if (active_contexts.empty()) continue;
            
            // Choose a random transaction to execute one operation
            std::uniform_int_distribution<> ctx_dist(0, active_contexts.size() - 1);
            int idx = ctx_dist(rng);
            TransactionContext& ctx = active_contexts[idx];
            
            // Execute one operation
            if (ctx.total_ops_remaining > 0) {
                execute_single_operation(ctx);
            } else {
                // Transaction ready to complete
                std::string error_msg;
                bool success;
                
                if (ctx.should_commit) {
                    // Check constraint
                    bool constraint_satisfied = true;
                    // Check each affected range by scanning it
                    for (int range_id : ctx.ranges_involved) {
                        std::string start_key = key_to_string(range_id * 100);
                        std::string end_key = key_to_string((range_id + 1) * 100);
                        
                        std::vector<std::pair<std::string, std::string>> range_data;
                        if (!kvt_manager->scan(ctx.tx_id, table_name, start_key, end_key, 100, range_data, error_msg)) {
                            constraint_satisfied = false;
                            break;
                        }
                        
                        // Calculate the sum for this range
                        int range_sum = 0;
                        for (const auto& [key, value] : range_data) {
                            range_sum += string_to_value(value);
                        }
                        
                        // Check if sum is divisible by 100
                        if (range_sum % 100 != 0) {
                            constraint_satisfied = false;
                            break;
                        }
                    }
                    
                    if (constraint_satisfied) {
                        success = kvt_manager->commit_transaction(ctx.tx_id, error_msg);
                        if (success) {
                            commit_count++;
                        } else {
                            abort_count++;
                        }
                    } else {
                        success = kvt_manager->rollback_transaction(ctx.tx_id, error_msg);
                        abort_count++;
                    }
                } else {
                    success = kvt_manager->rollback_transaction(ctx.tx_id, error_msg);
                    abort_count++;
                }
                
                transaction_count++;
                completed++;
                active_contexts.erase(active_contexts.begin() + idx);
                
                // Periodic consistency check
                if (completed % CONSISTENCY_CHECK_INTERVAL == 0) {
                    if (!check_consistency("interleaved " + std::to_string(completed))) {
                        std::cerr << "Consistency check failed after " << completed << " transactions" << std::endl;
                        break;
                    }
                }
            }
        }
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "Test completed in " << duration.count() << " ms" << std::endl;
        print_statistics();
    }
    
    void worker_thread(int thread_id, int num_transactions) {
        std::cout << "Thread " << thread_id << " starting..." << std::endl;
        
        for (int i = 0; i < num_transactions && test_running; i++) {
            TransactionContext ctx = create_transaction_context();
            run_single_transaction(ctx);
        }
        
        std::cout << "Thread " << thread_id << " completed" << std::endl;
    }
    
    void run_multi_threaded_test(int num_threads, int transactions_per_thread) {
        std::cout << "\n=== Running Multi-Threaded Test ===" << std::endl;
        std::cout << "Starting " << num_threads << " threads, " 
                  << transactions_per_thread << " transactions each..." << std::endl;
        
        auto start_time = std::chrono::steady_clock::now();
        
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back(&KVTStressTest::worker_thread, this, i, transactions_per_thread);
        }
        
        // Monitor thread for consistency checks
        std::thread monitor([this, num_threads, transactions_per_thread]() {
            int target = num_threads * transactions_per_thread;
            while (test_running && transaction_count < target) {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                if (transaction_count > 0 && transaction_count % CONSISTENCY_CHECK_INTERVAL == 0) {
                    if (!check_consistency("multi-threaded " + std::to_string(transaction_count.load()))) {
                        std::cerr << "Consistency check failed!" << std::endl;
                        test_running = false;
                    }
                }
            }
        });
        
        for (auto& t : threads) {
            t.join();
        }
        
        test_running = false;
        monitor.join();
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "Test completed in " << duration.count() << " ms" << std::endl;
        print_statistics();
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
        
        // Reset counters
        transaction_count = 0;
        commit_count = 0;
        abort_count = 0;
    }
    
    void run_all_tests() {
        std::cout << "\n========================================" << std::endl;
        std::cout << "    KVT COMPREHENSIVE STRESS TEST" << std::endl;
        std::cout << "========================================" << std::endl;
        
        // Test 1: Non-interleaved
        run_single_non_interleaved_test(100);  // Full test
        
        // Final consistency check
        if (!check_consistency("after non-interleaved")) {
            std::cerr << "FATAL: Consistency violated after non-interleaved test!" << std::endl;
            return;
        }
        
        // Test 2: Interleaved
        run_single_interleaved_test(200);
        
        // Final consistency check
        if (!check_consistency("after interleaved")) {
            std::cerr << "FATAL: Consistency violated after interleaved test!" << std::endl;
            return;
        }
        
        // Test 3: Multi-threaded
        run_multi_threaded_test(4, 50);
        
        // Final consistency check
        if (!check_consistency("final")) {
            std::cerr << "FATAL: Final consistency check failed!" << std::endl;
            return;
        }
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "    ALL TESTS COMPLETED SUCCESSFULLY!" << std::endl;
        std::cout << "========================================" << std::endl;
    }
};

void test_implementation(KVTManagerWrapperInterface* wrapper, const std::string& impl_name) {
    std::cout << "\n\n##################################################" << std::endl;
    std::cout << "Testing implementation: " << impl_name << std::endl;
    std::cout << "##################################################" << std::endl;
    
    KVTStressTest test(wrapper);
    
    if (!test.initialize()) {
        std::cerr << "Failed to initialize test for " << impl_name << std::endl;
        return;
    }
    
    test.run_all_tests();
}

int main(int argc, char* argv[]) {
    std::cout << "KVT Memory Database Stress Test" << std::endl;
    std::cout << "================================" << std::endl;
    
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
    
    // Test KVTManagerWrapper2PL
    {
        std::cout << "\nTesting KVTManagerWrapper2PL..." << std::endl;
        KVTManagerWrapper2PL twopl_wrapper;
        test_implementation(&twopl_wrapper, "KVTManagerWrapper2PL");
    }
    
    std::cout << "\n\n=== ALL IMPLEMENTATIONS TESTED ===" << std::endl;
    return 0;
}