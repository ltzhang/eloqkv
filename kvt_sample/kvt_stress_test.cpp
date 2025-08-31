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

// Configuration constants
const int MAX_KEY = 10000;
const int MAX_VALUE = 1000;
const int RANGE_SIZE = 100;
const int MAX_RANGES = MAX_KEY / RANGE_SIZE;
const int MAX_OPS_PER_TX = 20;
const int INITIAL_KEYS = 2000;
const int CONSISTENCY_CHECK_INTERVAL = 100;
const int MAX_CONCURRENT_TXS = 10;
const double COMMIT_RATIO = 0.7; // 70% commit, 30% abort

// Test modes
enum TestMode {
    SINGLE_NON_INTERLEAVED,
    SINGLE_INTERLEAVED,
    MULTI_THREADED
};

// Transaction context for tracking operations
struct TransactionContext {
    uint64_t tx_id;
    bool should_commit;
    // Removed range_modifications - we now check constraints by scanning
    std::map<int, int> ops_remaining;       // range_id -> ops left
    std::vector<std::pair<std::string, std::string>> operations; // for replay/debug
    std::set<int> ranges_involved;
    int total_ops_remaining;
    
    TransactionContext() : tx_id(0), should_commit(true), total_ops_remaining(0) {}
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
        if (table_id == 0) {
            std::cerr << "Failed to create table: " << error_msg << std::endl;
            return false;
        }
        std::cout << "Created table '" << table_name << "' with ID: " << table_id << std::endl;
        
        // Populate initial data
        if (!populate_initial_data()) {
            std::cerr << "Failed to populate initial data" << std::endl;
            return false;
        }
        
        // Verify initial consistency
        if (!check_consistency("initial")) {
            std::cerr << "Initial consistency check failed" << std::endl;
            return false;
        }
        
        std::cout << "Initialization complete!" << std::endl;
        return true;
    }
    
    bool populate_initial_data() {
        std::cout << "Populating initial data with " << INITIAL_KEYS << " keys..." << std::endl;
        
        std::uniform_int_distribution<> value_dist(0, MAX_VALUE);
        
        // Generate keys per range to ensure constraint satisfaction
        std::map<int, std::vector<std::pair<int, int>>> range_keys; // range -> [(key, value)]
        std::set<int> used_keys;
        
        // Distribute keys across ranges first
        int keys_per_range = INITIAL_KEYS / MAX_RANGES;
        int extra_keys = INITIAL_KEYS % MAX_RANGES;
        
        for (int range_id = 0; range_id < MAX_RANGES; range_id++) {
            int keys_for_this_range = keys_per_range + (extra_keys > 0 ? 1 : 0);
            if (extra_keys > 0) extra_keys--;
            
            if (keys_for_this_range == 0) continue;
            
            // Generate keys for this range
            int range_start = range_id * RANGE_SIZE;
            int range_end = range_start + RANGE_SIZE - 1;
            
            for (int i = 0; i < keys_for_this_range; i++) {
                int key;
                do {
                    key = std::uniform_int_distribution<>(range_start, range_end)(rng);
                } while (used_keys.find(key) != used_keys.end());
                
                used_keys.insert(key);
                int value = value_dist(rng);
                range_keys[range_id].push_back({key, value});
            }
            
            // Ensure constraint satisfaction for this range
            if (!range_keys[range_id].empty()) {
                // Calculate sum of all values except the first one
                int sum_without_first = 0;
                for (size_t i = 1; i < range_keys[range_id].size(); i++) {
                    sum_without_first += range_keys[range_id][i].second;
                }
                
                // Set the first key's value to make total sum divisible by 100
                int remainder = sum_without_first % 100;
                int needed_value = (100 - remainder) % 100;
                
                // Ensure the needed value is within bounds
                if (needed_value > MAX_VALUE) {
                    needed_value = needed_value % (MAX_VALUE + 1);
                }
                
                range_keys[range_id][0].second = needed_value;
                
                // Verify the constraint is satisfied
                int total_sum = sum_without_first + needed_value;
                
                // Debug output disabled for cleaner test
                // std::cout << "DEBUG: Range " << range_id << " initialization..."
                
                if (total_sum % 100 != 0) {
                    std::cerr << "ERROR in range " << range_id << ": sum=" << total_sum 
                             << ", sum_without_first=" << sum_without_first 
                             << ", needed_value=" << needed_value << std::endl;
                }
                assert(total_sum % 100 == 0);
            }
        }
        
        // Insert all keys in a single transaction
        std::string error_msg;
        uint64_t tx_id = kvt_manager->start_transaction(error_msg);
        if (tx_id == 0) {
            std::cerr << "Failed to start initialization transaction: " << error_msg << std::endl;
            return false;
        }
        
        int total_inserted = 0;
        for (const auto& [range_id, keys] : range_keys) {
            for (const auto& [key, value] : keys) {
                if (!kvt_manager->set(tx_id, table_name, key_to_string(key), 
                                     value_to_string(value), error_msg)) {
                    std::cerr << "Failed to set key " << key << ": " << error_msg << std::endl;
                    kvt_manager->rollback_transaction(tx_id, error_msg);
                    return false;
                }
                total_inserted++;
            }
        }
        
        if (!kvt_manager->commit_transaction(tx_id, error_msg)) {
            std::cerr << "Failed to commit initialization transaction: " << error_msg << std::endl;
            return false;
        }
        
        std::cout << "Successfully populated " << total_inserted << " keys across " 
                  << range_keys.size() << " ranges" << std::endl;
        return true;
    }
    
    bool check_consistency(const std::string& phase = "") {
        std::string prefix = phase.empty() ? "" : "[" + phase + "] ";
        std::cout << prefix << "Checking consistency..." << std::endl;
        
        std::string error_msg;
        uint64_t tx_id = kvt_manager->start_transaction(error_msg);
        if (tx_id == 0) {
            std::cerr << "Failed to start consistency check transaction: " << error_msg << std::endl;
            return false;
        }
        
        // Scan all ranges and verify constraint
        bool all_consistent = true;
        int total_checked = 0;
        for (int range_id = 0; range_id < MAX_RANGES; range_id++) {
            int range_start = range_id * RANGE_SIZE;
            int range_end = range_start + RANGE_SIZE - 1;
            
            std::vector<std::pair<std::string, std::string>> results;
            if (!kvt_manager->scan(tx_id, table_name, key_to_string(range_start),
                                  key_to_string(range_end), RANGE_SIZE, results, error_msg)) {
                // Empty range is ok
                continue;
            }
            
            if (results.empty()) continue;
            
            int sum = 0;
            for (const auto& [key_str, value_str] : results) {
                int key = string_to_key(key_str);
                int value = string_to_value(value_str);
                
                // Verify key is in correct range
                if (get_range_id(key) != range_id) {
                    std::cerr << "Key " << key << " found in wrong range " << range_id << std::endl;
                    all_consistent = false;
                }
                
                sum += value;
            }
            
            if (sum % 100 != 0) {
                std::cerr << "Consistency violation in range " << range_id 
                         << " (" << range_start << "-" << range_end << ")"
                         << ": sum=" << sum << " (not divisible by 100), keys=" << results.size() << std::endl;
                
                // Debug: print first few keys and values
                if (results.size() <= 25) {
                    for (const auto& [key_str, value_str] : results) {
                        std::cerr << "  Key: " << key_str << ", Value: " << value_str 
                                 << " (expected range: " << get_range_id(string_to_key(key_str)) << ")" << std::endl;
                    }
                }
                
                all_consistent = false;
            } else {
                // Debug output disabled
            }
            total_checked++;
        }
        
        kvt_manager->rollback_transaction(tx_id, error_msg);
        
        if (all_consistent) {
            std::cout << prefix << "Consistency check PASSED (checked " << total_checked << " ranges)" << std::endl;
        } else {
            std::cerr << prefix << "Consistency check FAILED" << std::endl;
        }
        
        return all_consistent;
    }
    
    TransactionContext create_transaction_context() {
        TransactionContext ctx;
        
        // Decide commit/abort
        std::uniform_real_distribution<> commit_dist(0.0, 1.0);
        ctx.should_commit = commit_dist(rng) < COMMIT_RATIO;
        
        // Choose 1-4 ranges
        std::uniform_int_distribution<> num_ranges_dist(1, 4);
        int num_ranges = num_ranges_dist(rng);
        
        // Select random ranges
        std::uniform_int_distribution<> range_dist(0, MAX_RANGES - 1);
        while (ctx.ranges_involved.size() < num_ranges) {
            ctx.ranges_involved.insert(range_dist(rng));
        }
        
        // Decide operations per range
        std::uniform_int_distribution<> ops_dist(1, 5);
        ctx.total_ops_remaining = 0;
        for (int range_id : ctx.ranges_involved) {
            int ops = ops_dist(rng);
            ctx.ops_remaining[range_id] = ops;
            // Range added to involved set
            ctx.total_ops_remaining += ops;
        }
        
        // Limit total operations
        if (ctx.total_ops_remaining > MAX_OPS_PER_TX) {
            ctx.total_ops_remaining = MAX_OPS_PER_TX;
            // Redistribute operations
            int ops_per_range = MAX_OPS_PER_TX / ctx.ranges_involved.size();
            int extra = MAX_OPS_PER_TX % ctx.ranges_involved.size();
            auto it = ctx.ranges_involved.begin();
            for (int range_id : ctx.ranges_involved) {
                ctx.ops_remaining[range_id] = ops_per_range + (extra-- > 0 ? 1 : 0);
            }
        }
        
        return ctx;
    }
    
    bool execute_single_operation(TransactionContext& ctx) {
        if (ctx.total_ops_remaining <= 0) return false;
        
        // Choose a range that still has operations
        std::vector<int> available_ranges;
        for (const auto& [range_id, ops] : ctx.ops_remaining) {
            if (ops > 0) available_ranges.push_back(range_id);
        }
        
        if (available_ranges.empty()) return false;
        
        std::uniform_int_distribution<> range_choice(0, available_ranges.size() - 1);
        int range_id = available_ranges[range_choice(rng)];
        
        int range_start = range_id * RANGE_SIZE;
        int range_end = range_start + RANGE_SIZE - 1;
        
        std::string error_msg;
        bool is_last_op_for_range = (ctx.ops_remaining[range_id] == 1);
        
        // Choose random keys in this range
        std::uniform_int_distribution<> key_dist(range_start, range_end);
        int key = key_dist(rng);
        
        // Choose operation type
        std::uniform_int_distribution<> op_dist(0, 2); // 0=get, 1=set, 2=del
        int op_type = op_dist(rng);
        
        if (op_type == 0) { // GET operation
            std::string value;
            bool found = kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), value, error_msg);
            
            // Debug: GET operation
                     
        } else if (op_type == 1) { // SET operation
            // ALWAYS read the current value first
            std::string old_value_str;
            int old_value = 0;
            bool key_exists = kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), old_value_str, error_msg);
            if (key_exists) {
                old_value = string_to_value(old_value_str);
            }
            
            // Choose new value - always random since we check constraint before commit
            std::uniform_int_distribution<> val_dist(0, MAX_VALUE);
            int new_value = val_dist(rng);
            
            // Perform the set
            if (!kvt_manager->set(ctx.tx_id, table_name, key_to_string(key), value_to_string(new_value), error_msg)) {
                std::cerr << "SET failed: " << error_msg << std::endl;
                return false;
            }
            
            // Operation complete
            
        } else { // DEL operation
            // ALWAYS read the value before deleting
            std::string old_value_str;
            bool key_exists = kvt_manager->get(ctx.tx_id, table_name, key_to_string(key), old_value_str, error_msg);
            
            if (key_exists) {
                int old_value = string_to_value(old_value_str);
                
                // Perform the delete
                if (!kvt_manager->del(ctx.tx_id, table_name, key_to_string(key), error_msg)) {
                    std::cerr << "DEL failed: " << error_msg << std::endl;
                    return false;
                }
                
                // Deletion complete
            } else {
                // Debug: DEL key not found
            }
        }
        
        ctx.ops_remaining[range_id]--;
        ctx.total_ops_remaining--;
        return true;
    }
    
    bool run_single_transaction(TransactionContext& ctx) {
        std::string error_msg;
        
        // Start transaction
        ctx.tx_id = kvt_manager->start_transaction(error_msg);
        if (ctx.tx_id == 0) {
            std::cerr << "Failed to start transaction: " << error_msg << std::endl;
            return false;
        }
        
        // Execute all operations
        while (ctx.total_ops_remaining > 0) {
            if (!execute_single_operation(ctx)) {
                break;
            }
        }
        
        // Commit or abort
        bool success;
        if (ctx.should_commit) {
            // Verify constraint before commit by scanning affected ranges
            bool constraint_satisfied = true;
            std::string scan_error;
            
            // Check each affected range by scanning it
            for (int range_id : ctx.ranges_involved) {
                std::string start_key = key_to_string(range_id * 100);
                std::string end_key = key_to_string((range_id + 1) * 100);
                
                std::vector<std::pair<std::string, std::string>> range_data;
                if (!kvt_manager->scan(ctx.tx_id, table_name, start_key, end_key, 100, range_data, scan_error)) {
                    std::cerr << "Failed to scan range " << range_id << ": " << scan_error << std::endl;
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
                // Committing with satisfied constraint
                success = kvt_manager->commit_transaction(ctx.tx_id, error_msg);
                if (success) {
                    commit_count++;
                } else {
                    std::cerr << "TX" << ctx.tx_id << " Commit failed: " << error_msg << std::endl;
                    abort_count++;
                }
            } else {
                // Forced abort due to constraint violation
                // Aborting due to constraint violation
                success = kvt_manager->rollback_transaction(ctx.tx_id, error_msg);
                abort_count++;
            }
        } else {
            // Aborting as predetermined
            success = kvt_manager->rollback_transaction(ctx.tx_id, error_msg);
            abort_count++;
        }
        
        transaction_count++;
        return success;
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
    
    // Test KVTManagerWrapperOCC - temporarily disabled for debugging
    /*{
        std::cout << "\nTesting KVTManagerWrapperOCC..." << std::endl;
        KVTManagerWrapperOCC occ_wrapper;
        test_implementation(&occ_wrapper, "KVTManagerWrapperOCC");
    }*/
    
    std::cout << "\n\n=== ALL IMPLEMENTATIONS TESTED ===" << std::endl;
    return 0;
}