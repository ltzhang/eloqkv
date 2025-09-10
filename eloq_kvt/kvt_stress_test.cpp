#include "kvt_inc.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <cassert>
#include <atomic>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <set>
#include <cstdlib>
#include <iomanip>
#include <mutex>

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

// Logging and crash simulation configuration
const bool ENABLE_LOGGING = true;  // Enable/disable logging
const bool ENABLE_CRASH_SIMULATION = true;  // Enable random crashes
const double CRASH_PROBABILITY = 0.01;  // Probability of crash per transaction (1% chance)
const int MIN_TXS_BEFORE_CRASH = 50;  // Minimum transactions before allowing crash 

// Test modes
enum TestMode {
    SINGLE_NON_INTERLEAVED,
    SINGLE_INTERLEAVED,
    MULTI_THREADED
};

#define PANIC(x) {x; exit(1);}

// Thread-safe random number generator
class ThreadSafeRNG {
private:
    std::mt19937 rng;
    std::mutex rng_mutex;
    
public:
    ThreadSafeRNG(unsigned int seed) : rng(seed) {}
    
    int rand_int(int min, int max) {
        std::lock_guard<std::mutex> lock(rng_mutex);
        std::uniform_int_distribution<> dist(min, max);
        return dist(rng);
    }
    
    double rand_double(double min, double max) {
        std::lock_guard<std::mutex> lock(rng_mutex);
        std::uniform_real_distribution<> dist(min, max);
        return dist(rng);
    }
    
    bool rand_bool(double probability) {
        return rand_double(0.0, 1.0) < probability;
    }
};

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
        Operation(int op_type, int key, int key2, int value, int value2) : op_type(op_type), key(key), key2(key2), value(value), value2(value2) {}
    };
    uint64_t tx_id;
    bool should_commit;
    int num_ops;
    std::vector<int> ranges; 
    ThreadSafeRNG* rng; // Thread-safe RNG for this transaction

private:    
    std::vector<Operation> operations; // for replay/debug

public:
    void append_op(const Operation& op) {
        operations.push_back(op);
        //print_op_list();
    }

    Operation & op(int index) {
        return operations[index];
    }

    std::vector<Operation> & ops() {
        return operations;
    }

    void print_op_list() {
        int i = 0; 
        std::cout << "TX   " << tx_id << " Op List: " << std::endl;
        for (const auto& op : operations) {
            std::string op_type_str;
            switch (op.op_type) {
                case Operation::OP_GET:
                    op_type_str = "GET";
                    break;
                case Operation::OP_SET:
                    op_type_str = "SET";
                    break;
                case Operation::OP_DEL:
                    op_type_str = "DEL";
                    break;
                case Operation::OP_SCAN:
                    op_type_str = "SCAN";
                    break;
                default:
                    op_type_str = "UNKNOWN";
                    break;
            }
            std::cout << "\tOp " << i <<" :" << op_type_str << " Key: " << op.key << " Value: " << op.value << std::endl;
            i++;
        }
    }
};

class KVTStressTest {
private:
    ThreadSafeRNG main_rng;
    std::string table_name;
    uint64_t table_id;
    std::atomic<int> transaction_count{0};
    std::atomic<int> commit_count{0};
    std::atomic<int> abort_count{0};
    std::atomic<bool> test_running{true};
    std::atomic<int> total_transaction_count{0};  // Total across all runs
    std::atomic<int> crash_count{0};  // Number of simulated crashes
    
    // Helper functions
    int get_range_id(int key) {
        return key / RANGE_SIZE;
    }
    
    KVTKey key_to_string(int key) {
        // Zero-pad to 5 digits so string comparison matches numerical order
        // MAX_KEY is 10000, so we need 5 digits
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(5) << key;
        return KVTKey(ss.str());
    }
    
    std::string value_to_string(int value) {
        return std::to_string(value);
    }
    
    int string_to_value(const std::string& str) {
        return std::stoi(str);
    }
    
    int string_to_key(const KVTKey& str) {
        return std::stoi(static_cast<const std::string&>(str));
    }
    
    // Ensure value stays within bounds
    int clamp_value(int value) {
        assert (MAX_VALUE % CONSTRAINT_DIVISOR == 0);
        return value % MAX_VALUE;
    }
    
    void simulate_crash_and_recovery() {
        crash_count++;
        std::cout << "\n*** SIMULATING CRASH #" << crash_count << " ***" << std::endl;
        std::cout << "Total transactions before crash: " << total_transaction_count << std::endl;
        
        // Shutdown without explicit cleanup (simulating crash)
        kvt_shutdown();
        
        // Restart the system (will load from checkpoint + log)
        std::cout << "*** RESTARTING SYSTEM (Recovery) ***" << std::endl;
        KVTError init_result = kvt_initialize();
        if (init_result != KVTError::SUCCESS) {
            PANIC(std::cerr << "Failed to reinitialize after crash!" << std::endl);
        }
        
        // Re-get the table ID
        std::string error_msg;
        KVTError result = kvt_get_table_id(table_name, table_id, error_msg);
        if (result != KVTError::SUCCESS) {
            // Table might not exist yet if we crashed before creating it
            result = kvt_create_table(table_name, "hash", table_id, error_msg);
            if (result != KVTError::SUCCESS && result != KVTError::TABLE_ALREADY_EXISTS) {
                PANIC(std::cerr << "Failed to recreate table after recovery: " << error_msg << std::endl);
            }
        }
        
        std::cout << "*** RECOVERY COMPLETE ***" << std::endl;
        
        // Verify consistency after recovery
        if (!check_consistency("after crash recovery")) {
            PANIC(std::cerr << "FATAL: Consistency violated after crash recovery!" << std::endl);
        }
        std::cout << "Consistency verified after recovery" << std::endl;
    }
    
    bool should_simulate_crash() {
        if (!ENABLE_CRASH_SIMULATION) return false;
        if (total_transaction_count < MIN_TXS_BEFORE_CRASH) return false;
        
        double rand_val = main_rng.rand_double(0.0, 1.0);
        return rand_val < CRASH_PROBABILITY;
    }
    
    void check_and_simulate_crash_on_operation() {
        // Check for crash after each operation (not just at transaction boundaries)
        if (should_simulate_crash()) {
            simulate_crash_and_recovery();
            // After crash, we need to throw an exception to stop current execution
            throw std::runtime_error("Simulated crash during operation");
        }
    }
    
public:
    KVTStressTest() : main_rng(42) {  // Fixed seed for debugging
        table_name = "stress_test_table";
    }
    
    bool initialize() {
        std::cout << "Initializing stress test..." << std::endl;
        
        // Create table
        std::string error_msg;
        KVTError result = kvt_create_table(table_name, "range", table_id, error_msg);
        if (result != KVTError::SUCCESS)
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
            int range_sum = 0; 
            std::unordered_map<int, int> key_values; 
            for (int i = 0; i < keys_each_range; i++) {
                int key = range_id * RANGE_SIZE + main_rng.rand_int(0, RANGE_SIZE - 1); 
                int value = main_rng.rand_int(0, MAX_VALUE - 1); 
                if (key_values.find(key) != key_values.end()) {
                    i--; 
                    continue; 
                }
                key_values[key] = value;
                range_sum += value;
            }
            int diff = CONSTRAINT_DIVISOR - range_sum % CONSTRAINT_DIVISOR; 
            key_values.begin()->second += diff;
            for (auto [key, value] : key_values) {
                KVTError result = kvt_set(0, table_id, key_to_string(key), value_to_string(value), error_msg);
                if (result != KVTError::SUCCESS) {
                    std::cerr << "Failed to populate initial data: " << error_msg << std::endl;
                }
            }
        }
    }
    
    bool check_consistency(const std::string& context = "") {
        std::cout << "Checking consistency: " << context << std::endl;
        std::string error_msg;
        uint64_t tx_id = 0;
        // Scan entire key range
        std::vector<std::pair<KVTKey, std::string>> all_results;
        KVTError result = kvt_scan(tx_id, table_id, key_to_string(0), 
                               key_to_string(MAX_KEY), MAX_KEY, all_results, error_msg);
        if (result != KVTError::SUCCESS)
            PANIC(std::cerr << "Failed to scan database: " << error_msg << std::endl;);
        // Check constraints - results are sorted
        int current_range = -1;
        int current_range_sum = 0;
        for (const auto& [key_str, value_str] : all_results) {
            int key = string_to_key(key_str);
            int value = string_to_value(value_str);
            int range_id = get_range_id(key);
            // If we moved to a new range, check the previous range's sum
            if (range_id != current_range) {
                if (current_range != -1 && current_range_sum > 0 && current_range_sum % CONSTRAINT_DIVISOR != 0)
                    PANIC(std::cerr << "Consistency violation in range " << current_range
                                 << ": sum=" << current_range_sum << " (not divisible by 100)" << std::endl;);
                // Start new range
                current_range = range_id;
                current_range_sum = 0;
            }
            current_range_sum += value;
        }
        // Check the last range
        if (current_range != -1 && current_range_sum > 0 && current_range_sum % CONSTRAINT_DIVISOR != 0) 
            PANIC(std::cerr << "Consistency violation in range " << current_range
                         << ": sum=" << current_range_sum << " (not divisible by 100)" << std::endl;);
        return true;
    }
    
    TransactionContext create_transaction_context() {
        TransactionContext ctx;
        std::string error_msg;
        KVTError result = kvt_start_transaction(ctx.tx_id, error_msg);
        if (result != KVTError::SUCCESS) 
            PANIC(std::cerr << "Failed to start transaction: " << error_msg << std::endl;);
        ctx.should_commit = main_rng.rand_bool(COMMIT_RATIO);
        ctx.num_ops = main_rng.rand_int(MIN_OPS_PER_TX, MAX_OPS_PER_TX);
        int num_ranges = main_rng.rand_int(1, MAX_RANGES_PER_TX);
        // Select random ranges
        for (int i = 0; i < num_ranges; i++) {
            int range_id = main_rng.rand_int(0, MAX_RANGES - 1); //can repeat
            ctx.ranges.push_back(range_id);
        }
        // Create thread-safe RNG for this transaction
        ctx.rng = new ThreadSafeRNG(main_rng.rand_int(1, 1000000));
        return ctx;
    }

    int try_get_key(TransactionContext& ctx, int range_id, bool check_previous) {
        //otherwise, choose a new key, and get its value
        std::string value;
        std::string error_msg;
        if (check_previous && ctx.ops().size() > 0 && 
              ctx.ops().back().op_type == TransactionContext::Operation::OP_GET) {
            int key = ctx.ops().back().key; //if previous op is a get, we can use the key
            return key;
        }
        for (int i = 0; i < 10; i++) { //try 10 times
            int key = rand() % RANGE_SIZE + range_id * RANGE_SIZE;
            KVTError result = kvt_get(ctx.tx_id, table_id, key_to_string(key), value, error_msg);
            if (result != KVTError::SUCCESS) {
                if (result == KVTError::KEY_IS_LOCKED ||
                    result == KVTError::KEY_NOT_FOUND ||
                    result == KVTError::KEY_IS_DELETED) {
                    continue; //just try another key
                }
                PANIC(std::cerr << "Failed to get key: " << error_msg << std::endl;)
            }
            //success, we do this and break, otherwise, after 10 times we will skip
            int value_int = string_to_value(value);
            //append to the operations
            ctx.append_op({TransactionContext::Operation::OP_GET, key, 0,  value_int, 0});
            return key;
        }
        return -1;
    }

    //return false if no more operations to execute
    void execute_single_operation(TransactionContext& ctx) {
        int rindex = ctx.rng->rand_int(0, ctx.ranges.size() - 1);
        int range_id = ctx.ranges[rindex];
        //randomly choose a op to execute according to the probability
        std::string error_msg;
        //first choose a range from ctx.ranges
        double prob = ctx.rng->rand_double(0.0, 1.0);
        std::string value;
        if (prob < 0.4) { //get
            //choose a key from the range
            try_get_key(ctx, range_id, false);
        } 
        else if (prob < 0.8) { //set
            //scan the ops to see whether we have already got a key's value. 
            int key = try_get_key(ctx, range_id, true);
            if (key == -1) //skip this one, cannot find key
                return;
            //choose a new value
            int new_value = clamp_value(ctx.rng->rand_int(0, MAX_VALUE - 1));
            KVTError result = kvt_set(ctx.tx_id, table_id, key_to_string(key), value_to_string(new_value), error_msg);
            if (result != KVTError::SUCCESS)
                PANIC(std::cerr << "Failed to set key: " << error_msg << std::endl;);
            ctx.append_op({TransactionContext::Operation::OP_SET, key, 0,  new_value, 0});
            check_and_simulate_crash_on_operation();  // Check for crash after operation
        } 
        else if (prob < 0.9) { //del
            int key = try_get_key(ctx, range_id, true);
            if (key == -1) //skip this one, cannot find key
                return;
            KVTError result = kvt_del(ctx.tx_id, table_id, key_to_string(key), error_msg);
            if (result != KVTError::SUCCESS)
                PANIC(std::cerr << "Failed to delete key: " << error_msg << std::endl;);
            ctx.append_op({TransactionContext::Operation::OP_DEL, key, 0, 0, 0});
            check_and_simulate_crash_on_operation();  // Check for crash after operation
        } 
        else { //scan does not need to care about range
            int start_key = ctx.rng->rand_int(0, MAX_KEY - 1);
            int end_key = ctx.rng->rand_int(0, MAX_KEY - 1);
            if (start_key > end_key) {
                int tmp = start_key;
                start_key = end_key;
                end_key = tmp;
            }
            std::vector<std::pair<KVTKey, std::string>> results;
            KVTError result = kvt_scan(ctx.tx_id, table_id, key_to_string(start_key), key_to_string(end_key), RANGE_SIZE, results, error_msg);
            if (result != KVTError::SUCCESS)
                PANIC(std::cerr << "Failed to scan key: " << error_msg << std::endl;);
            //append to the operations
            ctx.append_op({TransactionContext::Operation::OP_SCAN, start_key, end_key, 0, static_cast<int>(results.size())});
        }
    }

    void fix_constraint(TransactionContext& ctx) {
        std::string error_msg;
        //now fix up the operations to make the constraint satisfied
        std::map<int, int> range_delta_sum;
        for (int range_id : ctx.ranges) {
            range_delta_sum[range_id] = 0;
        }
        //scan the ops to get the range_delta_sum, a mutation op(set, del) will always have a get before it. 
        for (size_t i = 0; i < ctx.ops().size(); i++) {
            if (ctx.op(i).op_type == TransactionContext::Operation::OP_SET) {
                assert (i > 0 && ctx.op(i-1).op_type == TransactionContext::Operation::OP_GET);
                int prev_value = ctx.op(i-1).value;
                int new_value = ctx.op(i).value;
                assert(range_delta_sum.find(ctx.op(i).key / RANGE_SIZE) != range_delta_sum.end());
                range_delta_sum[ctx.op(i).key / RANGE_SIZE] += new_value - prev_value;
            } else if (ctx.op(i).op_type == TransactionContext::Operation::OP_DEL) {
                assert (i > 0 && ctx.op(i-1).op_type == TransactionContext::Operation::OP_GET);
                int prev_value = ctx.op(i-1).value;
                assert(range_delta_sum.find(ctx.op(i).key / RANGE_SIZE) != range_delta_sum.end());
                range_delta_sum[ctx.op(i).key / RANGE_SIZE] -= prev_value;
            }
        }
        //now fix up the operations to make the constraint satisfied
        for (auto [range_id, delta_sum] : range_delta_sum) {
            int diff = (CONSTRAINT_DIVISOR - delta_sum % CONSTRAINT_DIVISOR) % CONSTRAINT_DIVISOR;
            if (diff != 0) { //we need to get a new value to add to the range
                bool done = false;
                while (!done) {
                    int key = rand() % RANGE_SIZE + range_id * RANGE_SIZE;
                    int existing_value = 0;
                    std::string value;
                    KVTError result = kvt_get(ctx.tx_id, table_id, key_to_string(key), value, error_msg);
                    if (result == KVTError::SUCCESS) {
                        ctx.append_op({TransactionContext::Operation::OP_GET, key, 0,  existing_value, 0});
                        existing_value = string_to_value(value);
                        int new_value = clamp_value(existing_value + diff);
                        result = kvt_set(ctx.tx_id, table_id, key_to_string(key), value_to_string(new_value), error_msg);
                        ctx.append_op({TransactionContext::Operation::OP_SET, key, 0,  new_value, 0});
                        done = true;
                    }
                    else if (result == KVTError::KEY_NOT_FOUND ||
                             result == KVTError::KEY_IS_DELETED) {
                        int new_value = diff; //existing value is 0
                        result = kvt_set(ctx.tx_id, table_id, key_to_string(key), value_to_string(new_value), error_msg);
                        ctx.append_op({TransactionContext::Operation::OP_SET, key, 0,  new_value, 0});
                        done = true;
                    }
                    else if (result == KVTError::KEY_IS_LOCKED) {
                        continue; //just chose another key
                    }
                    else {
                        PANIC(std::cerr << "Failed to get key: " << error_msg << std::endl;);
                    }
                }
            }
        }
    }
    
    bool run_single_transaction_normal(TransactionContext& ctx) {
        std::string error_msg;
        //total number of operations to execute will be larger than the predefined number
        //first execute the predefined number of operations, some may create more than 1 op.
        for (int i = 0; i < ctx.num_ops; i++) {
            execute_single_operation(ctx);
        }
        //add another op to fix the constraint
        if (ctx.should_commit) {
            fix_constraint(ctx);
            //commit
            KVTError result = kvt_commit_transaction(ctx.tx_id, error_msg);
            if (result == KVTError::SUCCESS) {
                commit_count++;
                return true;
            } 
            else if (result == KVTError::TRANSACTION_HAS_STALE_DATA) {
                //std::cerr << "TX" << ctx.tx_id << " Commit failed: " << error_msg << std::endl;
                abort_count++;
                return false;
            }
            else {
                PANIC(std::cerr << "Commit failed: " << error_msg << std::endl;);
            }
        } else {
            //abort
            KVTError result = kvt_rollback_transaction(ctx.tx_id, error_msg);
            assert(result == KVTError::SUCCESS);
            abort_count++;
        }
        return true;
    }

    void run_non_interleaved(int num_transactions) {
        std::cout << "\n=== Running Single-Threaded Non-Interleaved Test ===" << std::endl;
        std::cout << "Executing " << num_transactions << " transactions sequentially..." << std::endl;

        auto start_time = std::chrono::steady_clock::now();
        
        for (int i = 0; i < num_transactions; i++) {
            try {
                TransactionContext ctx = create_transaction_context();
                run_single_transaction_normal(ctx);
                transaction_count++;
                total_transaction_count++;
                
                // Check for crash simulation at transaction boundary
                if (should_simulate_crash()) {
                    simulate_crash_and_recovery();
                    // Reset local counters after crash
                    transaction_count = 0;
                    commit_count = 0;
                    abort_count = 0;
                }
            } catch (const std::runtime_error& e) {
                // Crash occurred during operation
                // Reset local counters after crash
                transaction_count = 0;
                commit_count = 0;
                abort_count = 0;
            }
            
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
    
    void run_interleaved(int num_transactions) {
        std::cout << "\n=== Running Single-Threaded Interleaved Test ===" << std::endl;
        std::cout << "Executing " << num_transactions << " transactions with interleaving..." << std::endl;
        
        auto start_time = std::chrono::steady_clock::now();
        
        std::vector<std::unique_ptr<std::pair<int, TransactionContext>>> active_contexts; //first number: steps executed. 
        int completed = 0;
        std::string error_msg;        
        while (completed < num_transactions) {
            // Start new transactions if below limit
            if (active_contexts.size() < MAX_CONCURRENT_TXS && 
                main_rng.rand_bool(0.7)) { // 70% chance to start new transaction
                TransactionContext ctx = create_transaction_context();
                active_contexts.push_back(std::make_unique<std::pair<int, TransactionContext>>(std::make_pair(0, ctx)));
            }
            
            if (active_contexts.empty()) continue;
            
            // Choose a random transaction to execute one operation
            int idx = main_rng.rand_int(0, active_contexts.size() - 1);
            TransactionContext& ctx = active_contexts[idx]->second;
            if (active_contexts[idx]->first == ctx.num_ops) {
                if (ctx.should_commit) {
                    fix_constraint(ctx);
                    KVTError result = kvt_commit_transaction(ctx.tx_id, error_msg);
                    if (result == KVTError::SUCCESS) {
                        commit_count++;
                    }
                    else if (result == KVTError::TRANSACTION_HAS_STALE_DATA) {
                        //std::cerr << "TX" << ctx.tx_id << " Commit failed: " << error_msg << std::endl;
                        abort_count++;
                    }
                    else {
                        PANIC(std::cerr << "Commit failed: " << error_msg << std::endl;);
                    }
                }
                else {
                    KVTError result = kvt_rollback_transaction(ctx.tx_id, error_msg);
                    assert(result == KVTError::SUCCESS);
                    abort_count++;
                }
                completed++;
                transaction_count++;
                total_transaction_count++;
                
                // Check for crash simulation
                if (should_simulate_crash()) {
                    // Clean up active contexts before crash
                    for (auto& ac : active_contexts) {
                        delete ac->second.rng;
                    }
                    active_contexts.clear();
                    
                    simulate_crash_and_recovery();
                    // Reset local counters after crash
                    transaction_count = 0;
                    commit_count = 0;
                    abort_count = 0;
                } else {
                    // Clean up RNG
                    delete ctx.rng;
                    active_contexts.erase(active_contexts.begin() + idx);
                }
                
                // Periodic consistency check
                if (completed % CONSISTENCY_CHECK_INTERVAL == 0) {
                    if (!check_consistency("interleaved " + std::to_string(completed))) {
                        std::cerr << "Consistency check failed after " << completed << " transactions" << std::endl;
                        break;
                    }
                }
            }
            else {
                execute_single_operation(active_contexts[idx]->second);
                active_contexts[idx]->first++;
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
            run_single_transaction_normal(ctx);
            transaction_count++;
            // Clean up RNG
            delete ctx.rng;
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
            while (test_running && transaction_count.load() < target) {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                if (transaction_count.load() > 0 && transaction_count.load() % CONSISTENCY_CHECK_INTERVAL == 0) {
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
        double commit_rate = (transaction_count.load() > 0) ? 
                            (100.0 * commit_count.load() / transaction_count.load()) : 0;
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
        std::cout << "    Logging: " << (ENABLE_LOGGING ? "ENABLED" : "DISABLED") << std::endl;
        std::cout << "    Crash Simulation: " << (ENABLE_CRASH_SIMULATION ? "ENABLED" : "DISABLED") << std::endl;
        if (ENABLE_CRASH_SIMULATION) {
            std::cout << "    Crash Probability: " << (CRASH_PROBABILITY * 100) << "%" << std::endl;
            std::cout << "    Min TXs before crash: " << MIN_TXS_BEFORE_CRASH << std::endl;
        }
        std::cout << "========================================" << std::endl;
        
        // Test 1: Non-interleaved
        run_non_interleaved(ENABLE_CRASH_SIMULATION ? 100 : 100);  // Small test for crash simulation
        
        // Final consistency check
        if (!check_consistency("after non-interleaved")) {
            std::cerr << "FATAL: Consistency violated after non-interleaved test!" << std::endl;
            return;
        }
        
        // Test 2: Interleaved
        run_interleaved(ENABLE_CRASH_SIMULATION ? 1000 : 200);  // More transactions if crash testing
        
        // Final consistency check
        if (!check_consistency("after interleaved")) {
            std::cerr << "FATAL: Consistency violated after interleaved test!" << std::endl;
            return;
        }
        
        // Test 3: Multi-threaded (skip if crash simulation enabled for simplicity)
        if (!ENABLE_CRASH_SIMULATION) {
            run_multi_threaded_test(4, 500);
        } else {
            std::cout << "\n(Skipping multi-threaded test with crash simulation for simplicity)" << std::endl;
        }
        
        // Final consistency check
        if (!check_consistency("final")) {
            std::cerr << "FATAL: Final consistency check failed!" << std::endl;
            return;
        }
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "    ALL TESTS COMPLETED SUCCESSFULLY!" << std::endl;
        if (ENABLE_CRASH_SIMULATION) {
            std::cout << "    Total simulated crashes: " << crash_count << std::endl;
            std::cout << "    Total transactions: " << total_transaction_count << std::endl;
        }
        std::cout << "========================================" << std::endl;
    }
};

void test_implementation(const std::string& impl_name) {
    std::cout << "\n\n##################################################" << std::endl;
    std::cout << "Testing implementation: " << impl_name << std::endl;
    std::cout << "##################################################" << std::endl;
    
    KVTStressTest test;
    
    if (!test.initialize()) {
        std::cerr << "Failed to initialize test for " << impl_name << std::endl;
        return;
    }
    
    test.run_all_tests();
}

int main(int argc, char* argv[]) {
    std::cout << "KVT Memory Database Stress Test" << std::endl;
    std::cout << "================================" << std::endl;
    
    // Configure checkpoint parameters for stress testing
    if (ENABLE_CRASH_SIMULATION) {
        kvt_set_persist_param(true, 500, 2, false);  // persist=true, log_size=500, keep_history=2, text_log=false
        std::cout << "Using small checkpoint parameters for crash testing" << std::endl;
    }
    
    // Initialize the KVT system
    KVTError init_result = kvt_initialize();
    if (init_result != KVTError::SUCCESS) {
        std::cerr << "Failed to initialize KVT system" << std::endl;
        return 1;
    }
    
    // Test with global API
    test_implementation("Global KVT API");
    
    // Shutdown the KVT system
    kvt_shutdown();
    
    std::cout << "\n\n=== ALL IMPLEMENTATIONS TESTED ===" << std::endl;
    return 0;
}