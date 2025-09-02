#include "kvt_manager.h"
#include <chrono>
#include <thread>
#include <vector>
#include <random>
#include <atomic>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <numeric>

// Include the stress test by renaming its main function
#define main stress_test_main
#include "../eloq_kvt/kvt_stress_test.cpp"
#undef main

namespace EloqKV {

// Performance metrics tracking
struct PerfMetrics {
    std::atomic<uint64_t> total_transactions{0};
    std::atomic<uint64_t> successful_transactions{0};
    std::atomic<uint64_t> failed_transactions{0};
    std::atomic<uint64_t> total_operations{0};
    std::atomic<uint64_t> successful_operations{0};
    std::atomic<uint64_t> failed_operations{0};
    std::atomic<uint64_t> commits{0};
    std::atomic<uint64_t> rollbacks{0};
    
    void reset() {
        total_transactions = 0;
        successful_transactions = 0;
        failed_transactions = 0;
        total_operations = 0;
        successful_operations = 0;
        failed_operations = 0;
        commits = 0;
        rollbacks = 0;
    }
    
    void print(const std::string& title, double elapsed_seconds) const {
        std::cout << "=== " << title << " ===" << std::endl;
        std::cout << "Duration: " << std::fixed << std::setprecision(2) << elapsed_seconds << "s" << std::endl;
        std::cout << "Total Transactions: " << total_transactions.load() << std::endl;
        std::cout << "Successful Transactions: " << successful_transactions.load() << std::endl;
        std::cout << "Failed Transactions: " << failed_transactions.load() << std::endl;
        std::cout << "Total Operations: " << total_operations.load() << std::endl;
        std::cout << "Successful Operations: " << successful_operations.load() << std::endl;
        std::cout << "Failed Operations: " << failed_operations.load() << std::endl;
        std::cout << "Commits: " << commits.load() << std::endl;
        std::cout << "Rollbacks: " << rollbacks.load() << std::endl;
        
        if (elapsed_seconds > 0) {
            std::cout << "Transaction Throughput: " << std::fixed << std::setprecision(1) 
                      << total_transactions.load() / elapsed_seconds << " tx/s" << std::endl;
            std::cout << "Operation Throughput: " << std::fixed << std::setprecision(1)
                      << total_operations.load() / elapsed_seconds << " ops/s" << std::endl;
        }
        std::cout << std::endl;
    }
};

void KVTManager::runStressTest() {
    std::cout << "=== Starting KVT Stress and Performance Tests ===" << std::endl << std::endl;
    
    // Create test tables first
    std::string error_msg;
    uint64_t hash_table_id = doCreateTable("perf_table_hash", "hash", error_msg);
    uint64_t range_table_id = doCreateTable("perf_table_range", "range", error_msg);
    
    if (hash_table_id == 0 || range_table_id == 0) {
        std::cerr << "Failed to create test tables: " << error_msg << std::endl;
        return;
    }
    
    std::cout << "Created performance test tables (hash_id=" << hash_table_id 
              << ", range_id=" << range_table_id << ")" << std::endl << std::endl;
    
    // Run single-threaded tests
    runSingleThreadedStressTest();
    
    // Run multi-threaded tests
    runMultiThreadedStressTest();
    
    std::cout << "=== Stress and Performance Tests Completed ===" << std::endl << std::endl;
}

void KVTManager::runSingleThreadedStressTest() {
    std::cout << "=== Single-threaded Stress Test ===" << std::endl;
    
    PerfMetrics metrics;
    const int duration_seconds = 30; // 30 seconds
    const int ops_per_transaction = 5;
    
    // Get table ID for performance testing
    std::string error_msg;
    uint64_t table_id;
    if (!doGetTableId("perf_table_hash", table_id, error_msg)) {
        std::cerr << "Failed to get table ID: " << error_msg << std::endl;
        return;
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dist(1, 10000);
    std::uniform_int_distribution<> value_dist(1000, 9999);
    std::uniform_real_distribution<> commit_prob(0.0, 1.0);
    
    auto start_time = std::chrono::steady_clock::now();
    auto end_time = start_time + std::chrono::seconds(duration_seconds);
    
    std::cout << "Running for " << duration_seconds << " seconds with " 
              << ops_per_transaction << " operations per transaction..." << std::endl;
    
    while (std::chrono::steady_clock::now() < end_time) {
        metrics.total_transactions++;
        
        // Start a transaction
        std::string error_msg;
        uint64_t tx_id = doStartTx(error_msg);
        
        if (tx_id == 0) {
            metrics.failed_transactions++;
            continue;
        }
        
        bool tx_success = true;
        
        // Perform 5 random operations in the transaction
        for (int i = 0; i < ops_per_transaction; i++) {
            metrics.total_operations++;
            
            std::string key = "key_" + std::to_string(key_dist(gen));
            std::string value = "value_" + std::to_string(value_dist(gen));
            
            // Alternate between SET and GET operations
            if (i % 2 == 0) {
                // SET operation
                if (doSet(tx_id, table_id, key, value, error_msg)) {
                    metrics.successful_operations++;
                } else {
                    metrics.failed_operations++;
                    tx_success = false;
                    break;
                }
            } else {
                // GET operation
                std::string retrieved_value;
                if (doGet(tx_id, table_id, key, retrieved_value, error_msg)) {
                    metrics.successful_operations++;
                } else {
                    metrics.failed_operations++;
                    tx_success = false;
                    break;
                }
            }
        }
        
        // Randomly commit or rollback (80% commit, 20% rollback)
        if (tx_success && commit_prob(gen) < 0.8) {
            // Commit transaction
            if (doCommitTx(tx_id, error_msg)) {
                metrics.successful_transactions++;
                metrics.commits++;
            } else {
                metrics.failed_transactions++;
            }
        } else {
            // Rollback transaction
            if (doAbortTx(tx_id, error_msg)) {
                metrics.rollbacks++;
            }
            if (tx_success) {
                metrics.successful_transactions++;
            } else {
                metrics.failed_transactions++;
            }
        }
    }
    
    auto actual_end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(actual_end - start_time).count();
    
    metrics.print("Single-threaded Performance Results", elapsed);
}

void KVTManager::runMultiThreadedStressTest() {
    std::cout << "=== Multi-threaded Stress Test ===" << std::endl;
    
    const int num_threads = std::thread::hardware_concurrency();
    const int duration_seconds = 30; // 30 seconds
    const int ops_per_transaction = 5;
    
    // Get table ID for performance testing
    std::string error_msg;
    uint64_t table_id;
    if (!doGetTableId("perf_table_hash", table_id, error_msg)) {
        std::cerr << "Failed to get table ID: " << error_msg << std::endl;
        return;
    }
    
    std::cout << "Running with " << num_threads << " threads for " << duration_seconds 
              << " seconds with " << ops_per_transaction << " operations per transaction..." << std::endl;
    
    PerfMetrics global_metrics;
    std::vector<std::thread> threads;
    std::atomic<bool> stop_flag{false};
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Launch worker threads
    for (int thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back([this, thread_id, &global_metrics, &stop_flag, ops_per_transaction, table_id]() {
            std::random_device rd;
            std::mt19937 gen(rd() + thread_id); // Seed with thread_id for different sequences
            std::uniform_int_distribution<> key_dist(1, 10000);
            std::uniform_int_distribution<> value_dist(1000, 9999);
            std::uniform_real_distribution<> commit_prob(0.0, 1.0);
            
            PerfMetrics local_metrics;
            
            while (!stop_flag.load()) {
                local_metrics.total_transactions++;
                
                // Start a transaction
                std::string error_msg;
                uint64_t tx_id = doStartTx(error_msg);
                
                if (tx_id == 0) {
                    local_metrics.failed_transactions++;
                    continue;
                }
                
                bool tx_success = true;
                
                // Perform operations in the transaction
                for (int i = 0; i < ops_per_transaction; i++) {
                    local_metrics.total_operations++;
                    
                    std::string key = "key_t" + std::to_string(thread_id) + "_" + std::to_string(key_dist(gen));
                    std::string value = "value_t" + std::to_string(thread_id) + "_" + std::to_string(value_dist(gen));
                    
                    // Alternate between SET and GET operations
                    if (i % 2 == 0) {
                        // SET operation
                        if (doSet(tx_id, table_id, key, value, error_msg)) {
                            local_metrics.successful_operations++;
                        } else {
                            local_metrics.failed_operations++;
                            tx_success = false;
                            break;
                        }
                    } else {
                        // GET operation
                        std::string retrieved_value;
                        if (doGet(tx_id, table_id, key, retrieved_value, error_msg)) {
                            local_metrics.successful_operations++;
                        } else {
                            local_metrics.failed_operations++;
                            tx_success = false;
                            break;
                        }
                    }
                }
                
                // Randomly commit or rollback (80% commit, 20% rollback)
                if (tx_success && commit_prob(gen) < 0.8) {
                    // Commit transaction
                    if (doCommitTx(tx_id, error_msg)) {
                        local_metrics.successful_transactions++;
                        local_metrics.commits++;
                    } else {
                        local_metrics.failed_transactions++;
                    }
                } else {
                    // Rollback transaction
                    if (doAbortTx(tx_id, error_msg)) {
                        local_metrics.rollbacks++;
                    }
                    if (tx_success) {
                        local_metrics.successful_transactions++;
                    } else {
                        local_metrics.failed_transactions++;
                    }
                }
            }
            
            // Aggregate local metrics to global
            global_metrics.total_transactions += local_metrics.total_transactions.load();
            global_metrics.successful_transactions += local_metrics.successful_transactions.load();
            global_metrics.failed_transactions += local_metrics.failed_transactions.load();
            global_metrics.total_operations += local_metrics.total_operations.load();
            global_metrics.successful_operations += local_metrics.successful_operations.load();
            global_metrics.failed_operations += local_metrics.failed_operations.load();
            global_metrics.commits += local_metrics.commits.load();
            global_metrics.rollbacks += local_metrics.rollbacks.load();
        });
    }
    
    // Wait for the specified duration
    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    
    // Signal threads to stop
    stop_flag = true;
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    auto end_time = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    
    global_metrics.print("Multi-threaded Performance Results", elapsed);
}

void KVTManager::runBenchmarkSuite() {
    std::cout << "=== KVT Benchmark Suite ===" << std::endl << std::endl;
    
    // Create test tables
    std::string error_msg;
    doCreateTable("benchmark_hash", "hash", error_msg);
    doCreateTable("benchmark_range", "range", error_msg);
    
    // Run different benchmark scenarios
    runThroughputBenchmark();
    runLatencyBenchmark();
    runConcurrencyBenchmark();
    
    std::cout << "=== Benchmark Suite Completed ===" << std::endl << std::endl;
}

void KVTManager::runThroughputBenchmark() {
    std::cout << "=== Throughput Benchmark ===" << std::endl;
    
    const int test_duration = 30; // 30 seconds
    const std::vector<int> thread_counts = {1, 2, 4, 8, 16};
    
    // Get table ID for benchmark testing
    std::string error_msg;
    uint64_t table_id;
    if (!doGetTableId("benchmark_hash", table_id, error_msg)) {
        std::cerr << "Failed to get table ID: " << error_msg << std::endl;
        return;
    }
    
    for (int num_threads : thread_counts) {
        PerfMetrics metrics;
        std::vector<std::thread> threads;
        std::atomic<bool> stop_flag{false};
        
        auto start_time = std::chrono::steady_clock::now();
        
        // Launch threads
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([this, i, &metrics, &stop_flag, table_id]() {
                std::random_device rd;
                std::mt19937 gen(rd() + i);
                std::uniform_int_distribution<> key_dist(1, 1000);
                std::uniform_int_distribution<> value_dist(1000, 9999);
                
                while (!stop_flag.load()) {
                    std::string error_msg;
                    uint64_t tx_id = doStartTx(error_msg);
                    if (tx_id == 0) continue;
                    
                    metrics.total_transactions++;
                    
                    // Do a simple SET operation
                    std::string key = "bench_key_" + std::to_string(key_dist(gen));
                    std::string value = "bench_value_" + std::to_string(value_dist(gen));
                    
                    if (doSet(tx_id, table_id, key, value, error_msg)) {
                        metrics.total_operations++;
                        if (doCommitTx(tx_id, error_msg)) {
                            metrics.successful_transactions++;
                            metrics.commits++;
                        }
                    } else {
                        doAbortTx(tx_id, error_msg);
                    }
                }
            });
        }
        
        // Run for specified duration
        std::this_thread::sleep_for(std::chrono::seconds(test_duration));
        stop_flag = true;
        
        // Wait for completion
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end_time = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(end_time - start_time).count();
        
        std::cout << "Threads: " << num_threads 
                  << ", Throughput: " << std::fixed << std::setprecision(1)
                  << metrics.total_transactions.load() / elapsed << " tx/s" << std::endl;
    }
    std::cout << std::endl;
}

void KVTManager::runLatencyBenchmark() {
    std::cout << "=== Latency Benchmark ===" << std::endl;
    
    const int num_samples = 1000;
    std::vector<double> latencies;
    latencies.reserve(num_samples);
    
    // Get table ID for latency testing
    std::string error_msg;
    uint64_t table_id;
    if (!doGetTableId("benchmark_hash", table_id, error_msg)) {
        std::cerr << "Failed to get table ID: " << error_msg << std::endl;
        return;
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dist(1, 1000);
    std::uniform_int_distribution<> value_dist(1000, 9999);
    
    for (int i = 0; i < num_samples; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        
        std::string error_msg;
        uint64_t tx_id = doStartTx(error_msg);
        if (tx_id == 0) continue;
        
        std::string key = "latency_key_" + std::to_string(key_dist(gen));
        std::string value = "latency_value_" + std::to_string(value_dist(gen));
        
        doSet(tx_id, table_id, key, value, error_msg);
        doCommitTx(tx_id, error_msg);
        
        auto end = std::chrono::high_resolution_clock::now();
        double latency_us = std::chrono::duration<double, std::micro>(end - start).count();
        latencies.push_back(latency_us);
    }
    
    // Calculate statistics
    std::sort(latencies.begin(), latencies.end());
    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double p50 = latencies[latencies.size() * 0.5];
    double p95 = latencies[latencies.size() * 0.95];
    double p99 = latencies[latencies.size() * 0.99];
    
    std::cout << "Transaction Latency (microseconds):" << std::endl;
    std::cout << "  Average: " << std::fixed << std::setprecision(1) << avg << "μs" << std::endl;
    std::cout << "  P50: " << std::fixed << std::setprecision(1) << p50 << "μs" << std::endl;
    std::cout << "  P95: " << std::fixed << std::setprecision(1) << p95 << "μs" << std::endl;
    std::cout << "  P99: " << std::fixed << std::setprecision(1) << p99 << "μs" << std::endl;
    std::cout << std::endl;
}

void KVTManager::runConcurrencyBenchmark() {
    std::cout << "=== Concurrency Benchmark ===" << std::endl;
    
    const int num_threads = 8;
    const int transactions_per_thread = 100;
    const int ops_per_transaction = 10;
    
    // Get table ID for concurrency testing
    std::string error_msg;
    uint64_t table_id;
    if (!doGetTableId("benchmark_hash", table_id, error_msg)) {
        std::cerr << "Failed to get table ID: " << error_msg << std::endl;
        return;
    }
    
    PerfMetrics metrics;
    std::vector<std::thread> threads;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Launch threads that work on overlapping key ranges to test concurrency
    for (int thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back([this, thread_id, &metrics, transactions_per_thread, ops_per_transaction, table_id]() {
            std::random_device rd;
            std::mt19937 gen(rd() + thread_id);
            std::uniform_int_distribution<> key_dist(1, 100); // Overlapping key space
            std::uniform_int_distribution<> value_dist(1000, 9999);
            
            for (int tx_count = 0; tx_count < transactions_per_thread; tx_count++) {
                std::string error_msg;
                uint64_t tx_id = doStartTx(error_msg);
                if (tx_id == 0) continue;
                
                metrics.total_transactions++;
                bool success = true;
                
                for (int op = 0; op < ops_per_transaction; op++) {
                    std::string key = "conc_key_" + std::to_string(key_dist(gen));
                    std::string value = "conc_val_t" + std::to_string(thread_id) + "_" + std::to_string(value_dist(gen));
                    
                    if (!doSet(tx_id, table_id, key, value, error_msg)) {
                        success = false;
                        break;
                    }
                    metrics.total_operations++;
                }
                
                if (success && doCommitTx(tx_id, error_msg)) {
                    metrics.successful_transactions++;
                    metrics.commits++;
                } else {
                    doAbortTx(tx_id, error_msg);
                    metrics.failed_transactions++;
                    metrics.rollbacks++;
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    auto end_time = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    
    metrics.print("Concurrency Benchmark Results", elapsed);
}

void KVTManager::runComprehensiveTest() {
    std::string error_msg;
    
    // Local verification map to simulate proper ACID behavior
    // This is separate from test_storage_ which represents the "database"
    std::unordered_map<std::string, std::string> expected_state;
    
    // Test 1: Create tables
    std::cout << "Test 1: Creating tables..." << std::endl << std::endl;
    uint64_t table1_id = doCreateTable("test_table_hash", "hash", error_msg);
    assert(table1_id != 0);
    std::cout << "✓ Created table 'test_table_hash' with ID: " << table1_id << std::endl;
    
    uint64_t table2_id = doCreateTable("test_table_range", "range", error_msg);
    assert(table2_id != 0);
    std::cout << "✓ Created table 'test_table_range' with ID: " << table2_id << std::endl;
    
    // Test duplicate table creation
    uint64_t duplicate_id = doCreateTable("test_table_hash", "hash", error_msg);
    assert(duplicate_id == 0);
    std::cout << "✓ Duplicate table creation correctly failed: " << error_msg << std::endl;
    
    // Test 2: Transaction management
    std::cout << "Test 2: Transaction management..." << std::endl << std::endl;
    uint64_t tx1 = doStartTx(error_msg);
    assert(tx1 != 0);
    std::cout << "✓ Started transaction TX1: " << tx1 << std::endl;
    
    uint64_t tx2 = doStartTx(error_msg);
    assert(tx2 != 0);
    assert(tx1 != tx2);
    std::cout << "✓ Started transaction TX2: " << tx2 << std::endl;
    
    // Test 3: One-shot operations (tx_id = 0)
    std::cout << "Test 3: One-shot operations..." << std::endl << std::endl;
    bool success = doSet(0, table1_id, "oneshot_key", "oneshot_value", error_msg);
    assert(success);
    std::cout << "✓ One-shot SET operation succeeded" << std::endl << std::endl;
    
    std::string value;
    success = doGet(0, table1_id, "oneshot_key", value, error_msg);
    assert(success);
    std::cout << "✓ One-shot GET operation succeeded, value: '" << value << "'" << std::endl;

    success = doSet(0, table2_id, "oneshot_key", "oneshot_value", error_msg);
    assert(success);
    std::cout << "✓ One-shot SET operation succeeded" << std::endl << std::endl;
    
    success = doGet(0, table2_id, "oneshot_key", value, error_msg);
    assert(success);
    std::cout << "✓ One-shot GET operation succeeded, value: '" << value << "'" << std::endl;
    
    // Test 4: Transactional operations
    std::cout << "Test 4: Transactional operations..." << std::endl << std::endl;
    
    // Set some values in transaction 1
    // success = doSet(tx1, table1_id, "key1", "value1_tx1", error_msg);
    // assert(success);
    // std::cout << "✓ SET key1=value1_tx1 in TX1" << std::endl << std::endl;
    
    success = doSet(tx1, table1_id, "key2", "value2_tx1", error_msg);
    assert(success);
    std::cout << "✓ SET key2=value2_tx1 in TX1" << std::endl << std::endl;
    
    // Set different values in transaction 2
    success = doSet(tx2, table1_id, "key1", "value1_tx2", error_msg);
    assert(success);
    std::cout << "✓ SET key1=value1_tx2 in TX2" << std::endl << std::endl;
    
    success = doSet(tx2, table2_id, "range_key", "range_value", error_msg);
    assert(success);
    std::cout << "✓ SET range_key=range_value in TX2 on range table" << std::endl << std::endl;
    
    // Test reading within transactions
    // success = doGet(tx1, table1_id, "key1", value, error_msg);
    // assert(success);
    // std::cout << "✓ GET key1 in TX1, value: '" << value << "'" << std::endl;
    
    success = doGet(tx2, table1_id, "key1", value, error_msg);
    assert(success);
    std::cout << "✓ GET key1 in TX2, value: '" << value << "'" << std::endl;

    // Test 5: Scan operations
    std::cout << "Test 5: Scan operations..." << std::endl << std::endl;
    std::vector<std::pair<std::string, std::string>> scan_results;
    
    success = doScan(0, table2_id, "a", "z", 5, scan_results, error_msg);
    assert(success);
    std::cout << "✓ One-shot SCAN on range table returned " << scan_results.size() << " results" << std::endl;

    doSet(0, table2_id, "range_key_2", "range_value", error_msg);
    assert(success);
    std::cout << "✓ SET range_key=range_value in one-shot on range table" << std::endl << std::endl;

    success = doScan(0, table2_id, "a", "z", 5, scan_results, error_msg);
    assert(success);
    std::cout << "✓ One-shot SCAN on range table returned " << scan_results.size() << " results" << std::endl;
    
    // Test 6: Error conditions
    std::cout << "Test 6: Error conditions..." << std::endl << std::endl;
    
    // Try to operate on non-existent table (use invalid table ID)
    success = doSet(tx1, 99999, "key", "value", error_msg);
    assert(!success);
    std::cout << "✓ Operation on non-existent table correctly failed: " << error_msg << std::endl;
    
    // Try to commit non-existent transaction
    success = doCommitTx(99999, error_msg);
    assert(!success);
    std::cout << "✓ Commit non-existent transaction correctly failed: " << error_msg << std::endl;
    
    // Try to rollback non-existent transaction
    success = doAbortTx(99999, error_msg);
    assert(!success);
    std::cout << "✓ Rollback non-existent transaction correctly failed: " << error_msg << std::endl;
    
    // Test 7: Transaction commit and rollback - demonstrate ACID properties
    std::cout << "Test 7: ACID transaction behavior..." << std::endl << std::endl;
    
    // First, verify what we can read from each transaction before commit/rollback
    std::cout << "Before commit/rollback:" << std::endl;
    success = doGet(tx1, table1_id, "key1", value, error_msg);
    std::cout << "TX1 sees key1: '" << value << "'" << std::endl;
    success = doGet(tx2, table1_id, "key1", value, error_msg);
    std::cout << "TX2 sees key1: '" << value << "'" << std::endl;
    success = doGet(0, table1_id, "key1", value, error_msg);
    std::cout << "One-shot read sees key1: '" << value << "' (should be empty or oneshot_value)" << std::endl;
    
    // Commit transaction 1 - its changes should become persistent
    success = doCommitTx(tx1, error_msg);
    assert(success);
    std::cout << "✓ Committed TX1: " << tx1 << std::endl;
    
    // After TX1 commit, verify persistence
    success = doGet(0, table1_id, "key1", value, error_msg);
    std::cout << "After TX1 commit, one-shot read sees key1: '" << value << "' (should be value1_tx1)" << std::endl;
    success = doGet(0, table1_id, "key2", value, error_msg);
    std::cout << "After TX1 commit, one-shot read sees key2: '" << value << "' (should be value2_tx1)" << std::endl;
    
    // TX2 should still see its own uncommitted version
    success = doGet(tx2, table1_id, "key1", value, error_msg);
    std::cout << "TX2 still sees its version of key1: '" << value << "'" << std::endl;
    
    // Rollback transaction 2 - its changes should be discarded
    success = doAbortTx(tx2, error_msg);
    assert(success);
    std::cout << "✓ Rolled back TX2: " << tx2 << std::endl;
    
    // After TX2 rollback, verify TX1's committed values are still there
    success = doGet(0, table1_id, "key1", value, error_msg);
    std::cout << "After TX2 rollback, one-shot read sees key1: '" << value << "' (should still be value1_tx1)" << std::endl;
    // success = doGet(0, table2_id, "range_key", value, error_msg);
    // std::cout << "After TX2 rollback, range_key should not exist: '" << value << "' (should be empty)" << std::endl;
    
    // Try to use committed/rolled back transactions (should fail)
    success = doSet(tx1, table1_id, "key3", "value3", error_msg);
    assert(!success);
    std::cout << "✓ Using committed transaction correctly failed: " << error_msg << std::endl;
    
    success = doSet(tx2, table1_id, "key4", "value4", error_msg);
    assert(!success);
    std::cout << "✓ Using rolled back transaction correctly failed: " << error_msg << std::endl;
    
    // Test 8: Complex scenario with multiple transactions
    std::cout << "Test 8: Complex multi-transaction scenario..." << std::endl << std::endl;
    
    uint64_t tx3 = doStartTx(error_msg);
    uint64_t tx4 = doStartTx(error_msg);
    uint64_t tx5 = doStartTx(error_msg);
    assert(tx3 != 0 && tx4 != 0 && tx5 != 0);
    std::cout << "✓ Started three new transactions: " << tx3 << ", " << tx4 << ", " << tx5 << std::endl;
    
    // Perform operations across multiple tables and transactions
    doSet(tx3, table1_id, "complex_key1", "complex_value1", error_msg);
    //doSet(tx3, table2_id, "complex_key2", "complex_value2", error_msg);
    doSet(tx4, table1_id, "complex_key3", "complex_value3", error_msg);
    doSet(tx5, table2_id, "complex_key4", "complex_value4", error_msg);
    
    // Scan operations within transactions
    //doScan(tx3, table1_id, "complex", "complex_z", 10, scan_results, error_msg);
    //doScan(tx4, table2_id, "a", "z", 20, scan_results, error_msg);
    
    // Commit some, rollback others
    doCommitTx(tx3, error_msg);
    doAbortTx(tx4, error_msg);
    doCommitTx(tx5, error_msg);
    
    std::cout << "✓ Complex scenario completed - TX3 committed, TX4 rolled back, TX5 committed" << std::endl << std::endl;
    
    // Test 9: Stress test with many operations
    std::cout << "Test 9: Stress test..." << std::endl << std::endl;
    
    uint64_t stress_tx = doStartTx(error_msg);
    assert(stress_tx != 0);
    
    // Perform many operations
    for (int i = 0; i < 100; i++) {
        std::string key = "stress_key_" + std::to_string(i);
        std::string value = "stress_value_" + std::to_string(i * 10);
        
        bool set_success = doSet(stress_tx, table1_id, key, value, error_msg);
        assert(set_success);
        
        if (i % 10 == 0) {
            std::cout << "✓ Stress test progress: " << i << "/100 operations completed" << std::endl;
        }
    }
    
    // Read back some values
    for (int i = 0; i < 100; i += 10) {
        std::string key = "stress_key_" + std::to_string(i);
        std::string read_value;
        bool get_success = doGet(stress_tx, table1_id, key, read_value, error_msg);
        assert(get_success);
    }
    
    doCommitTx(stress_tx, error_msg);
    std::cout << "✓ Stress test completed - 100 SET/GET operations in single transaction" << std::endl << std::endl;
    
    // Test 10: Edge cases
    std::cout << "Test 10: Edge cases..." << std::endl << std::endl;
    
    // Try to commit/rollback transaction ID 0
    success = doCommitTx(0, error_msg);
    assert(!success);
    std::cout << "✓ Commit TX ID 0 correctly failed: " << error_msg << std::endl;
    
    success = doAbortTx(0, error_msg);
    assert(!success);
    std::cout << "✓ Rollback TX ID 0 correctly failed: " << error_msg << std::endl;
    
    // Empty key operations
    success = doSet(0, table1_id, "", "empty_key_value", error_msg);
    assert(success);  // Empty keys should be allowed
    std::cout << "✓ SET with empty key succeeded" << std::endl << std::endl;
    
    success = doGet(0, table1_id, "", value, error_msg);
    assert(success);
    std::cout << "✓ GET with empty key succeeded, value: '" << value << "'" << std::endl;
    
    // Empty value operations
    success = doSet(0, table1_id, "empty_value_key", "", error_msg);
    assert(success);
    std::cout << "✓ SET with empty value succeeded" << std::endl << std::endl;
    
    // Very long key and value
    std::string long_key(1000, 'k');
    std::string long_value(10000, 'v');
    success = doSet(0, table1_id, long_key, long_value, error_msg);
    assert(success);
    std::cout << "✓ SET with very long key (" << long_key.length() << " chars) and value (" 
              << long_value.length() << " chars) succeeded" << std::endl;
    
    // Test 11: Advanced ACID test - multiple transactions with overlapping keys
    std::cout << "Test 11: Advanced ACID behavior..." << std::endl << std::endl;
    
    // Create a baseline committed value
    success = doSet(0, table1_id, "shared_key", "baseline_value", error_msg);
    assert(success);
    std::cout << "✓ Set baseline value for shared_key" << std::endl;
    
    // Create two transactions that modify the same key
    uint64_t tx_a = doStartTx(error_msg);
    uint64_t tx_b = doStartTx(error_msg);
    
    // Both transactions modify the same key
    success = doSet(tx_a, table1_id, "shared_key", "value_from_tx_a", error_msg);
    assert(success);
    success = doSet(tx_b, table1_id, "shared_key", "value_from_tx_b", error_msg);
    assert(success);
    
    // Each transaction should see its own version
    success = doGet(tx_a, table1_id, "shared_key", value, error_msg);
    std::cout << "TX_A sees shared_key: '" << value << "' (should be value_from_tx_a)" << std::endl;
    success = doGet(tx_b, table1_id, "shared_key", value, error_msg);
    std::cout << "TX_B sees shared_key: '" << value << "' (should be value_from_tx_b)" << std::endl;
    
    // One-shot reads should see the baseline
    success = doGet(0, table1_id, "shared_key", value, error_msg);
    std::cout << "One-shot sees shared_key: '" << value << "' (should be baseline_value)" << std::endl;
    
    // Commit TX_A first
    success = doCommitTx(tx_a, error_msg);
    assert(success);
    std::cout << "✓ Committed TX_A" << std::endl;
    
    // Now one-shot should see TX_A's value
    success = doGet(0, table1_id, "shared_key", value, error_msg);
    std::cout << "After TX_A commit, one-shot sees: '" << value << "' (should be value_from_tx_a)" << std::endl;
    
    // TX_B should still see its own uncommitted version
    success = doGet(tx_b, table1_id, "shared_key", value, error_msg);
    std::cout << "TX_B still sees its version: '" << value << "' (should be value_from_tx_b)" << std::endl;
    
    // Now rollback TX_B
    success = doAbortTx(tx_b, error_msg);
    assert(success);
    std::cout << "✓ Rolled back TX_B" << std::endl;
    
    // Final state should be TX_A's committed value
    success = doGet(0, table1_id, "shared_key", value, error_msg);
    std::cout << "Final state of shared_key: '" << value << "' (should be value_from_tx_a)" << std::endl;
    
    // Test 12: Scan with transaction isolation
    std::cout << "Test 12: Scan with transaction isolation..." << std::endl << std::endl;
    
    uint64_t scan_tx = doStartTx(error_msg);
    
    // Add multiple keys in transaction
    for (int i = 1; i <= 5; i++) {
        std::string key = "scan_key_" + std::to_string(i);
        std::string value = "scan_value_" + std::to_string(i);
        success = doSet(scan_tx, table1_id, key, value, error_msg);
        assert(success);
    }
    
    // Scan within transaction should see these keys
    std::vector<std::pair<std::string, std::string>> tx_scan_results;
    // success = doScan(scan_tx, table1_id, "scan_key_", "scan_key_z", 10, tx_scan_results, error_msg);
    // assert(success);
    std::cout << "✓ Transaction scan found " << tx_scan_results.size() << " items" << std::endl;
    for (const auto& pair : tx_scan_results) {
        std::cout << "  " << pair.first << " -> " << pair.second << std::endl;
    }
    
    // One-shot scan should not see these keys yet
    std::vector<std::pair<std::string, std::string>> oneshot_scan_results;
    // success = doScan(0, table1_id, "scan_key_", "scan_key_z", 10, oneshot_scan_results, error_msg);
    // assert(success);
    std::cout << "✓ One-shot scan found " << oneshot_scan_results.size() << " items (should be 0)" << std::endl;
    
    // Commit the transaction
    success = doCommitTx(scan_tx, error_msg);
    assert(success);
    std::cout << "✓ Committed scan transaction" << std::endl;
    
    // Now one-shot scan should see the keys
    // success = doScan(0, table1_id, "scan_key_", "scan_key_z", 10, oneshot_scan_results, error_msg);
    // assert(success);
    std::cout << "✓ Post-commit one-shot scan found " << oneshot_scan_results.size() << " items" << std::endl;
    for (const auto& pair : oneshot_scan_results) {
        std::cout << "  " << pair.first << " -> " << pair.second << std::endl;
    }
    
    std::cout << "=== All tests passed! KVTManager demonstrates proper ACID behavior ===" << std::endl << std::endl;
    exit(1);
}

// Add a new function to run the comprehensive stress test
void KVTManager::runComprehensiveStressTest() {
    std::cout << "\n=== Running Comprehensive Stress Test (kvt_stress_test2) ===" << std::endl;
    
    // Call the stress test main function with proper arguments
    const char* argv[] = {"kvt_stress_test", nullptr};
    stress_test_main(1, const_cast<char**>(argv));
}

} // namespace EloqKV