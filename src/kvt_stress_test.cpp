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
                if (doSet(tx_id, "perf_table_hash", key, value, error_msg)) {
                    metrics.successful_operations++;
                } else {
                    metrics.failed_operations++;
                    tx_success = false;
                    break;
                }
            } else {
                // GET operation
                std::string retrieved_value;
                if (doGet(tx_id, "perf_table_hash", key, retrieved_value, error_msg)) {
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
    
    std::cout << "Running with " << num_threads << " threads for " << duration_seconds 
              << " seconds with " << ops_per_transaction << " operations per transaction..." << std::endl;
    
    PerfMetrics global_metrics;
    std::vector<std::thread> threads;
    std::atomic<bool> stop_flag{false};
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Launch worker threads
    for (int thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back([this, thread_id, &global_metrics, &stop_flag, ops_per_transaction]() {
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
                        if (doSet(tx_id, "perf_table_hash", key, value, error_msg)) {
                            local_metrics.successful_operations++;
                        } else {
                            local_metrics.failed_operations++;
                            tx_success = false;
                            break;
                        }
                    } else {
                        // GET operation
                        std::string retrieved_value;
                        if (doGet(tx_id, "perf_table_hash", key, retrieved_value, error_msg)) {
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
    
    for (int num_threads : thread_counts) {
        PerfMetrics metrics;
        std::vector<std::thread> threads;
        std::atomic<bool> stop_flag{false};
        
        auto start_time = std::chrono::steady_clock::now();
        
        // Launch threads
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([this, i, &metrics, &stop_flag]() {
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
                    
                    if (doSet(tx_id, "benchmark_hash", key, value, error_msg)) {
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
        
        doSet(tx_id, "benchmark_hash", key, value, error_msg);
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
    
    PerfMetrics metrics;
    std::vector<std::thread> threads;
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Launch threads that work on overlapping key ranges to test concurrency
    for (int thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back([this, thread_id, &metrics, transactions_per_thread, ops_per_transaction]() {
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
                    
                    if (!doSet(tx_id, "benchmark_hash", key, value, error_msg)) {
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

}