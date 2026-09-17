#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <cmath>
#include <algorithm>
#include "leveldb/db.h"
#include "../include/store.hpp"

// Simple Zipfian generator for YCSB compliance
class ZipfianGenerator {
    std::mt19937 gen;
    std::vector<double> cdf;
public:
    ZipfianGenerator(int n, double alpha) : gen(std::random_device{}()) {
        double c = 0.0;
        for (int i = 1; i <= n; i++) c += (1.0 / std::pow(i, alpha));
        c = 1.0 / c;
        double sum = 0.0;
        for (int i = 1; i <= n; i++) {
            sum += (c / std::pow(i, alpha));
            cdf.push_back(sum);
        }
    }
    int next() {
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        double u = dist(gen);
        auto it = std::lower_bound(cdf.begin(), cdf.end(), u);
        return std::distance(cdf.begin(), it) + 1;
    }
};

void run_leveldb_benchmark(int num_ops, int num_keys) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/leveldb_test", &db);
    
    ZipfianGenerator zipf(num_keys, 0.99); // YCSB standard Zipfian constant
    std::vector<double> latencies;
    std::mutex lat_mutex;

    auto start_time = std::chrono::high_resolution_clock::now();

    auto worker = [&]() {
        for (int i = 0; i < num_ops; i++) {
            std::string key = "user" + std::to_string(zipf.next());
            
            auto req_start = std::chrono::high_resolution_clock::now();
            
            // YCSB Workload A: 50% Read, 50% Write
            if (i % 2 == 0) {
                db->Put(leveldb::WriteOptions(), key, "payload_data_block");
            } else {
                std::string value;
                db->Get(leveldb::ReadOptions(), key, &value);
            }
            
            auto req_end = std::chrono::high_resolution_clock::now();
            double ms = std::chrono::duration<double, std::milli>(req_end - req_start).count();
            
            std::lock_guard<std::mutex> lock(lat_mutex);
            latencies.push_back(ms);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 16; i++) threads.emplace_back(worker);
    for (auto& t : threads) t.join();

    delete db;

    std::sort(latencies.begin(), latencies.end());
    std::cout << "[LevelDB] p99 Latency: " << latencies[latencies.size() * 0.99] << " ms\n";
}

void run_custom_benchmark(int num_ops, int num_keys) {
    kvstore::Store store(1);
    ZipfianGenerator zipf(num_keys, 0.99);
    std::vector<double> latencies;
    std::mutex lat_mutex;

    auto worker = [&]() {
        for (int i = 0; i < num_ops; i++) {
            std::string key = "user" + std::to_string(zipf.next());
            
            auto req_start = std::chrono::high_resolution_clock::now();
            
            if (i % 2 == 0) {
                store.set(key, "payload_data_block");
            } else {
                store.get(key);
            }
            
            auto req_end = std::chrono::high_resolution_clock::now();
            double ms = std::chrono::duration<double, std::milli>(req_end - req_start).count();
            
            std::lock_guard<std::mutex> lock(lat_mutex);
            latencies.push_back(ms);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 16; i++) threads.emplace_back(worker);
    for (auto& t : threads) t.join();

    std::sort(latencies.begin(), latencies.end());
    std::cout << "[Custom Async LSM] p99 Latency: " << latencies[latencies.size() * 0.99] << " ms\n";
}

int main() {
    int ops_per_thread = 5000; // 80,000 total operations
    int pool_size = 10000;
    
    std::cout << "Starting YCSB Workload A (Zipfian) against LevelDB...\n";
    run_leveldb_benchmark(ops_per_thread, pool_size);
    
    std::cout << "Starting YCSB Workload A (Zipfian) against Custom Architecture...\n";
    run_custom_benchmark(ops_per_thread, pool_size);
    
    return 0;
}