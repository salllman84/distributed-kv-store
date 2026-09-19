#ifndef STORE_HPP
#define STORE_HPP

#include <string>
#include <map>
#include <vector>
#include <shared_mutex>
#include <optional>
#include <mutex>
#include <fstream>
#include <cstdint>
#include <iostream>
#include <cstdio>
#include <thread>
#include <atomic>
#include <algorithm>
#include <functional> // REQUIRED FOR CPU HASHING & CALLBACKS
#include <array>      // REQUIRED FOR LOCK STRIPING
#include "config.hpp" // Feature toggles for ablation study

namespace kvstore {

class Store {
private:
    int node_id_;
    
    // ----------------------------------------------------
    // INNOVATION 1: Lock-Striped Sharded MemTable
    // ----------------------------------------------------
    static constexpr size_t NUM_SHARDS = 16;
    
    struct Shard {
        std::map<std::string, std::optional<std::string>> data;
        mutable std::shared_mutex mutex; // Each shard gets its own lock!
    };
    
    std::array<Shard, NUM_SHARDS> shards_;
    std::atomic<size_t> total_memtable_size_{0};
    
    // --- ABLATION: Global mutex for single-lock MemTable mode (when lock-striping disabled) ---
    mutable std::shared_mutex global_memtable_mutex_;

    std::vector<std::string> sstables_; 
    mutable std::shared_mutex sstables_mutex_; 
    
    std::atomic<int> sstable_id_counter_{0};   
    std::atomic<bool> is_compacting_{false};   

    // --- INNOVATION 3: Compaction-Aware Consensus Tripwire ---
    std::function<void()> leader_stepdown_callback_ = nullptr;

    const size_t MEMTABLE_LIMIT = 50; 
    const size_t COMPACTION_THRESHOLD = 4;

    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % NUM_SHARDS;
    }

    void flushMemtableToDisk() {
        std::map<std::string, std::optional<std::string>> frozen_memtable;
        
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            std::unique_lock<std::shared_mutex> lock(shards_[i].mutex);
            frozen_memtable.merge(shards_[i].data); 
        }
        
        total_memtable_size_.store(0, std::memory_order_relaxed);

        if (frozen_memtable.empty()) return;

        std::string filename = "node_" + std::to_string(node_id_) + "_sstable_" + std::to_string(sstable_id_counter_++) + ".sst";
        std::ofstream out(filename, std::ios::binary | std::ios::trunc);
        
        size_t size = frozen_memtable.size();
        out.write(reinterpret_cast<const char*>(&size), sizeof(size));

        for (const auto& [k, v_opt] : frozen_memtable) {
            size_t k_len = k.size();
            out.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            out.write(k.data(), k_len);

            bool is_tombstone = !v_opt.has_value();
            out.write(reinterpret_cast<const char*>(&is_tombstone), sizeof(is_tombstone));

            if (!is_tombstone) {
                size_t v_len = v_opt.value().size();
                out.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
                out.write(v_opt.value().data(), v_len);
            }
        }
        out.close();

        size_t current_sstable_count = 0;
        {
            std::unique_lock<std::shared_mutex> lock(sstables_mutex_);
            sstables_.push_back(filename);
            current_sstable_count = sstables_.size();
        }
        
        std::cout << "[LSM-Tree Node " << node_id_ << "] Flushed Lock-Striped MemTable: " << filename << "\n" << std::flush;

        // ============================================================================
        // ABLATION: Tripwire check - CONDITIONAL on enable_tripwire flag
        // ============================================================================
        // This check MUST run on EVERY flush IF enabled, regardless of compaction state.
        // If disk is saturated (current_sstable_count >= 6), the leader must step down
        // EVEN IF a compaction is already running. This is when the system is most at risk.
        // 
        // ABLATION MODE: When disable, the system intentionally allows Gray Failures
        // (network thread starvation) to demonstrate the value of the tripwire.
        if (config::GlobalConfig::instance().enable_tripwire) {
            if (current_sstable_count >= COMPACTION_THRESHOLD + 2) { 
                if (leader_stepdown_callback_) {
                    std::cout << "[LSM-Tree Node " << node_id_ << "] TRIPWIRE FIRED: " 
                              << current_sstable_count << " SSTables detected. Signaling leader stepdown.\n" << std::flush;
                    leader_stepdown_callback_();
                }
            }
        } else {
            // ABLATION: Tripwire disabled - system is vulnerable to Gray Failures
            if (current_sstable_count >= COMPACTION_THRESHOLD + 2) {
                std::cout << "[LSM-Tree Node " << node_id_ << "] WARNING: Disk saturation detected but tripwire DISABLED. "
                          << "Gray failure likely!\n" << std::flush;
            }
        }

        // ============================================================================
        // Only after the tripwire has fired, attempt to spawn a compaction if needed
        // ============================================================================
        bool expected = false;
        if (current_sstable_count >= COMPACTION_THRESHOLD && is_compacting_.compare_exchange_strong(expected, true)) {
            std::thread([this]() {
                this->compactSSTables();
                this->is_compacting_.store(false);
            }).detach(); 
        }
    }

    void compactSSTables() {
        std::vector<std::string> files_to_compact;
        {
            std::shared_lock<std::shared_mutex> lock(sstables_mutex_);
            files_to_compact = sstables_;
        }
        if (files_to_compact.empty()) return;
        std::cout << "[LSM-Tree Node " << node_id_ << "] Starting Async Major Compaction...\n" << std::flush;
        // --- DETERMINISTIC FAULT INJECTION ---
        // Simulating a 100ms EBS volume latency spike to test Consensus step-down
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        std::map<std::string, std::string> compacted_data;
        for (const auto& sst : files_to_compact) {
            std::ifstream in(sst, std::ios::binary);
            if (!in) continue;
            size_t size;
            if (in.read(reinterpret_cast<char*>(&size), sizeof(size))) {
                for (size_t i = 0; i < size; ++i) {
                    size_t k_len;
                    in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
                    std::string k(k_len, '\0');
                    in.read(&k[0], k_len);
                    
                    bool is_tombstone;
                    in.read(reinterpret_cast<char*>(&is_tombstone), sizeof(is_tombstone));
                    
                    if (is_tombstone) {
                        compacted_data.erase(k);
                    } else {
                        size_t v_len;
                        in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
                        std::string v(v_len, '\0');
                        in.read(&v[0], v_len);
                        compacted_data[k] = v;
                    }
                }
            }
        }

        std::string compacted_filename = "node_" + std::to_string(node_id_) + "_sstable_" + std::to_string(sstable_id_counter_++) + "_compacted.sst";
        std::ofstream out(compacted_filename, std::ios::binary | std::ios::trunc);
        size_t size = compacted_data.size();
        out.write(reinterpret_cast<const char*>(&size), sizeof(size));
        for (const auto& [k, v] : compacted_data) {
            size_t k_len = k.size();
            out.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            out.write(k.data(), k_len);
            bool is_tombstone = false;
            out.write(reinterpret_cast<const char*>(&is_tombstone), sizeof(is_tombstone));
            size_t v_len = v.size();
            out.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            out.write(v.data(), v_len);
        }
        out.close();

        {
            std::unique_lock<std::shared_mutex> lock(sstables_mutex_);
            std::vector<std::string> new_sstables;
            new_sstables.push_back(compacted_filename);
            for (const auto& sst : sstables_) {
                auto it = std::find(files_to_compact.begin(), files_to_compact.end(), sst);
                if (it == files_to_compact.end()) {
                    new_sstables.push_back(sst);
                }
            }
            sstables_ = std::move(new_sstables);
        }
        for (const auto& sst : files_to_compact) {
            std::remove(sst.c_str());
        }
        std::cout << "[LSM-Tree Node " << node_id_ << "] Async Compaction complete.\n" << std::flush;
    }

    bool searchSSTable(const std::string& filename, const std::string& key, std::optional<std::string>& result) const {
        std::ifstream in(filename, std::ios::binary);
        if (!in) return false;
        size_t size;
        if (!in.read(reinterpret_cast<char*>(&size), sizeof(size))) return false;
        for (size_t i = 0; i < size; ++i) {
            size_t k_len;
            in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
            std::string k(k_len, '\0');
            in.read(&k[0], k_len);
            bool is_tombstone;
            in.read(reinterpret_cast<char*>(&is_tombstone), sizeof(is_tombstone));
            std::string v;
            if (!is_tombstone) {
                size_t v_len;
                in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
                v.resize(v_len);
                in.read(&v[0], v_len);
            }
            if (k == key) {
                result = is_tombstone ? std::nullopt : std::optional<std::string>(v);
                return true;
            }
            if (k > key) break;
        }
        return false;
    }

public:
    explicit Store(int node_id = 0) : node_id_(node_id) {}
    ~Store() = default;

    // Register the Raft layer callback
    void registerStepDownCallback(std::function<void()> cb) {
        leader_stepdown_callback_ = std::move(cb);
    }

    void set(const std::string& key, const std::string& value) {
        bool should_flush = false;
        
        // ABLATION: Choose locking strategy based on enable_lock_striping flag
        if (config::GlobalConfig::instance().enable_lock_striping) {
            // STRIPED MODE: Use 16-way lock striping
            size_t idx = getShardIndex(key);
            std::unique_lock<std::shared_mutex> lock(shards_[idx].mutex);
            auto [it, inserted] = shards_[idx].data.insert_or_assign(key, value);
            
            if (inserted) {
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) {
                    should_flush = true;
                }
            }
        } else {
            // NAIVE MODE: Use single global mutex (simulates non-striped LSM-tree)
            std::unique_lock<std::shared_mutex> lock(global_memtable_mutex_);
            
            // Find key in all shards
            bool found = false;
            for (size_t i = 0; i < NUM_SHARDS && !found; i++) {
                if (shards_[i].data.find(key) != shards_[i].data.end()) {
                    found = true;
                    shards_[i].data.insert_or_assign(key, value);
                    break;
                }
            }
            if (!found) {
                // Insert to first shard (naive round-robin)
                shards_[0].data.insert_or_assign(key, value);
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) {
                    should_flush = true;
                }
            }
        }
        
        if (should_flush) {
            flushMemtableToDisk();
        }
    }

    std::optional<std::string> get(const std::string& key) const {
        // ABLATION: Choose locking strategy based on enable_lock_striping flag
        if (config::GlobalConfig::instance().enable_lock_striping) {
            // STRIPED MODE: Use 16-way lock striping
            size_t idx = getShardIndex(key);
            {
                std::shared_lock<std::shared_mutex> lock(shards_[idx].mutex);
                auto it = shards_[idx].data.find(key);
                if (it != shards_[idx].data.end()) {
                    return it->second; 
                }
            }
        } else {
            // NAIVE MODE: Use single global mutex
            std::shared_lock<std::shared_mutex> lock(global_memtable_mutex_);
            for (size_t i = 0; i < NUM_SHARDS; i++) {
                auto it = shards_[i].data.find(key);
                if (it != shards_[i].data.end()) {
                    return it->second; 
                }
            }
        }

        std::shared_lock<std::shared_mutex> sst_lock(sstables_mutex_);
        for (auto rit = sstables_.rbegin(); rit != sstables_.rend(); ++rit) {
            std::optional<std::string> result;
            if (searchSSTable(*rit, key, result)) {
                return result; 
            }
        }
        return std::nullopt;
    }

    bool remove(const std::string& key) {
        bool should_flush = false;
        
        // ABLATION: Choose locking strategy based on enable_lock_striping flag
        if (config::GlobalConfig::instance().enable_lock_striping) {
            // STRIPED MODE: Use 16-way lock striping
            size_t idx = getShardIndex(key);
            std::unique_lock<std::shared_mutex> lock(shards_[idx].mutex);
            auto [it, inserted] = shards_[idx].data.insert_or_assign(key, std::nullopt);
            if (inserted) {
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) {
                    should_flush = true;
                }
            }
        } else {
            // NAIVE MODE: Use single global mutex
            std::unique_lock<std::shared_mutex> lock(global_memtable_mutex_);
            
            bool found = false;
            for (size_t i = 0; i < NUM_SHARDS && !found; i++) {
                if (shards_[i].data.find(key) != shards_[i].data.end()) {
                    found = true;
                    shards_[i].data.insert_or_assign(key, std::nullopt);
                    break;
                }
            }
            if (!found) {
                shards_[0].data.insert_or_assign(key, std::nullopt);
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) {
                    should_flush = true;
                }
            }
        }
        
        if (should_flush) flushMemtableToDisk();
        return true;
    }

    void clear() {
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            std::unique_lock<std::shared_mutex> lock(shards_[i].mutex);
            shards_[i].data.clear();
        }
        std::unique_lock<std::shared_mutex> sst_lock(sstables_mutex_);
        sstables_.clear();
        total_memtable_size_.store(0);
    }
    
    bool saveSnapshot(const std::string& filename) const {
        std::map<std::string, std::string> logical_state;
        
        std::vector<std::string> current_sstables;
        {
            std::shared_lock<std::shared_mutex> sst_lock(sstables_mutex_);
            current_sstables = sstables_;
        }

        for (const auto& sst : current_sstables) {
            std::ifstream in(sst, std::ios::binary);
            if (!in) continue;
            size_t size;
            if (in.read(reinterpret_cast<char*>(&size), sizeof(size))) {
                for (size_t i = 0; i < size; ++i) {
                    size_t k_len;
                    in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
                    std::string k(k_len, '\0');
                    in.read(&k[0], k_len);
                    
                    bool is_tombstone;
                    in.read(reinterpret_cast<char*>(&is_tombstone), sizeof(is_tombstone));
                    
                    if (is_tombstone) logical_state.erase(k);
                    else {
                        size_t v_len;
                        in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
                        std::string v(v_len, '\0');
                        in.read(&v[0], v_len);
                        logical_state[k] = v;
                    }
                }
            }
        }

        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            std::shared_lock<std::shared_mutex> lock(shards_[i].mutex);
            for (const auto& [k, v_opt] : shards_[i].data) {
                if (!v_opt.has_value()) logical_state.erase(k);
                else logical_state[k] = v_opt.value();
            }
        }

        std::ofstream out(filename, std::ios::binary | std::ios::trunc);
        if (!out) return false;

        size_t size = logical_state.size();
        out.write(reinterpret_cast<const char*>(&size), sizeof(size));

        for (const auto& [k, v] : logical_state) {
            size_t k_len = k.size();
            size_t v_len = v.size();
            out.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            out.write(k.data(), k_len);
            out.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            out.write(v.data(), v_len);
        }
        return true;
    }

    bool loadSnapshot(const std::string& filename) {
        std::vector<std::unique_lock<std::shared_mutex>> shard_locks;
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            shard_locks.emplace_back(shards_[i].mutex);
        }
        std::unique_lock<std::shared_mutex> sst_lock(sstables_mutex_);
        
        std::ifstream in(filename, std::ios::binary);
        if (!in) return false;

        for (size_t i = 0; i < NUM_SHARDS; ++i) shards_[i].data.clear();
        total_memtable_size_.store(0, std::memory_order_relaxed);
        sstables_.clear();
        sstable_id_counter_ = 0;
        
        size_t size;
        if (!in.read(reinterpret_cast<char*>(&size), sizeof(size))) return true; 
        if (size == 0) return true;

        std::string sst_filename = "node_" + std::to_string(node_id_) + "_sstable_" + std::to_string(sstable_id_counter_++) + ".sst";
        std::ofstream out(sst_filename, std::ios::binary | std::ios::trunc);
        
        out.write(reinterpret_cast<const char*>(&size), sizeof(size));

        for (size_t i = 0; i < size; ++i) {
            size_t k_len, v_len;
            
            in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
            std::string k(k_len, '\0');
            in.read(&k[0], k_len);
            
            in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
            std::string v(v_len, '\0');
            in.read(&v[0], v_len);
            
            out.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            out.write(k.data(), k_len);
            bool tomb = false;
            out.write(reinterpret_cast<const char*>(&tomb), sizeof(tomb));
            out.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            out.write(v.data(), v_len);
        }

        sstables_.push_back(sst_filename);
        return true;
    }
};

} // namespace kvstore

#endif // STORE_HPP