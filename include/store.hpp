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
#include <array>
#include <deque>
#include <functional>
#include <condition_variable>
#include "config.hpp"

namespace kvstore {

class Store {
private:
    int node_id_;

    // =====================================================================
    // Persistent async flush worker.
    // ---------------------------------------------------------------------
    // Replaces std::thread(std::move(do_flush)).detach(), which spawned
    // one OS thread per MemTable flush. At MEMTABLE_LIMIT=50 over a 10k
    // workload that is 200 thread creations, each costing 50-100 us and
    // contending with the striped writers on sstables_mutex_.
    //
    // The persistent worker also serializes flush execution, which is
    // strictly more correct: SSTable registration order in sstables_
    // now matches flush order, so get()'s reverse iteration sees
    // newest-first without races.
    // =====================================================================
    std::thread                        async_flush_thread_;
    std::mutex                         async_flush_mutex_;
    std::condition_variable            async_flush_cv_;
    std::deque<std::function<void()>>  async_flush_queue_;
    std::atomic<bool>                  async_flush_running_{false};

    // =====================================================================
    // Duplicate-flush guard.
    // ---------------------------------------------------------------------
    // Under lock striping, 8 concurrent writers can all observe
    // total_memtable_size_ >= MEMTABLE_LIMIT in the same window and all
    // call flushMemtableToDisk(). Without this flag, each of them enters
    // the freeze path, takes all 16 shard mutexes sequentially, and
    // queues a separate async job — most of which find empty shards.
    //
    // The flag makes the threshold crossing atomic: only the writer that
    // flips false→true proceeds to freeze + dispatch; everyone else
    // returns immediately. The flag is reset at the end of do_flush
    // (which runs on the async worker thread in async mode).
    // =====================================================================
    std::atomic<bool> flush_in_progress_{false};

    void asyncFlushLoop() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(async_flush_mutex_);
                async_flush_cv_.wait(lk, [this]() {
                    return !async_flush_queue_.empty()
                        || !async_flush_running_.load(std::memory_order_acquire);
                });
                if (!async_flush_running_.load(std::memory_order_acquire)
                    && async_flush_queue_.empty()) {
                    return;
                }
                job = std::move(async_flush_queue_.front());
                async_flush_queue_.pop_front();
            }
            job();
        }
    }

    void enqueueAsyncFlush(std::function<void()> job) {
        std::lock_guard<std::mutex> lk(async_flush_mutex_);
        async_flush_queue_.push_back(std::move(job));
        if (!async_flush_thread_.joinable()) {
            async_flush_running_.store(true, std::memory_order_release);
            async_flush_thread_ = std::thread([this]() { asyncFlushLoop(); });
        }
        async_flush_cv_.notify_one();
    }

    // =====================================================================
    // INNOVATION 1: Lock-Striped Sharded MemTable
    // =====================================================================
    static constexpr size_t NUM_SHARDS = 16;

    struct Shard {
        std::map<std::string, std::optional<std::string>> data;
        mutable std::shared_mutex mutex;
    };

    std::array<Shard, NUM_SHARDS> shards_;
    std::atomic<size_t> total_memtable_size_{0};

    // ABLATION: single global mutex used when lock-striping is disabled.
    mutable std::shared_mutex global_memtable_mutex_;

    std::vector<std::string> sstables_;
    mutable std::shared_mutex sstables_mutex_;

    std::atomic<int> sstable_id_counter_{0};
    std::atomic<bool> is_compacting_{false};

    // =====================================================================
    // INNOVATION 3: Async Event-Driven Compaction Tripwire
    // ---------------------------------------------------------------------
    // The LSM-tree never mutates Raft state. On storage overload, we
    // publish a single-bit event to this atomic flag. The Raft main tick
    // thread is the sole consumer and the sole mutator of Raft state.
    // =====================================================================
    std::atomic<bool> storage_degraded_flag_{false};

    const size_t MEMTABLE_LIMIT       = 50;
    const size_t COMPACTION_THRESHOLD = 4;

    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % NUM_SHARDS;
    }

    // ---------------------------------------------------------------------
    // MemTable flush
    // ---------------------------------------------------------------------
    void flushMemtableToDisk() {
        // ---------------------------------------------------------------------
        // Duplicate-flush guard.
        // ---------------------------------------------------------------------
        // Only one caller may be inside the freeze path at a time. Under
        // lock striping, N writers can cross MEMTABLE_LIMIT simultaneously;
        // without this CAS, all N would proceed and queue N async flushes.
        // ---------------------------------------------------------------------
        bool expected = false;
        if (!flush_in_progress_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }

        // ---------------------------------------------------------------------
        // STEP 1: Freeze the MemTable synchronously.
        // ---------------------------------------------------------------------
        std::map<std::string, std::optional<std::string>> frozen_memtable;

        {
            std::unique_lock<std::shared_mutex> global_lock(
                global_memtable_mutex_, std::defer_lock);
            if (!config::GlobalConfig::instance().enable_lock_striping) {
                global_lock.lock();
            }

            for (size_t i = 0; i < NUM_SHARDS; ++i) {
                std::unique_lock<std::shared_mutex> lock(shards_[i].mutex);
                frozen_memtable.merge(shards_[i].data);
            }
        }

        total_memtable_size_.store(0, std::memory_order_relaxed);

        if (frozen_memtable.empty()) {
            flush_in_progress_.store(false, std::memory_order_release);
            return;
        }

        // ---------------------------------------------------------------------
        // STEP 2: Prepare the work unit.
        // ---------------------------------------------------------------------
        const std::string filename =
            "node_" + std::to_string(node_id_) + "_sstable_"
          + std::to_string(sstable_id_counter_++) + ".sst";

        auto do_flush = [
            this,
            frozen = std::move(frozen_memtable),
            filename,
            captured_node_id = node_id_
        ]() mutable {

            // ---- 2a. Write the SSTable to disk ------------------------------
            std::ofstream out(filename, std::ios::binary | std::ios::trunc);

            size_t size = frozen.size();
            out.write(reinterpret_cast<const char*>(&size), sizeof(size));

            for (const auto& [k, v_opt] : frozen) {
                size_t k_len = k.size();
                out.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
                out.write(k.data(), k_len);

                bool is_tombstone = !v_opt.has_value();
                out.write(reinterpret_cast<const char*>(&is_tombstone),
                          sizeof(is_tombstone));

                if (!is_tombstone) {
                    size_t v_len = v_opt.value().size();
                    out.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
                    out.write(v_opt.value().data(), v_len);
                }
            }
            out.close();

            // ---- 2b. Register the SSTable under the sstables_ mutex --------
            size_t current_sstable_count = 0;
            {
                std::unique_lock<std::shared_mutex> lock(sstables_mutex_);
                sstables_.push_back(filename);
                current_sstable_count = sstables_.size();
            }

            std::cout << "[LSM-Tree Node " << captured_node_id
                      << "] Flushed Lock-Striped MemTable: " << filename
                      << "\n" << std::flush;

            // ---- 2c. ASYNC EVENT TRIPWIRE — signal only --------------------
            int offset = config::GlobalConfig::instance().tripwire_offset;
            if (offset < 0) offset = 0;

            if (current_sstable_count >=
                COMPACTION_THRESHOLD + static_cast<size_t>(offset)) {

                if (config::GlobalConfig::instance().enable_tripwire) {
                    std::cout << "[LSM-Tree Node " << captured_node_id
                              << "] ASYNC EVENT TRIPWIRE SIGNALED: "
                              << current_sstable_count
                              << " SSTables detected. Posting storage-degraded event.\n"
                              << std::flush;
                    storage_degraded_flag_.store(true, std::memory_order_release);
                } else {
                    std::cout << "[LSM-Tree Node " << captured_node_id
                              << "] WARNING: Disk saturation detected but tripwire DISABLED. "
                              << "Gray failure likely!\n" << std::flush;
                }
            }

            // ---- 2d. Spawn async compaction if above threshold -------------
            bool expected_compaction = false;
            if (current_sstable_count >= COMPACTION_THRESHOLD
                && is_compacting_.compare_exchange_strong(expected_compaction, true)) {
                std::thread([this]() {
                    this->compactSSTables();
                    this->is_compacting_.store(false);
                }).detach();
            }

            // ---- 2e. Release the duplicate-flush guard ---------------------
            // Reset only after the SSTable is on disk AND registered, so a
            // subsequent threshold crossing cannot start a new freeze while
            // this one's file write is still in flight.
            flush_in_progress_.store(false, std::memory_order_release);
        };

        // ---------------------------------------------------------------------
        // STEP 3: Dispatch — async or sync, based on the config flag.
        // ---------------------------------------------------------------------
        if (config::GlobalConfig::instance().enable_async_io) {
            enqueueAsyncFlush(std::move(do_flush));
        } else {
            do_flush();
        }
    }

    void compactSSTables() {
        std::vector<std::string> files_to_compact;
        {
            std::shared_lock<std::shared_mutex> lock(sstables_mutex_);
            files_to_compact = sstables_;
        }
        if (files_to_compact.empty()) return;

        std::cout << "[LSM-Tree Node " << node_id_
                  << "] Starting Async Major Compaction...\n" << std::flush;

        // Deterministic fault injection: simulates a 5s EBS latency spike.
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

        std::string compacted_filename = "node_" + std::to_string(node_id_)
                                       + "_sstable_" + std::to_string(sstable_id_counter_++)
                                       + "_compacted.sst";
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
        std::cout << "[LSM-Tree Node " << node_id_
                  << "] Async Compaction complete.\n" << std::flush;
    }

    bool searchSSTable(const std::string& filename, const std::string& key,
                       std::optional<std::string>& result) const {
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

    ~Store() {
        {
            std::lock_guard<std::mutex> lk(async_flush_mutex_);
            async_flush_running_.store(false, std::memory_order_release);
        }
        async_flush_cv_.notify_all();
        if (async_flush_thread_.joinable()) {
            async_flush_thread_.join();
        }
    }

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    // =====================================================================
    // ASYNC EVENT TRIPWIRE — consumer API
    // =====================================================================
    bool consumeStorageDegradedFlag() {
        return storage_degraded_flag_.exchange(false, std::memory_order_acq_rel);
    }

    // =====================================================================
    // SET
    // =====================================================================
    void set(const std::string& key, const std::string& value) {
        bool should_flush = false;

        if (config::GlobalConfig::instance().enable_lock_striping) {
            size_t idx = getShardIndex(key);
            std::unique_lock<std::shared_mutex> lock(shards_[idx].mutex);
            auto [it, inserted] = shards_[idx].data.insert_or_assign(key, value);
            if (inserted) {
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) should_flush = true;
            }
        } else {
            std::unique_lock<std::shared_mutex> lock(global_memtable_mutex_);
            bool found = false;
            for (size_t i = 0; i < NUM_SHARDS && !found; i++) {
                if (shards_[i].data.find(key) != shards_[i].data.end()) {
                    found = true;
                    shards_[i].data.insert_or_assign(key, value);
                    break;
                }
            }
            if (!found) {
                shards_[0].data.insert_or_assign(key, value);
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) should_flush = true;
            }
        }

        if (should_flush) flushMemtableToDisk();
    }

    // =====================================================================
    // GET
    // =====================================================================
    std::optional<std::string> get(const std::string& key) const {
        if (config::GlobalConfig::instance().enable_lock_striping) {
            size_t idx = getShardIndex(key);
            {
                std::shared_lock<std::shared_mutex> lock(shards_[idx].mutex);
                auto it = shards_[idx].data.find(key);
                if (it != shards_[idx].data.end()) return it->second;
            }
        } else {
            std::shared_lock<std::shared_mutex> lock(global_memtable_mutex_);
            for (size_t i = 0; i < NUM_SHARDS; i++) {
                auto it = shards_[i].data.find(key);
                if (it != shards_[i].data.end()) return it->second;
            }
        }

        std::shared_lock<std::shared_mutex> sst_lock(sstables_mutex_);
        for (auto rit = sstables_.rbegin(); rit != sstables_.rend(); ++rit) {
            std::optional<std::string> result;
            if (searchSSTable(*rit, key, result)) return result;
        }
        return std::nullopt;
    }

    // =====================================================================
    // REMOVE
    // =====================================================================
    bool remove(const std::string& key) {
        bool should_flush = false;

        if (config::GlobalConfig::instance().enable_lock_striping) {
            size_t idx = getShardIndex(key);
            std::unique_lock<std::shared_mutex> lock(shards_[idx].mutex);
            auto [it, inserted] = shards_[idx].data.insert_or_assign(key, std::nullopt);
            if (inserted) {
                size_t current_size = total_memtable_size_.fetch_add(1, std::memory_order_relaxed);
                if (current_size + 1 >= MEMTABLE_LIMIT) should_flush = true;
            }
        } else {
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
                if (current_size + 1 >= MEMTABLE_LIMIT) should_flush = true;
            }
        }

        if (should_flush) flushMemtableToDisk();
        return true;
    }

    // =====================================================================
    // CLEAR
    // =====================================================================
    void clear() {
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            std::unique_lock<std::shared_mutex> lock(shards_[i].mutex);
            shards_[i].data.clear();
        }
        std::unique_lock<std::shared_mutex> sst_lock(sstables_mutex_);
        sstables_.clear();
        total_memtable_size_.store(0);
    }

    // =====================================================================
    // SNAPSHOT SAVE
    // =====================================================================
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

                    if (is_tombstone) {
                        logical_state.erase(k);
                    } else {
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

    // =====================================================================
    // SNAPSHOT LOAD
    // =====================================================================
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

        std::string sst_filename = "node_" + std::to_string(node_id_)
                                 + "_sstable_" + std::to_string(sstable_id_counter_++) + ".sst";
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