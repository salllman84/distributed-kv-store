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
#include <cstdio> // Added for std::remove (file deletion)

namespace kvstore {

class Store {
private:
    int node_id_;
    
    std::map<std::string, std::optional<std::string>> memtable_;
    std::vector<std::string> sstables_; 
    mutable std::shared_mutex mutex_;
    int sstable_id_counter_ = 0;

    const size_t MEMTABLE_LIMIT = 5; 
    const size_t COMPACTION_THRESHOLD = 3; // <--- ADDED: Trigger compaction when we hit 3 SSTables

    void flushMemtableToDisk() {
        if (memtable_.empty()) return;

        std::string filename = "node_" + std::to_string(node_id_) + "_sstable_" + std::to_string(sstable_id_counter_++) + ".sst";
        std::ofstream out(filename, std::ios::binary | std::ios::trunc);
        
        size_t size = memtable_.size();
        out.write(reinterpret_cast<const char*>(&size), sizeof(size));

        for (const auto& [k, v_opt] : memtable_) {
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

        sstables_.push_back(filename);
        memtable_.clear();
        std::cout << "[LSM-Tree Node " << node_id_ << "] Flushed MemTable to disk: " << filename << "\n" << std::flush;

        // <--- ADDED: Check if we need to compact
        if (sstables_.size() >= COMPACTION_THRESHOLD) {
            compactSSTables();
        }
    }

    // <--- ADDED: Major Compaction Engine
    void compactSSTables() {
        std::cout << "[LSM-Tree Node " << node_id_ << "] Starting Major Compaction of " << sstables_.size() << " SSTables...\n" << std::flush;

        // A temporary map to hold the merged, logical state of all SSTables
        std::map<std::string, std::string> compacted_data;

        // 1. Read all SSTables from oldest to newest
        for (const auto& sst : sstables_) {
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
                        // Conflict Resolution: If a newer file has a tombstone, obliterate the older data.
                        compacted_data.erase(k);
                    } else {
                        size_t v_len;
                        in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
                        std::string v(v_len, '\0');
                        in.read(&v[0], v_len);
                        // Conflict Resolution: Newer values automatically overwrite older values here
                        compacted_data[k] = v;
                    }
                }
            }
        }

        // 2. Write the clean, merged dataset to a new compacted file (Notice: Tombstones are completely gone!)
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

        // 3. Delete the old fragmented files from the hard drive
        for (const auto& sst : sstables_) {
            std::remove(sst.c_str());
        }

        // 4. Update the tracking array to only point to the new master file
        sstables_.clear();
        sstables_.push_back(compacted_filename);
        std::cout << "[LSM-Tree Node " << node_id_ << "] Compaction complete. Reclaimed disk space. Generated: " << compacted_filename << "\n" << std::flush;
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

    void set(const std::string& key, const std::string& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        memtable_[key] = value;
        if (memtable_.size() >= MEMTABLE_LIMIT) {
            flushMemtableToDisk();
        }
    }

    std::optional<std::string> get(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        auto it = memtable_.find(key);
        if (it != memtable_.end()) {
            return it->second; 
        }

        for (auto rit = sstables_.rbegin(); rit != sstables_.rend(); ++rit) {
            std::optional<std::string> result;
            if (searchSSTable(*rit, key, result)) {
                return result; 
            }
        }
        
        return std::nullopt;
    }

    bool remove(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        memtable_[key] = std::nullopt; // <--- Insert Tombstone
        if (memtable_.size() >= MEMTABLE_LIMIT) {
            flushMemtableToDisk();
        }
        return true;
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        memtable_.clear();
        sstables_.clear();
    }

    bool saveSnapshot(const std::string& filename) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::map<std::string, std::string> logical_state;

        for (const auto& sst : sstables_) {
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

        for (const auto& [k, v_opt] : memtable_) {
            if (!v_opt.has_value()) logical_state.erase(k);
            else logical_state[k] = v_opt.value();
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
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::ifstream in(filename, std::ios::binary);
        if (!in) return false;

        memtable_.clear();
        sstables_.clear();
        sstable_id_counter_ = 0;
        
        size_t size;
        if (!in.read(reinterpret_cast<char*>(&size), sizeof(size))) return true; 

        for (size_t i = 0; i < size; ++i) {
            size_t k_len, v_len;
            
            in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
            std::string k(k_len, '\0');
            in.read(&k[0], k_len);
            
            in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
            std::string v(v_len, '\0');
            in.read(&v[0], v_len);
            
            memtable_[k] = v;
        }

        if (!memtable_.empty()) {
            flushMemtableToDisk();
        }

        return true;
    }
};

} // namespace kvstore

#endif // STORE_HPP