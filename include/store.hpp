#ifndef STORE_HPP
#define STORE_HPP

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <mutex>
#include <fstream>
#include <cstdint>

namespace kvstore {

class Store {
private:
    std::unordered_map<std::string, std::string> data_;
    mutable std::shared_mutex mutex_; 

public:
    Store() = default;
    ~Store() = default;

    // Write operation: Requires exclusive lock
    void set(const std::string& key, const std::string& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[key] = value;
    }

    // Read operation: Allows multiple simultaneous readers
    std::optional<std::string> get(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    // Delete operation: Requires exclusive lock
    bool remove(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return data_.erase(key) > 0;
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.clear();
    }

    // --- Snapshot Methods ---

    // Dumps the entire KV store to a binary file
    bool saveSnapshot(const std::string& filename) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::ofstream out(filename, std::ios::binary | std::ios::trunc);
        if (!out) return false;

        size_t size = data_.size();
        out.write(reinterpret_cast<const char*>(&size), sizeof(size));

        for (const auto& [k, v] : data_) {
            size_t k_len = k.size();
            size_t v_len = v.size();
            
            out.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            out.write(k.data(), k_len);
            out.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            out.write(v.data(), v_len);
        }
        return true;
    }

    // Replaces current store state with a binary snapshot file
    bool loadSnapshot(const std::string& filename) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::ifstream in(filename, std::ios::binary);
        if (!in) return false;

        data_.clear();
        
        size_t size;
        if (!in.read(reinterpret_cast<char*>(&size), sizeof(size))) return true; // Empty snapshot

        for (size_t i = 0; i < size; ++i) {
            size_t k_len, v_len;
            
            in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
            std::string k(k_len, '\0');
            in.read(&k[0], k_len);
            
            in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
            std::string v(v_len, '\0');
            in.read(&v[0], v_len);
            
            data_[k] = v;
        }
        return true;
    }
};

} // namespace kvstore

#endif // STORE_HPP