#ifndef STORE_HPP
#define STORE_HPP

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <mutex>

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
};

} // namespace kvstore

#endif // STORE_HPP