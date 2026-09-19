#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <iostream>

namespace config {

/**
 * GlobalConfig: Runtime feature toggles for ablation studies
 * 
 * These flags allow us to test the system with/without:
 * - Compaction-Aware Consensus Tripwire (prevents gray failures)
 * - Lock-Striped MemTable sharding (reduces contention)
 * 
 * Default: Both enabled (the "full system" configuration)
 * Ablation: Disable each to measure its individual impact
 */
class GlobalConfig {
public:
    static GlobalConfig& instance() {
        static GlobalConfig cfg;
        return cfg;
    }

    // Feature flags
    bool enable_tripwire = true;           // Enable disk saturation detection & leader stepdown
    bool enable_lock_striping = true;      // Enable 16-way lock-striped MemTable
    
    // Print current configuration
    void printConfig() const {
        std::cout << "\n=== CONFIGURATION ===\n";
        std::cout << "Tripwire:       " << (enable_tripwire ? "ENABLED" : "DISABLED (Gray Failure Risk)") << "\n";
        std::cout << "Lock-Striping:  " << (enable_lock_striping ? "ENABLED (16 shards)" : "DISABLED (Single Global Mutex)") << "\n";
        std::cout << "====================\n\n";
    }

private:
    GlobalConfig() = default;
    ~GlobalConfig() = default;
    
    // Prevent copying
    GlobalConfig(const GlobalConfig&) = delete;
    GlobalConfig& operator=(const GlobalConfig&) = delete;
};

} // namespace config

#endif // CONFIG_HPP