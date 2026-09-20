#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <iostream>

namespace config {

/**
 * GlobalConfig: Runtime feature toggles for ablation studies.
 *
 * Flags
 * -----
 *   enable_tripwire       Async event tripwire: step down on storage overload
 *   enable_lock_striping  16-way lock-striped MemTable sharding
 *   enable_async_io       Async I/O path for SSTable writes
 *   tripwire_offset       Extra SSTables above COMPACTION_THRESHOLD before
 *                         the tripwire fires. Higher = fewer step-downs but
 *                         higher risk of Gray Failure.
 *
 * Default is the full system: both features enabled, offset = 2.
 */
class GlobalConfig {
public:
    static GlobalConfig& instance() {
        static GlobalConfig cfg;
        return cfg;
    }

    // ---- Feature flags -----------------------------------------------------
    bool enable_tripwire      = true;
    bool enable_lock_striping = true;
    bool enable_async_io      = false;
    int  tripwire_offset      = 2;

    // ---- Diagnostics -------------------------------------------------------
    void printConfig() const {
        std::cout << "\n=== CONFIGURATION ===\n";
        std::cout << "Tripwire:        "
                  << (enable_tripwire ? "ENABLED" : "DISABLED (Gray Failure Risk)") << "\n";
        std::cout << "Lock-Striping:   "
                  << (enable_lock_striping ? "ENABLED (16 shards)"
                                           : "DISABLED (Single Global Mutex)") << "\n";
        std::cout << "Async I/O:       "
                  << (enable_async_io ? "ENABLED" : "DISABLED (Sync I/O)") << "\n";
        std::cout << "Tripwire Offset: "
                  << tripwire_offset << " SSTables above COMPACTION_THRESHOLD\n";
        std::cout << "====================\n\n";
    }

private:
    GlobalConfig() = default;
    ~GlobalConfig() = default;

    GlobalConfig(const GlobalConfig&) = delete;
    GlobalConfig& operator=(const GlobalConfig&) = delete;
};

} // namespace config

#endif // CONFIG_HPP