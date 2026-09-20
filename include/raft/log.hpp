#ifndef RAFT_LOG_HPP
#define RAFT_LOG_HPP

#include "raft/state.hpp"
#include <vector>
#include <optional>
#include <string>
#include <cstdint>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>

namespace raft {

class RaftLog {
private:
    int node_id_;
    std::vector<LogEntry> entries_;
    uint64_t commit_index_;
    uint64_t last_applied_;
    std::string filename_;

    // --- Snapshot / compaction offsets ---
    uint64_t last_included_index_;
    uint64_t last_included_term_;

    // =========================================================================
    // WAL — append-only with group commit
    // =========================================================================
    // The previous implementation rewrote the entire WAL on every append via
    //     std::ofstream(filename_, std::ios::trunc)
    // which is O(N) per append, O(N^2) total. At 10k entries this dominated
    // every client request and was the sole cause of the ~141 ms flat p50.
    //
    // New design:
    //   * One persistent POSIX fd, opened O_APPEND.
    //   * append() writes 30-60 bytes to the fd and returns.
    //   * A background thread fsyncs the fd in batches (group commit), so
    //     durability is decoupled from the client-facing write path.
    //   * rewriteWALRaw() is the O(N) path, invoked ONLY on truncate/compact.
    // =========================================================================
    int wal_fd_ = -1;
    mutable std::mutex wal_mutex_;
    std::condition_variable fsync_cv_;
    std::condition_variable durable_cv_;
    uint64_t unsynced_index_ = 0;   // last index written but not yet fsynced
    uint64_t durable_index_  = 0;   // last index guaranteed on stable storage

    std::thread fsync_thread_;
    std::atomic<bool> fsync_running_{false};

    // --- I/O helpers ---
    void load();                        // parse existing WAL into entries_
    void openWALForAppend();            // open fd with O_APPEND
    void closeWAL();
    void writeEntryRaw(uint64_t term, const std::string& command);
    void rewriteWALRaw();               // full rewrite; only for truncate/compact
    void fsyncLoop();

public:
    explicit RaftLog(int node_id);
    ~RaftLog();

    RaftLog(const RaftLog&) = delete;
    RaftLog& operator=(const RaftLog&) = delete;

    // Append a new entry, returns its 1-based index
    uint64_t append(uint64_t term, const std::string& command);

    // Append raw entry vector (used during replication sync)
    void appendEntries(uint64_t prev_log_index, const std::vector<LogEntry>& entries);

    // Retrieve entry at a specific 1-based index
    std::optional<LogEntry> getEntry(uint64_t index) const;

    // Get term of entry at index (returns 0 for index 0)
    uint64_t getTerm(uint64_t index) const;

    // Log metadata helpers
    uint64_t lastIndex() const;
    uint64_t lastTerm() const;
    size_t size() const;

    // Truncate conflicting entries starting from index
    void truncate(uint64_t index);

    // --- Snapshot / Compaction ---
    void compact(uint64_t snapshot_index, uint64_t snapshot_term);
    uint64_t getLastIncludedIndex() const { return last_included_index_; }
    uint64_t getLastIncludedTerm() const { return last_included_term_; }

    // --- Commit / apply index management ---
    uint64_t getCommitIndex() const { return commit_index_; }
    void setCommitIndex(uint64_t index) { commit_index_ = index; }
    uint64_t getLastApplied() const { return last_applied_; }
    void setLastApplied(uint64_t index) { last_applied_ = index; }

    // =========================================================================
    // Durability API (group commit)
    // =========================================================================
    // waitForDurable(i): blocks until entry i is fsynced to disk.
    // getDurableIndex(): last fsynced index (lock-free snapshot).
    // =========================================================================
    bool waitForDurable(uint64_t index, std::chrono::milliseconds timeout);
    uint64_t getDurableIndex() const;
};

} // namespace raft

#endif // RAFT_LOG_HPP