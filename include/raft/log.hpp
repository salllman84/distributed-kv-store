#ifndef RAFT_LOG_HPP
#define RAFT_LOG_HPP

#include "raft/state.hpp"
#include <vector>
#include <optional>
#include <string>
#include <cstdint>

namespace raft {

class RaftLog {
private:
    int node_id_;
    std::vector<LogEntry> entries_;
    uint64_t commit_index_;
    uint64_t last_applied_;
    std::string filename_;

    // --- Added offset state for Log Compaction ---
    uint64_t last_included_index_;
    uint64_t last_included_term_;

    void persist() const;
    void load();

public:
    explicit RaftLog(int node_id);
    ~RaftLog() = default;

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
    size_t size() const; // Returns logical log size

    // Truncate conflicting entries starting from index
    void truncate(uint64_t index);

    // --- Snapshot / Compaction Methods ---
    void compact(uint64_t snapshot_index, uint64_t snapshot_term);
    uint64_t getLastIncludedIndex() const { return last_included_index_; }
    uint64_t getLastIncludedTerm() const { return last_included_term_; }

    // Commit index management
    uint64_t getCommitIndex() const { return commit_index_; }
    void setCommitIndex(uint64_t index) { commit_index_ = index; }

    uint64_t getLastApplied() const { return last_applied_; }
    void setLastApplied(uint64_t index) { last_applied_ = index; }
};

} // namespace raft

#endif // RAFT_LOG_HPP