#ifndef RAFT_STATE_HPP
#define RAFT_STATE_HPP

#include <string>
#include <vector>
#include <cstdint>

namespace raft {

enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

// A single entry in the replicated log
struct LogEntry {
    uint64_t term;
    std::string command; // e.g., "SET key value"
};

// RPC structure for RequestVote (Leader Election)
struct RequestVoteArgs {
    uint64_t term;          // Candidate's term
    int candidate_id;       // Candidate requesting vote
    uint64_t last_log_index;// Index of candidate's last log entry
    uint64_t last_log_term; // Term of candidate's last log entry
};

struct RequestVoteReply {
    uint64_t term;          // Current term, for candidate to update itself
    bool vote_granted;      // True means candidate received vote
};

// RPC structure for AppendEntries (Heartbeats & Log Replication)
struct AppendEntriesArgs {
    uint64_t term;              // Leader's term
    int leader_id;              // So follower can redirect clients
    uint64_t prev_log_index;    // Index of log entry immediately preceding new ones
    uint64_t prev_log_term;     // Term of prev_log_index entry
    std::vector<LogEntry> entries;// Log entries to store (empty for heartbeat)
    uint64_t leader_commit;     // Leader's commit index
};

struct AppendEntriesReply {
    uint64_t term;              // Current term, for leader to update itself
    bool success;               // True if follower contained entry matching prev_log_index/term
};

// <--- ADDED: RPC structures for InstallSnapshot
struct InstallSnapshotArgs {
    uint64_t term;
    int leader_id;
    uint64_t last_included_index;
    uint64_t last_included_term;
    std::string data;           // The raw binary snapshot file content
};

struct InstallSnapshotReply {
    uint64_t term;              // Current term, for leader to update itself
};

} // namespace raft

#endif // RAFT_STATE_HPP