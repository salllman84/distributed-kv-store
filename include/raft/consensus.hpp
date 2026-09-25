#ifndef RAFT_CONSENSUS_HPP
#define RAFT_CONSENSUS_HPP

#include "raft/state.hpp"
#include "raft/log.hpp"
#include "store.hpp"
#include <vector>
#include <string>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>
#include <atomic>

namespace raft {

struct PeerInfo {
    int id;
    std::string ip;
    int port;
};

class RaftNode {
private:
    // -------------------------------------------------------------------------
    // Telemetry
    // -------------------------------------------------------------------------
    std::chrono::steady_clock::time_point telemetry_start_time_;
    std::thread telemetry_thread_;
    std::atomic<uint64_t> rpc_counter_append_entries_{0};
    std::atomic<uint64_t> rpc_counter_request_vote_{0};
    uint64_t last_rpc_count_{0};

    // -------------------------------------------------------------------------
    // Persistent state on all servers
    // -------------------------------------------------------------------------
    int node_id_;
    uint64_t current_term_;
    int voted_for_;
    RaftLog log_;

    // -------------------------------------------------------------------------
    // Volatile state tracking the known leader
    // -------------------------------------------------------------------------
    int current_leader_;

    // -------------------------------------------------------------------------
    // Volatile state on all servers
    // -------------------------------------------------------------------------
    NodeState state_;
    uint64_t commit_index_;
    uint64_t last_applied_;

    // -------------------------------------------------------------------------
    // Compaction state
    // -------------------------------------------------------------------------
    size_t max_log_size_;
    void checkAndTriggerSnapshot();

    // -------------------------------------------------------------------------
    // Linearizable read lease
    // -------------------------------------------------------------------------
    std::chrono::steady_clock::time_point leader_lease_end_;
    
    // ---------------------------------------------------------------------
    // Sticky step-down: a node that just stepped down due to storage
    // degradation must not campaign until this deadline passes.
    // ---------------------------------------------------------------------
    std::chrono::steady_clock::time_point storage_self_excluded_until_{
        std::chrono::steady_clock::time_point::min()};

    // -------------------------------------------------------------------------
    // Volatile state on leaders (reinitialized after election)
    // -------------------------------------------------------------------------
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;

    // -------------------------------------------------------------------------
    // Cluster topology
    // -------------------------------------------------------------------------
    std::vector<PeerInfo> peers_;

    // -------------------------------------------------------------------------
    // Reference to local state machine
    // -------------------------------------------------------------------------
    kvstore::Store& store_;

    // -------------------------------------------------------------------------
    // Concurrency control
    // -------------------------------------------------------------------------
    mutable std::mutex mtx_;
    std::atomic<bool> running_;

    // -------------------------------------------------------------------------
    // FIX D — Commit-pending wakeup.
    // -------------------------------------------------------------------------

    std::condition_variable tick_cv_;
    std::atomic<bool> commit_pending_{false};
    std::condition_variable commit_cv_;

    // -------------------------------------------------------------------------
    // Election timeout management
    // -------------------------------------------------------------------------
    std::chrono::milliseconds election_timeout_;
    std::chrono::steady_clock::time_point last_heartbeat_time_;
    std::thread background_thread_;

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------
    void runBackgroundLoop();
    void startElection();
    void sendHeartbeats();
    void applyLogsToStore();
    void persistMetadata();
    void loadMetadata();

    // =========================================================================
    // ASYNC EVENT TRIPWIRE — main-thread step-down handler.
    // -------------------------------------------------------------------------
    // Invoked EXCLUSIVELY from runBackgroundLoop() after the Store's
    // storage_degraded_flag_ has been consumed. Acquires mtx_ and performs
    // the FOLLOWER transition under the same lock that guards every other
    // Raft event, so the step-down is serialized with AppendEntries,
    // RequestVote, propose(), and election timeouts.
    //
    // This replaces the previous design where a detached thread inside the
    // LSM-tree compaction path acquired mtx_ out-of-band and raced with the
    // Raft event loop.
    // =========================================================================
    void checkStorageDegraded();

public:
    // -------------------------------------------------------------------------
    // Telemetry API
    // -------------------------------------------------------------------------
    void recordRequestVoteRPC();
    void recordAppendEntriesRPC();
    std::string getStateString() const;
    uint64_t getMemoryUsageMB() const;
    void runTelemetryLoop();

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------
    RaftNode(int node_id, const std::vector<PeerInfo>& peers, kvstore::Store& store);
    ~RaftNode();

    // Prevent copying
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void start();
    void stop();

    // -------------------------------------------------------------------------
    // RPC handlers called by the network layer
    // -------------------------------------------------------------------------
    RequestVoteReply handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply handleAppendEntries(const AppendEntriesArgs& args);
    InstallSnapshotReply handleInstallSnapshot(const InstallSnapshotArgs& args);

    // -------------------------------------------------------------------------
    // Client request entrypoint
    // -------------------------------------------------------------------------
    bool propose(const std::string& command, uint64_t& out_index);

    // =========================================================================
    // FIX C1 — waitForCommit
    // -------------------------------------------------------------------------
    // Block until commit_index_ >= target_index, or until this node loses
    // leadership, or until the timeout expires.
    //
    // Called by the client handler in main.cpp so that a successful "OK"
    // from SET/DEL means the entry is durably committed (replicated to a
    // majority and applied to the state machine), not merely appended to
    // the leader's local log.
    //
    // Returns:
    //   true  — entry is committed and applied
    //   false — timed out, stepped down, or never became leader
    //
    // Thread-safety: releases mtx_ between 2 ms polls so the Raft tick
    // thread and RPC handlers can continue making progress.
    // =========================================================================
    bool waitForCommit(uint64_t target_index,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(2000));

    // -------------------------------------------------------------------------
    // State getters for testing and monitoring
    // -------------------------------------------------------------------------
    NodeState getState() const;
    uint64_t getCurrentTerm() const;
    int getLeaderId() const;

    // -------------------------------------------------------------------------
    // Linearizable read lease check
    // -------------------------------------------------------------------------
    bool hasValidLease() const;

    // =========================================================================
    // NOTE: registerStepDownCallback() has been REMOVED.
    // -------------------------------------------------------------------------
    // The Raft <-> Store boundary is now a single std::atomic<bool> owned by
    // the Store (storage_degraded_flag_) and consumed exclusively by
    // runBackgroundLoop(). No external thread may mutate Raft state.
    // =========================================================================
};

} // namespace raft

#endif // RAFT_CONSENSUS_HPP