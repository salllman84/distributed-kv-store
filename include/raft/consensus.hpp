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
    // Persistent state on all servers
    int node_id_;
    uint64_t current_term_;
    int voted_for_;
    RaftLog log_;

    // Volatile state on all servers
    NodeState state_;
    uint64_t commit_index_;
    uint64_t last_applied_;

    // Volatile state on leaders (reinitialized after election)
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;

    // Cluster topology
    std::vector<PeerInfo> peers_;

    // Reference to local state machine
    kvstore::Store& store_;

    // Concurrency control
    mutable std::mutex mtx_;
    std::atomic<bool> running_;

    // Election timeout management
    std::chrono::milliseconds election_timeout_;
    std::chrono::steady_clock::time_point last_heartbeat_time_;
    std::thread background_thread_;

    // Helper methods
    void runBackgroundLoop();
    void startElection();
    void sendHeartbeats();
    void applyLogsToStore();
    void persistMetadata();
    void loadMetadata();

public:
    RaftNode(int node_id, const std::vector<PeerInfo>& peers, kvstore::Store& store);
    ~RaftNode();

    // Prevent copying
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // Lifecycle control
    void start();
    void stop();

    // RPC Handlers called by the network layer
    RequestVoteReply handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply handleAppendEntries(const AppendEntriesArgs& args);

    // Client request entrypoint
    bool propose(const std::string& command, uint64_t& out_index);

    // State getters for testing and monitoring
    NodeState getState() const;
    uint64_t getCurrentTerm() const;
    int getLeaderId() const;
};

} // namespace raft

#endif // RAFT_CONSENSUS_HPP