#include "raft/consensus.hpp"
#include "common/protocol.hpp"
#include "network/client.hpp"
#include <random>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>

namespace raft {

RaftNode::RaftNode(int node_id, const std::vector<PeerInfo>& peers, kvstore::Store& store)
    : node_id_(node_id),
      current_term_(0),
      voted_for_(-1),
      log_(node_id),
      current_leader_(-1), 
      state_(NodeState::FOLLOWER),
      commit_index_(0),
      last_applied_(0),
      max_log_size_(10), 
      peers_(peers),
      store_(store),
      running_(false) {
    
    loadMetadata();

    std::string snap_file = "node_" + std::to_string(node_id_) + ".snap";
    if (store_.loadSnapshot(snap_file)) {
        std::cout << "[RaftNode " << node_id_ << "] Loaded State Machine Snapshot from disk.\n" << std::flush;
    }

    last_applied_ = log_.getLastIncludedIndex();
    commit_index_ = log_.getLastIncludedIndex();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(150, 300);
    election_timeout_ = std::chrono::milliseconds(dis(gen));
    last_heartbeat_time_ = std::chrono::steady_clock::now();
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::persistMetadata() {
    std::ofstream outfile("node_" + std::to_string(node_id_) + "_meta.dat", std::ios::trunc);
    if (outfile.is_open()) {
        outfile << current_term_ << " " << voted_for_ << "\n";
    }
}

void RaftNode::loadMetadata() {
    std::ifstream infile("node_" + std::to_string(node_id_) + "_meta.dat");
    if (infile.is_open()) {
        infile >> current_term_ >> voted_for_;
        std::cout << "[RaftNode " << node_id_ << "] Loaded metadata from disk: term=" << current_term_ << ", voted_for=" << voted_for_ << "\n" << std::flush;
    }
}

void RaftNode::start() {
    running_ = true;
    background_thread_ = std::thread(&RaftNode::runBackgroundLoop, this);
    std::cout << "[RaftNode " << node_id_ << "] Started in FOLLOWER state (Term: " << current_term_ << ")\n";
}

void RaftNode::stop() {
    if (running_) {
        running_ = false;
        if (background_thread_.joinable()) {
            background_thread_.join();
        }
    }
}

void RaftNode::runBackgroundLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        std::unique_lock<std::mutex> lock(mtx_);
        auto now = std::chrono::steady_clock::now();

        if (state_ == NodeState::LEADER) {
            sendHeartbeats();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } else {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_time_);
            if (elapsed > election_timeout_) {
                startElection();
            }
        }
    }
}

void RaftNode::startElection() {
    state_ = NodeState::CANDIDATE;
    current_term_++;
    voted_for_ = node_id_;
    current_leader_ = -1; 
    persistMetadata();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(150, 300);
    election_timeout_ = std::chrono::milliseconds(dis(gen));
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    std::cout << "[RaftNode " << node_id_ << "] Election timeout expired. Starting election for Term " << current_term_ << "\n";

    uint64_t saved_term = current_term_;
    uint64_t last_log_idx = log_.lastIndex();
    uint64_t last_log_term = log_.lastTerm();
    int votes = 1;

    std::vector<PeerInfo> current_peers = peers_;

    if (votes > (current_peers.size() + 1) / 2) {
        state_ = NodeState::LEADER;
        // <--- ADDED: Initialize per-peer routing tables
        next_index_.assign(10, log_.lastIndex() + 1); 
        match_index_.assign(10, 0);
        std::cout << "[RaftNode " << node_id_ << "] Won election! Promoted to LEADER for Term " << current_term_ << "\n";
        return;
    }

    mtx_.unlock();

    network::Client client;
    for (const auto& peer : current_peers) {
        RequestVoteArgs args;
        args.term = saved_term;
        args.candidate_id = node_id_;
        args.last_log_index = last_log_idx;
        args.last_log_term = last_log_term;

        std::string payload = common::Protocol::serializeRequestVote(args);
        auto reply_str = client.sendRpc(peer.ip, peer.port, payload);

        if (reply_str.has_value()) {
            std::istringstream iss(reply_str.value());
            std::string type;
            iss >> type;
            if (type == "VOTE_REPLY") {
                uint64_t reply_term;
                bool granted;
                iss >> reply_term >> granted;

                std::unique_lock<std::mutex> lock(mtx_);
                if (current_term_ != saved_term || state_ != NodeState::CANDIDATE) {
                    return;
                }
                if (reply_term > current_term_) {
                    current_term_ = reply_term;
                    state_ = NodeState::FOLLOWER;
                    voted_for_ = -1;
                    current_leader_ = -1; 
                    persistMetadata();
                    return;
                }
                if (granted) {
                    votes++;
                    if (votes > (current_peers.size() + 1) / 2) {
                        state_ = NodeState::LEADER;
                        // <--- ADDED: Initialize per-peer routing tables
                        next_index_.assign(10, log_.lastIndex() + 1); 
                        match_index_.assign(10, 0);
                        std::cout << "[RaftNode " << node_id_ << "] Won election! Promoted to LEADER for Term " << current_term_ << "\n";
                        return;
                    }
                }
            }
        }
    }
    mtx_.lock();
}

void RaftNode::sendHeartbeats() {
    uint64_t last_idx = log_.lastIndex();
    std::vector<PeerInfo> current_peers = peers_;
    uint64_t saved_term = current_term_;

    // Structure to securely build RPCs while holding the lock
    struct RPCContext {
        int peer_id;
        bool is_snapshot;
        std::string payload_str;
        uint64_t target_next_idx; 
        uint64_t target_match_idx;
    };
    std::vector<RPCContext> rpcs;

    for (const auto& peer : current_peers) {
        // Safety bound for vector
        if (peer.id >= next_index_.size()) continue; 
        
        uint64_t next_idx = next_index_[peer.id];
        RPCContext ctx;
        ctx.peer_id = peer.id;

        // <--- ADDED: Determine if we must send a Snapshot instead of standard logs
        if (next_idx <= log_.getLastIncludedIndex()) {
            ctx.is_snapshot = true;
            InstallSnapshotArgs snap_args;
            snap_args.term = saved_term;
            snap_args.leader_id = node_id_;
            snap_args.last_included_index = log_.getLastIncludedIndex();
            snap_args.last_included_term = log_.getLastIncludedTerm();
            
            std::ifstream infile("node_" + std::to_string(node_id_) + ".snap", std::ios::binary);
            if (infile) {
                std::ostringstream ss;
                ss << infile.rdbuf();
                snap_args.data = ss.str();
            }
            
            ctx.payload_str = common::Protocol::serializeInstallSnapshot(snap_args);
            ctx.target_next_idx = snap_args.last_included_index + 1;
            ctx.target_match_idx = snap_args.last_included_index;
        } 
        // <--- MODIFIED: Build true per-peer AppendEntries logic
        else {
            ctx.is_snapshot = false;
            uint64_t prev_idx = next_idx - 1;
            
            AppendEntriesArgs args;
            args.term = saved_term;
            args.leader_id = node_id_;
            args.prev_log_index = prev_idx;
            args.prev_log_term = log_.getTerm(prev_idx);
            args.leader_commit = commit_index_;

            for (uint64_t i = next_idx; i <= last_idx; i++) {
                auto entry = log_.getEntry(i);
                if (entry) args.entries.push_back(*entry);
            }
            
            ctx.payload_str = common::Protocol::serializeAppendEntries(args);
            ctx.target_next_idx = last_idx + 1;
            ctx.target_match_idx = last_idx;
        }
        rpcs.push_back(ctx);
    }

    mtx_.unlock();

    int acks = 1; // Self
    network::Client client;
    
    for (size_t i = 0; i < current_peers.size(); ++i) {
        const auto& peer = current_peers[i];
        const auto& ctx = rpcs[i];
        
        // Snapshots get a longer timeout (2000ms) to allow for large disk writes
        int timeout = ctx.is_snapshot ? 2000 : 100;
        auto reply_str = client.sendRpc(peer.ip, peer.port, ctx.payload_str, timeout);

        if (reply_str.has_value()) {
            std::istringstream iss(reply_str.value());
            std::string type;
            iss >> type;
            
            if (type == "SNAPSHOT_REPLY") {
                uint64_t reply_term;
                iss >> reply_term;

                std::unique_lock<std::mutex> lock(mtx_);
                if (reply_term > current_term_) {
                    current_term_ = reply_term;
                    state_ = NodeState::FOLLOWER;
                    voted_for_ = -1;
                    current_leader_ = -1;
                    persistMetadata();
                    return;
                }
                // Snapshot accepted! Advance pointers
                next_index_[peer.id] = ctx.target_next_idx;
                match_index_[peer.id] = ctx.target_match_idx;
            }
            else if (type == "APPEND_REPLY") {
                uint64_t reply_term;
                bool success;
                iss >> reply_term >> success;

                std::unique_lock<std::mutex> lock(mtx_);
                if (reply_term > current_term_) {
                    current_term_ = reply_term;
                    state_ = NodeState::FOLLOWER;
                    voted_for_ = -1;
                    current_leader_ = -1; 
                    persistMetadata();
                    return;
                }
                
                if (success) {
                    next_index_[peer.id] = ctx.target_next_idx;
                    match_index_[peer.id] = ctx.target_match_idx;
                    acks++;
                } else {
                    // <--- ADDED: Raft backfilling logic. Decrement so we send an older log next time
                    if (next_index_[peer.id] > 1) {
                        next_index_[peer.id]--;
                    }
                }
            }
        }
    }

    mtx_.lock();
    
    if (state_ == NodeState::LEADER && current_term_ == saved_term) {
        if (acks > (current_peers.size() + 1) / 2) {
            if (last_idx > commit_index_) {
                commit_index_ = last_idx;
                applyLogsToStore(); 
            }
        }
    }
}

RequestVoteReply RaftNode::handleRequestVote(const RequestVoteArgs& args) {
    std::unique_lock<std::mutex> lock(mtx_);
    RequestVoteReply reply;

    if (args.term > current_term_) {
        current_term_ = args.term;
        state_ = NodeState::FOLLOWER;
        voted_for_ = -1;
        current_leader_ = -1; 
        persistMetadata();
    }

    if (args.term == current_term_ && (voted_for_ == -1 || voted_for_ == args.candidate_id)) {
        uint64_t local_last_term = log_.lastTerm();
        uint64_t local_last_index = log_.lastIndex();

        bool log_ok = (args.last_log_term > local_last_term) ||
                      (args.last_log_term == local_last_term && args.last_log_index >= local_last_index);

        if (log_ok) {
            voted_for_ = args.candidate_id;
            persistMetadata();
            reply.vote_granted = true;
            last_heartbeat_time_ = std::chrono::steady_clock::now(); 
            std::cout << "[RaftNode " << node_id_ << "] Granted vote to Node " << args.candidate_id << " for Term " << current_term_ << "\n";
        } else {
            reply.vote_granted = false;
        }
    } else {
        reply.vote_granted = false;
    }

    reply.term = current_term_;
    return reply;
}

AppendEntriesReply RaftNode::handleAppendEntries(const AppendEntriesArgs& args) {
    std::unique_lock<std::mutex> lock(mtx_);
    AppendEntriesReply reply;

    if (args.term > current_term_) {
        current_term_ = args.term;
        state_ = NodeState::FOLLOWER;
        voted_for_ = -1;
        current_leader_ = -1; 
        persistMetadata();
    }

    if (args.term < current_term_) {
        reply.term = current_term_;
        reply.success = false;
        return reply;
    }

    state_ = NodeState::FOLLOWER;
    current_leader_ = args.leader_id; 
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    if (args.prev_log_index > 0 && log_.getTerm(args.prev_log_index) != args.prev_log_term) {
        reply.term = current_term_;
        reply.success = false;
        return reply;
    }

    if (!args.entries.empty()) {
        log_.appendEntries(args.prev_log_index, args.entries);
    }

    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, log_.lastIndex());
        applyLogsToStore();
    }

    reply.term = current_term_;
    reply.success = true;
    return reply;
}

// <--- ADDED: Follower applying the binary snapshot
InstallSnapshotReply RaftNode::handleInstallSnapshot(const InstallSnapshotArgs& args) {
    std::unique_lock<std::mutex> lock(mtx_);
    InstallSnapshotReply reply;

    if (args.term > current_term_) {
        current_term_ = args.term;
        state_ = NodeState::FOLLOWER;
        voted_for_ = -1;
        current_leader_ = -1;
        persistMetadata();
    }

    reply.term = current_term_;
    if (args.term < current_term_) {
        return reply; // Reject obsolete snapshots
    }

    state_ = NodeState::FOLLOWER;
    current_leader_ = args.leader_id;
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    // Write the binary data to disk
    std::string snap_file = "node_" + std::to_string(node_id_) + ".snap";
    std::ofstream out(snap_file, std::ios::binary | std::ios::trunc);
    if (out) {
        out.write(args.data.data(), args.data.size());
        out.close();
        
        // Instruct the KV store to wipe memory and load the file
        store_.loadSnapshot(snap_file);
        
        // Sync the log's compaction offset to match the new snapshot
        log_.compact(args.last_included_index, args.last_included_term);
        
        // Update tracking variables so we don't try to apply old logs
        commit_index_ = args.last_included_index;
        last_applied_ = args.last_included_index;
        
        std::cout << "[RaftNode " << node_id_ << "] Installed Snapshot from Leader (Index offset now: " 
                  << args.last_included_index << ")\n";
    }

    return reply;
}

void RaftNode::applyLogsToStore() {
    bool applied_any = false;

    while (commit_index_ > last_applied_) {
        last_applied_++;
        auto entry = log_.getEntry(last_applied_);
        if (entry.has_value()) {
            std::string cmd = entry.value().command;
            std::istringstream iss(cmd);
            std::string op;
            iss >> op;
            
            if (op == "SET") {
                std::string key, val;
                iss >> key >> val;
                store_.set(key, val); 
                std::cout << "[RaftNode " << node_id_ << "] COMMITTED to Store: " << key << "=" << val << "\n";
            }
            applied_any = true;
        }
    }

    if (applied_any) {
        checkAndTriggerSnapshot();
    }
}

void RaftNode::checkAndTriggerSnapshot() {
    uint64_t physical_size = log_.lastIndex() - log_.getLastIncludedIndex();
    
    if (physical_size >= max_log_size_) {
        std::cout << "[RaftNode " << node_id_ << "] Log size (" << physical_size 
                  << ") exceeded threshold. Triggering snapshot at index " << last_applied_ << "...\n";
                  
        std::string snap_file = "node_" + std::to_string(node_id_) + ".snap";
        
        if (store_.saveSnapshot(snap_file)) {
            log_.compact(last_applied_, log_.getTerm(last_applied_));
            std::cout << "[RaftNode " << node_id_ << "] Compaction complete.\n";
        } else {
            std::cerr << "[RaftNode " << node_id_ << "] ERROR: Failed to write state machine snapshot!\n";
        }
    }
}

bool RaftNode::propose(const std::string& command, uint64_t& out_index) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (state_ != NodeState::LEADER) {
        return false;
    }
    out_index = log_.append(current_term_, command);
    return true;
}

NodeState RaftNode::getState() const {
    std::unique_lock<std::mutex> lock(mtx_);
    return state_;
}

uint64_t RaftNode::getCurrentTerm() const {
    std::unique_lock<std::mutex> lock(mtx_);
    return current_term_;
}

int RaftNode::getLeaderId() const {
    std::unique_lock<std::mutex> lock(mtx_);
    return (state_ == NodeState::LEADER) ? node_id_ : current_leader_;
}

} // namespace raft