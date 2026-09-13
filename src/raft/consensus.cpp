#include "raft/consensus.hpp"
#include "common/protocol.hpp"
#include "network/client.hpp"
#include <random>
#include <iostream>
#include <thread>
#include <chrono>

namespace raft {

RaftNode::RaftNode(int node_id, const std::vector<PeerInfo>& peers, kvstore::Store& store)
    : node_id_(node_id),
      current_term_(0),
      voted_for_(-1),
      state_(NodeState::FOLLOWER),
      commit_index_(0),
      last_applied_(0),
      peers_(peers),
      store_(store),
      running_(false) {
    
    // Seed randomized election timeout between 150ms and 300ms
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(150, 300);
    election_timeout_ = std::chrono::milliseconds(dis(gen));
    last_heartbeat_time_ = std::chrono::steady_clock::now();
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    running_ = true;
    background_thread_ = std::thread(&RaftNode::runBackgroundLoop, this);
    std::cout << "[RaftNode " << node_id_ << "] Started in FOLLOWER state (Term: 0)\n";
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
            // Leader periodically sends heartbeats
            sendHeartbeats();
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Heartbeat interval
        } else {
            // Check if election timeout has elapsed for Follower/Candidate
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
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(150, 300);
    election_timeout_ = std::chrono::milliseconds(dis(gen));
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    std::cout << "[RaftNode " << node_id_ << "] Election timeout expired. Starting election for Term " << current_term_ << "\n";

    uint64_t saved_term = current_term_;
    uint64_t last_log_idx = log_.lastIndex();
    uint64_t last_log_term = log_.lastTerm();
    int votes = 1; // Vote for self

    // Copy peers list under lock, then release lock for network calls
    std::vector<PeerInfo> current_peers = peers_;
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
                    return; // State changed during RPC
                }
                if (reply_term > current_term_) {
                    current_term_ = reply_term;
                    state_ = NodeState::FOLLOWER;
                    voted_for_ = -1;
                    return;
                }
                if (granted) {
                    votes++;
                    if (votes > (current_peers.size() + 1) / 2) {
                        state_ = NodeState::LEADER;
                        std::cout << "[RaftNode " << node_id_ << "] Won election! Promoted to LEADER for Term " << current_term_ << "\n";
                        // Initialize next_index and match_index for peers here
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
    
    AppendEntriesArgs args;
    args.term = current_term_;
    args.leader_id = node_id_;
    args.prev_log_index = commit_index_; 
    args.prev_log_term = log_.getTerm(commit_index_);
    args.leader_commit = commit_index_;

    // Attach any uncommitted entries to the payload
    for (uint64_t i = commit_index_ + 1; i <= last_idx; i++) {
        auto entry = log_.getEntry(i);
        if (entry) args.entries.push_back(*entry);
    }

    std::vector<PeerInfo> current_peers = peers_;
    uint64_t saved_term = current_term_;

    mtx_.unlock();

    int acks = 1; // Leader implicitly acks its own log
    network::Client client;
    
    for (const auto& peer : current_peers) {
        std::string payload = common::Protocol::serializeAppendEntries(args);
        auto reply_str = client.sendRpc(peer.ip, peer.port, payload, 100);

        if (reply_str.has_value()) {
            std::istringstream iss(reply_str.value());
            std::string type;
            iss >> type;
            if (type == "APPEND_REPLY") {
                uint64_t reply_term;
                bool success;
                iss >> reply_term >> success;

                if (reply_term > saved_term) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    current_term_ = reply_term;
                    state_ = NodeState::FOLLOWER;
                    voted_for_ = -1;
                    return;
                }
                if (success) {
                    acks++;
                }
            }
        }
    }

    mtx_.lock();
    
    // If a majority replicated the log, advance the commit index
    if (state_ == NodeState::LEADER && current_term_ == saved_term) {
        if (acks > (current_peers.size() + 1) / 2) {
            if (last_idx > commit_index_) {
                commit_index_ = last_idx;
                applyLogsToStore(); // Leader applies first
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
    }

    if (args.term == current_term_ && (voted_for_ == -1 || voted_for_ == args.candidate_id)) {
        // Check if candidate's log is at least as up-to-date as receiver's log
        uint64_t local_last_term = log_.lastTerm();
        uint64_t local_last_index = log_.lastIndex();

        bool log_ok = (args.last_log_term > local_last_term) ||
                      (args.last_log_term == local_last_term && args.last_log_index >= local_last_index);

        if (log_ok) {
            voted_for_ = args.candidate_id;
            reply.vote_granted = true;
            last_heartbeat_time_ = std::chrono::steady_clock::now(); // Reset timer
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
    }

    if (args.term < current_term_) {
        reply.term = current_term_;
        reply.success = false;
        return reply;
    }

    // Valid heartbeat or log replication from current leader
    state_ = NodeState::FOLLOWER;
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    // Check if log contains an entry at prev_log_index matching prev_log_term
    if (args.prev_log_index > 0 && log_.getTerm(args.prev_log_index) != args.prev_log_term) {
        reply.term = current_term_;
        reply.success = false;
        return reply;
    }

    // Process new entries
    if (!args.entries.empty()) {
        log_.appendEntries(args.prev_log_index, args.entries);
    }

    // Update commit index
    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, log_.lastIndex());
        applyLogsToStore();
    }

    reply.term = current_term_;
    reply.success = true;
    return reply;
}

void RaftNode::applyLogsToStore() {
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
                store_.set(key, val); // Actually execute the write!
                std::cout << "[RaftNode " << node_id_ << "] COMMITTED to Store: " << key << "=" << val << "\n";
            }
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
    return (state_ == NodeState::LEADER) ? node_id_ : -1;
}

} // namespace raft