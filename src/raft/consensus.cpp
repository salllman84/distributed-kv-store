#include "config.hpp"
#include "raft/consensus.hpp"
#include "common/protocol.hpp"
#include "network/client.hpp"
#include <random>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <sstream>
#include <algorithm>
#include <atomic>

namespace raft {

// =============================================================================
// CONSTRUCTOR
// =============================================================================
// CHANGE FROM PREVIOUS VERSION:
//   The old constructor called store_.registerStepDownCallback([this]() {
//       std::thread([this]() { std::lock_guard<std::mutex> lock(mtx_); ... }).detach();
//   });
//   That detached thread mutated Raft state from an arbitrary background thread
//   and raced with AppendEntries / RequestVote / propose().
//
// NEW DESIGN:
//   The Store now publishes a single atomic event (storage_degraded_flag_).
//   The Raft main tick thread (runBackgroundLoop) consumes it and performs
//   the step-down under mtx_, serialized with every other Raft event.
//   No callback registration is required — the boundary is a single atomic.
// =============================================================================
RaftNode::RaftNode(int node_id, const std::vector<PeerInfo>& peers, kvstore::Store& store)
    : node_id_(node_id),
      current_term_(0),
      voted_for_(-1),
      log_(node_id),
      current_leader_(-1),
      state_(NodeState::FOLLOWER),
      commit_index_(0),
      last_applied_(0),
      max_log_size_(10000),
      leader_lease_end_(std::chrono::steady_clock::time_point::min()),
      peers_(peers),
      store_(store),
      running_(false),
      telemetry_start_time_(std::chrono::steady_clock::now()) {

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

    // -------------------------------------------------------------------------
    // REMOVED: store_.registerStepDownCallback(...) + detached thread.
    // The Store now signals via storage_degraded_flag_ (std::atomic<bool>).
    // The Raft main tick thread polls and consumes it — see runBackgroundLoop().
    // -------------------------------------------------------------------------
}

RaftNode::~RaftNode() {
    stop();
}

// =============================================================================
// PERSISTENCE
// =============================================================================
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
        std::cout << "[RaftNode " << node_id_ << "] Loaded metadata from disk: term="
                  << current_term_ << ", voted_for=" << voted_for_ << "\n" << std::flush;
    }
}

// =============================================================================
// LIFECYCLE
// =============================================================================
void RaftNode::start() {
    running_ = true;
    background_thread_ = std::thread(&RaftNode::runBackgroundLoop, this);

    telemetry_start_time_ = std::chrono::steady_clock::now();
    telemetry_thread_ = std::thread(&RaftNode::runTelemetryLoop, this);

    std::cout << "[RaftNode " << node_id_ << "] Started in FOLLOWER state (Term: "
              << current_term_ << ")\n" << std::flush;
}

void RaftNode::stop() {
    if (running_) {
        running_ = false;
        if (background_thread_.joinable()) {
            background_thread_.join();
        }
        if (telemetry_thread_.joinable()) {
            telemetry_thread_.join();
        }
    }
}

// =============================================================================
// MAIN RAFT TICK LOOP — the ONLY thread permitted to transition Raft state.
// =============================================================================
// Polls the async event tripwire at the top of every tick. If the LSM-tree
// has signaled storage degradation, we consume the flag and execute the
// step-down here, under mtx_, so it is serialized with AppendEntries,
// RequestVote, propose(), and election timeouts.
// =============================================================================
void RaftNode::runBackgroundLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        // ---------------------------------------------------------------------
        // ASYNC EVENT TRIPWIRE — poll on the main Raft tick thread.
        // consumeStorageDegradedFlag() is a lock-free atomic exchange.
        // ---------------------------------------------------------------------
        if (store_.consumeStorageDegradedFlag()) {
            checkStorageDegraded();
        }

        std::unique_lock<std::mutex> lock(mtx_);
        auto now = std::chrono::steady_clock::now();

        if (state_ == NodeState::LEADER) {
            sendHeartbeats();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } else {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_heartbeat_time_);
            if (elapsed > election_timeout_) {
                startElection();
            }
        }
    }
}

// =============================================================================
// ASYNC EVENT TRIPWIRE — main-thread step-down handler.
// =============================================================================
// Invoked exclusively from runBackgroundLoop(). Acquires mtx_ and performs
// the FOLLOWER transition under the same lock that guards every other Raft
// event. Followers/candidates simply ignore the event (the flag was already
// consumed, so no action is needed).
// =============================================================================
void RaftNode::checkStorageDegraded() {
    std::unique_lock<std::mutex> lock(mtx_);

    // Only a leader can step down. Non-leaders have nothing to do.
    if (state_ != NodeState::LEADER) return;

    // Defensive guard: the Store only signals when the tripwire is enabled,
    // but we re-check here so behavior is deterministic under ablation.
    if (!config::GlobalConfig::instance().enable_tripwire) return;

    std::cout << "\n[RaftNode " << node_id_
              << "] \033[1;31mASYNC EVENT TRIPWIRE: STORAGE OVERLOAD DETECTED!\033[0m\n";
    std::cout << "[RaftNode " << node_id_
              << "] Gracefully stepping down to FOLLOWER to protect cluster tail latency.\n\n"
              << std::flush;

    state_ = NodeState::FOLLOWER;
    voted_for_ = -1;
    current_leader_ = -1;
    persistMetadata();

    // Reset election timer so this node does not immediately campaign against
    // the still-healthy peers and trigger an unnecessary election storm.
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    // NOTE on client connections:
    // This Raft node does not own client sockets — the network::Server owns
    // them. Once state_ != LEADER, every subsequent propose() from a client
    // returns false, and any in-flight client RPC observes that on its next
    // propose(). The application layer translates this into an error/redirect
    // to the newly elected leader. No socket ownership is transferred here,
    // which is precisely why the refactor is safe: we mutate only Raft state.
}

// =============================================================================
// ELECTION
// =============================================================================
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

    std::cout << "[RaftNode " << node_id_ << "] Election timeout expired. Starting election for Term "
              << current_term_ << "\n" << std::flush;

    uint64_t saved_term = current_term_;
    uint64_t last_log_idx = log_.lastIndex();
    uint64_t last_log_term = log_.lastTerm();
    int votes = 1;

    std::vector<PeerInfo> current_peers = peers_;

    if (votes > (current_peers.size() + 1) / 2) {
        state_ = NodeState::LEADER;
        next_index_.assign(10, log_.lastIndex() + 1);
        match_index_.assign(10, 0);
        leader_lease_end_ = std::chrono::steady_clock::now() + election_timeout_;
        std::cout << "[RaftNode " << node_id_ << "] Won election! Promoted to LEADER for Term "
                  << current_term_ << "\n" << std::flush;
        return;
    }

    mtx_.unlock();

    network::Client client;
    bool step_down = false;
    uint64_t new_term = 0;
    int votes_received = 0;

    for (const auto& peer : current_peers) {
        RequestVoteArgs args;
        args.term = saved_term;
        args.candidate_id = node_id_;
        args.last_log_index = last_log_idx;
        args.last_log_term = last_log_term;

        std::string payload = common::Protocol::serializeRequestVote(args);

        // --- PHASE 2: Record RequestVote RPC for telemetry ---
        recordRequestVoteRPC();

        auto reply_str = client.sendRpc(peer.ip, peer.port, payload, 50);

        if (reply_str.has_value()) {
            std::istringstream iss(reply_str.value());
            std::string type;
            iss >> type;
            if (type == "VOTE_REPLY") {
                uint64_t reply_term;
                bool granted;
                iss >> reply_term >> granted;

                if (reply_term > saved_term) {
                    step_down = true;
                    new_term = reply_term;
                }
                if (granted) {
                    votes_received++;
                }
            }
        }
    }

    mtx_.lock();

    if (step_down && new_term > current_term_) {
        current_term_ = new_term;
        state_ = NodeState::FOLLOWER;
        voted_for_ = -1;
        current_leader_ = -1;
        persistMetadata();
        return;
    }

    if (current_term_ != saved_term || state_ != NodeState::CANDIDATE) {
        return;
    }

    votes += votes_received;
    if (votes > (current_peers.size() + 1) / 2) {
        state_ = NodeState::LEADER;
        next_index_.assign(10, log_.lastIndex() + 1);
        match_index_.assign(10, 0);
        leader_lease_end_ = std::chrono::steady_clock::now() + election_timeout_;
        std::cout << "[RaftNode " << node_id_ << "] Won election! Promoted to LEADER for Term "
                  << current_term_ << "\n" << std::flush;
    }
}

// =============================================================================
// HEARTBEATS / LOG REPLICATION
// =============================================================================
void RaftNode::sendHeartbeats() {
    uint64_t last_idx = log_.lastIndex();
    std::vector<PeerInfo> current_peers = peers_;
    uint64_t saved_term = current_term_;
    auto lease_start_time = std::chrono::steady_clock::now();

    struct RPCContext {
        int peer_id;
        bool is_snapshot;
        std::string payload_str;
        uint64_t target_next_idx;
        uint64_t target_match_idx;
    };
    std::vector<RPCContext> rpcs;

    for (const auto& peer : current_peers) {
        if (peer.id >= next_index_.size()) continue;

        uint64_t next_idx = next_index_[peer.id];
        RPCContext ctx;
        ctx.peer_id = peer.id;

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
        else {
            ctx.is_snapshot = false;
            uint64_t prev_idx = next_idx - 1;

            AppendEntriesArgs args;
            args.term = saved_term;
            args.leader_id = node_id_;
            args.prev_log_index = prev_idx;
            args.prev_log_term = log_.getTerm(prev_idx);
            args.leader_commit = commit_index_;

            // Cap batch size to 200 entries to prevent Death Spiral.
            uint64_t end_idx = std::min(last_idx, next_idx + 50 - 1);
            for (uint64_t i = next_idx; i <= end_idx; i++) {
                auto entry = log_.getEntry(i);
                if (entry) args.entries.push_back(*entry);
            }

            ctx.payload_str = common::Protocol::serializeAppendEntries(args);
            ctx.target_next_idx = end_idx + 1;
            ctx.target_match_idx = end_idx;
        }
        rpcs.push_back(ctx);
    }

    mtx_.unlock();

    int acks = 1; // Self
    bool step_down = false;
    uint64_t new_term = 0;

    struct ReplyInfo {
        int peer_id;
        bool success;
        uint64_t target_next;
        uint64_t target_match;
    };
    std::vector<ReplyInfo> valid_replies;

    network::Client client;
    for (size_t i = 0; i < current_peers.size(); ++i) {
        const auto& peer = current_peers[i];
        const auto& ctx = rpcs[i];

        // Increased normal RPC timeout to 1 full second (1000ms).
        int timeout = ctx.is_snapshot ? 2000 : 1000;

        // --- PHASE 2: Record AppendEntries RPC for telemetry ---
        if (!ctx.is_snapshot) {
            recordAppendEntriesRPC();
        }

        auto reply_str = client.sendRpc(peer.ip, peer.port, ctx.payload_str, timeout);

        if (reply_str.has_value()) {
            std::istringstream iss(reply_str.value());
            std::string type;
            iss >> type;

            if (type == "SNAPSHOT_REPLY") {
                uint64_t reply_term;
                iss >> reply_term;
                if (reply_term > saved_term) {
                    step_down = true;
                    new_term = reply_term;
                } else {
                    valid_replies.push_back({peer.id, true, ctx.target_next_idx, ctx.target_match_idx});
                }
            }
            else if (type == "APPEND_REPLY") {
                uint64_t reply_term;
                bool success;
                iss >> reply_term >> success;

                if (reply_term > saved_term) {
                    step_down = true;
                    new_term = reply_term;
                } else {
                    valid_replies.push_back({peer.id, success, ctx.target_next_idx, ctx.target_match_idx});
                }
            }
        }
    }

    mtx_.lock();

    if (step_down && new_term > current_term_) {
        current_term_ = new_term;
        state_ = NodeState::FOLLOWER;
        voted_for_ = -1;
        current_leader_ = -1;
        persistMetadata();
        return;
    }

    if (state_ != NodeState::LEADER || current_term_ != saved_term) {
        return;
    }

    for (const auto& r : valid_replies) {
        if (r.success) {
            next_index_[r.peer_id] = r.target_next;
            match_index_[r.peer_id] = r.target_match;
            acks++;
        } else {
            if (next_index_[r.peer_id] > 1) {
                next_index_[r.peer_id]--;
            }
        }
    }

    if (acks > (current_peers.size() + 1) / 2) {
        leader_lease_end_ = lease_start_time + election_timeout_;
        if (last_idx > commit_index_) {
            commit_index_ = last_idx;
            applyLogsToStore();
        }
    }
}

// =============================================================================
// RPC HANDLERS
// =============================================================================
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
            std::cout << "[RaftNode " << node_id_ << "] Granted vote to Node "
                      << args.candidate_id << " for Term " << current_term_ << "\n" << std::flush;
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
        return reply;
    }

    state_ = NodeState::FOLLOWER;
    current_leader_ = args.leader_id;
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    std::string snap_file = "node_" + std::to_string(node_id_) + ".snap";
    std::ofstream out(snap_file, std::ios::binary | std::ios::trunc);
    if (out) {
        out.write(args.data.data(), args.data.size());
        out.close();

        store_.loadSnapshot(snap_file);
        log_.compact(args.last_included_index, args.last_included_term);

        commit_index_ = args.last_included_index;
        last_applied_ = args.last_included_index;

        std::cout << "[RaftNode " << node_id_ << "] Installed Snapshot from Leader (Index offset now: "
                  << args.last_included_index << ")\n" << std::flush;
    }

    return reply;
}

// =============================================================================
// LOG APPLICATION
// =============================================================================
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
            }
            else if (op == "DEL") {
                std::string key;
                iss >> key;
                store_.remove(key);
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
                  << ") exceeded threshold. Triggering snapshot at index "
                  << last_applied_ << "...\n" << std::flush;

        std::string snap_file = "node_" + std::to_string(node_id_) + ".snap";

        if (store_.saveSnapshot(snap_file)) {
            log_.compact(last_applied_, log_.getTerm(last_applied_));
            std::cout << "[RaftNode " << node_id_ << "] Compaction complete.\n" << std::flush;
        } else {
            std::cerr << "[RaftNode " << node_id_
                      << "] ERROR: Failed to write state machine snapshot!\n" << std::flush;
        }
    }
}

// =============================================================================
// CLIENT ENTRYPOINT
// =============================================================================
bool RaftNode::propose(const std::string& command, uint64_t& out_index) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (state_ != NodeState::LEADER) {
        // After an async-event-tripwire step-down, clients observe this as
        // false and the application layer redirects them to the new leader.
        return false;
    }
    out_index = log_.append(current_term_, command);
    return true;
}

// =============================================================================
// STATE GETTERS
// =============================================================================
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

bool RaftNode::hasValidLease() const {
    std::unique_lock<std::mutex> lock(mtx_);
    if (state_ != NodeState::LEADER) return false;
    return std::chrono::steady_clock::now() < leader_lease_end_;
}

// =============================================================================
// TELEMETRY
// =============================================================================
std::string RaftNode::getStateString() const {
    std::unique_lock<std::mutex> lock(mtx_);
    switch(state_) {
        case NodeState::FOLLOWER:  return "FOLLOWER";
        case NodeState::CANDIDATE: return "CANDIDATE";
        case NodeState::LEADER:    return "LEADER";
        default:                   return "UNKNOWN";
    }
}

uint64_t RaftNode::getMemoryUsageMB() const {
    std::ifstream statm("/proc/self/statm");
    if (!statm.is_open()) {
        return 0;
    }

    uint64_t vsize, rss;
    statm >> vsize >> rss;
    statm.close();

    uint64_t page_size = 4096;
    return (rss * page_size) / (1024 * 1024);
}

void RaftNode::runTelemetryLoop() {
    std::string csv_file = "logs/node_" + std::to_string(node_id_) + "_telemetry.csv";
    std::ofstream csv_out(csv_file, std::ios::trunc);

    if (!csv_out.is_open()) {
        std::cerr << "[RaftNode " << node_id_
                  << "] Failed to open telemetry CSV: " << csv_file << "\n" << std::flush;
        return;
    }

    csv_out << "Timestamp_ms,Raft_State,Outgoing_RPCs_Per_Sec,Memory_Usage_MB\n";
    csv_out.flush();

    std::cout << "[RaftNode " << node_id_
              << "] Telemetry tracking started. Logging to: " << csv_file << "\n" << std::flush;

    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        uint64_t current_rpc_count = rpc_counter_append_entries_.load()
                                   + rpc_counter_request_vote_.load();
        uint64_t rpc_delta = current_rpc_count - last_rpc_count_;
        uint64_t rpcs_per_sec = (rpc_delta * 2); // *2 because we sample every 500ms
        last_rpc_count_ = current_rpc_count;

        std::string state = getStateString();
        uint64_t memory_mb = getMemoryUsageMB();

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - telemetry_start_time_
        );
        uint64_t timestamp_ms = elapsed.count();

        csv_out << timestamp_ms << ","
                << state << ","
                << rpcs_per_sec << ","
                << memory_mb << "\n";
        csv_out.flush();
    }

    csv_out.close();
}

void RaftNode::recordRequestVoteRPC() {
    rpc_counter_request_vote_.fetch_add(1, std::memory_order_relaxed);
}

void RaftNode::recordAppendEntriesRPC() {
    rpc_counter_append_entries_.fetch_add(1, std::memory_order_relaxed);
}

} // namespace raft