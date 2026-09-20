#include "store.hpp"
#include "network/server.hpp"
#include "network/socket.hpp"
#include "raft/consensus.hpp"
#include "common/protocol.hpp"
#include "config.hpp"
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <csignal>
#include <cstdlib>
#include <thread>
#include <chrono>

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------
raft::RaftNode* g_raft_node = nullptr;

void handle_signal(int signum) {
    std::cout << "\n[Node] Caught signal " << signum
              << ". Initiating graceful shutdown...\n";
    if (g_raft_node) {
        g_raft_node->stop();
    }
    std::cout.flush();
    std::cerr.flush();
    std::exit(0);
}

// ---------------------------------------------------------------------------
// Flag parsing helpers
// ---------------------------------------------------------------------------
static bool parse_bool(const std::string& s) {
    return s == "true" || s == "1" || s == "yes" || s == "on";
}

static std::string strip_prefix(const std::string& arg, const std::string& prefix) {
    return arg.substr(prefix.size());
}

int main(int argc, char* argv[]) {
    int node_id      = 1;
    int port         = 8081;
    int cluster_size = 3;

    // ---- Positional arguments ---------------------------------------------
    if (argc > 1) node_id = std::stoi(argv[1]);
    if (argc > 2) port    = std::stoi(argv[2]);

    // ---- Optional feature toggles -----------------------------------------
    auto& cfg = config::GlobalConfig::instance();

    for (int i = 3; i < argc; i++) {
        std::string arg = argv[i];

        // --striping=true|false   (and legacy --enable-lock-striping)
        if (arg.rfind("--striping=", 0) == 0) {
            cfg.enable_lock_striping = parse_bool(strip_prefix(arg, "--striping="));
        } else if (arg == "--striping") {
            cfg.enable_lock_striping = true;
        } else if (arg.rfind("--enable-lock-striping=", 0) == 0) {
            cfg.enable_lock_striping = parse_bool(strip_prefix(arg, "--enable-lock-striping="));
        } else if (arg == "--enable-lock-striping") {
            cfg.enable_lock_striping = true;

        // --async_io=true|false
        } else if (arg.rfind("--async_io=", 0) == 0) {
            cfg.enable_async_io = parse_bool(strip_prefix(arg, "--async_io="));
        } else if (arg == "--async_io") {
            cfg.enable_async_io = true;

        // --tripwire_offset=N
        } else if (arg.rfind("--tripwire_offset=", 0) == 0) {
            cfg.tripwire_offset = std::stoi(strip_prefix(arg, "--tripwire_offset="));

        // --cluster_size=N
        } else if (arg.rfind("--cluster_size=", 0) == 0) {
            cluster_size = std::stoi(strip_prefix(arg, "--cluster_size="));

        // --enable-tripwire=true|false
        } else if (arg.rfind("--enable-tripwire=", 0) == 0) {
            cfg.enable_tripwire = parse_bool(strip_prefix(arg, "--enable-tripwire="));
        } else if (arg == "--enable-tripwire") {
            cfg.enable_tripwire = true;

        } else {
            std::cerr << "[Node] Unknown flag: " << arg << "\n";
        }
    }

    // ---- Validate cluster_size / node_id ----------------------------------
    if (cluster_size < 1 || cluster_size > 3) {
        std::cerr << "[Node] Invalid cluster_size=" << cluster_size
                  << " (must be 1..3)\n";
        return 1;
    }
    if (node_id < 1 || node_id > cluster_size) {
        std::cerr << "[Node] node_id=" << node_id
                  << " is out of range for cluster_size=" << cluster_size << "\n";
        return 1;
    }

    cfg.printConfig();

    kvstore::Store db(node_id);

    // ---- Static topology (trimmed to cluster_size) ------------------------
    std::vector<raft::PeerInfo> all_nodes = {
        {1, "127.0.0.1", 8081},
        {2, "127.0.0.1", 8082},
        {3, "127.0.0.1", 8083}
    };
    all_nodes.resize(cluster_size);

    // ---- Peers = everyone except this node --------------------------------
    std::vector<raft::PeerInfo> peers;
    for (const auto& node : all_nodes) {
        if (node.id != node_id) {
            peers.push_back(node);
        }
    }

    // ---- Boot Raft ---------------------------------------------------------
    raft::RaftNode raft_node(node_id, peers, db);

    g_raft_node = &raft_node;
    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    raft_node.start();

    // ---- Boot server -------------------------------------------------------
    network::Server server(4);

    server.setHandler([&db, &raft_node, &all_nodes](int client_fd) {
        network::Socket client(client_fd);

        // Per-thread reusable receive buffer. The previous version allocated
        // and zero-initialized 1 MB on every request, which is ~1-2% of p50
        // and, because the allocation could fall back to mmap(), added
        // per-request kernel work that contended with the striped writers.
        thread_local std::vector<char> buffer(1024 * 1024);

        ssize_t bytes_rx = client.receiveData(buffer.data(), buffer.size() - 1);
        if (bytes_rx <= 0) return;

        std::string request(buffer.data(), bytes_rx);
        std::istringstream iss(request);
        std::string msg_type;
        iss >> msg_type;

        std::string response;

        // -------------------------------------------------------------------
        // Helper: build a redirect / election-in-progress response.
        // -------------------------------------------------------------------
        auto make_redirect_response = [&raft_node, &all_nodes]() -> std::string {
            int leader_id = raft_node.getLeaderId();
            if (leader_id == -1) {
                return "-ERROR Election in progress\n";
            }
            int leader_port = -1;
            for (const auto& node : all_nodes) {
                if (node.id == leader_id) {
                    leader_port = node.port;
                    break;
                }
            }
            if (leader_port == -1) {
                return "-ERROR Leader unknown\n";
            }
            return "-MOVED " + std::to_string(leader_port) + "\n";
        };

        // ---- Raft RPCs -----------------------------------------------------
        if (msg_type == "REQ_VOTE") {
            auto args  = common::Protocol::deserializeRequestVote(iss);
            auto reply = raft_node.handleRequestVote(args);
            response   = common::Protocol::serializeRequestVoteReply(reply);
        }
        else if (msg_type == "APPEND_ENTRIES") {
            auto args  = common::Protocol::deserializeAppendEntries(iss);
            auto reply = raft_node.handleAppendEntries(args);
            response   = common::Protocol::serializeAppendEntriesReply(reply);
        }
        else if (msg_type == "INSTALL_SNAPSHOT") {
            auto args  = common::Protocol::deserializeInstallSnapshot(iss);
            auto reply = raft_node.handleInstallSnapshot(args);
            response   = common::Protocol::serializeInstallSnapshotReply(reply);
        }

        // ---- Test-only: local read (used by eval_suite.py) -----------------
#ifdef KV_TEST_MODE
        else if (msg_type == "READ_LOCAL") {
            std::string key;
            iss >> key;
            auto val = db.get(key);
            response = val.has_value() ? val.value() + "\n" : "(nil)\n";
        }
#endif // KV_TEST_MODE

        // ---- Client KV operations ------------------------------------------
        else if (msg_type == "SET" || msg_type == "GET" || msg_type == "DEL") {
            if (raft_node.getState() != raft::NodeState::LEADER) {
                response = make_redirect_response();
            } else {
                if (msg_type == "SET") {
                    std::string key, val;
                    iss >> key >> val;
                    uint64_t index = 0;

                    if (!raft_node.propose("SET " + key + " " + val, index)) {
                        response = "-ERROR Consensus failure\n";
                    }
                    else if (!raft_node.waitForCommit(index)) {
                        response = make_redirect_response();
                    }
                    else {
                        response = "OK (Committed at index "
                                 + std::to_string(index) + ")\n";
                    }
                }
                else if (msg_type == "DEL") {
                    std::string key;
                    iss >> key;
                    uint64_t index = 0;

                    if (!raft_node.propose("DEL " + key, index)) {
                        response = "-ERROR Consensus failure\n";
                    }
                    else if (!raft_node.waitForCommit(index)) {
                        response = make_redirect_response();
                    }
                    else {
                        response = "OK (Committed DEL at index "
                                 + std::to_string(index) + ")\n";
                    }
                }
                else if (msg_type == "GET") {
                    if (!raft_node.hasValidLease()) {
                        response = "-ERROR Stale read prevented: Leader lease expired (network partition likely)\n";
                    } else {
                        std::string key;
                        iss >> key;
                        auto val = db.get(key);
                        response = val.has_value() ? val.value() + "\n" : "(nil)\n";
                    }
                }
            }
        }
        else {
            response = "ERROR: Unknown RPC type\n";
        }

        client.sendData(response);
    });

    std::cout << "[Node " << node_id
              << "] Distributed KV & Raft Node booting on port " << port << "...\n";
    std::cout.flush();

    server.start(port);

    while (true) {
        std::this_thread::sleep_for(std::chrono::hours(1));
    }
}