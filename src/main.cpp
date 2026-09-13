#include "store.hpp"
#include "network/server.hpp"
#include "network/socket.hpp"
#include "raft/consensus.hpp"
#include "common/protocol.hpp"
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    int node_id = 1;
    int port = 8081;

    if (argc > 1) node_id = std::stoi(argv[1]);
    if (argc > 2) port = std::stoi(argv[2]);

    kvstore::Store db;

    // Define the static topology for our 3-node local cluster
    std::vector<raft::PeerInfo> all_nodes = {
        {1, "127.0.0.1", 8081},
        {2, "127.0.0.1", 8082},
        {3, "127.0.0.1", 8083}
    };

    // Populate peers list with everyone except this node itself
    std::vector<raft::PeerInfo> peers;
    for (const auto& node : all_nodes) {
        if (node.id != node_id) {
            peers.push_back(node);
        }
    }

    // Initialize the Raft Node instance with its peers
    raft::RaftNode raft_node(node_id, peers, db);
    raft_node.start();

    network::Server server(4);

    server.setHandler([&db, &raft_node](int client_fd) {
        network::Socket client(client_fd);
        char buffer[2048] = {0};
        
        ssize_t bytes_rx = client.receiveData(buffer, sizeof(buffer) - 1);
        if (bytes_rx <= 0) return;

        std::string request(buffer);
        std::istringstream iss(request);
        std::string msg_type;
        iss >> msg_type;

        std::string response;

        if (msg_type == "REQ_VOTE") {
            auto args = common::Protocol::deserializeRequestVote(iss);
            auto reply = raft_node.handleRequestVote(args);
            response = common::Protocol::serializeRequestVoteReply(reply);
        } 
        else if (msg_type == "APPEND_ENTRIES") {
            auto args = common::Protocol::deserializeAppendEntries(iss);
            auto reply = raft_node.handleAppendEntries(args);
            response = common::Protocol::serializeAppendEntriesReply(reply);
        }
        else if (msg_type == "SET") {
            std::string key, val;
            iss >> key >> val;
            uint64_t index = 0;
            if (raft_node.propose("SET " + key + " " + val, index)) {
                response = "OK (Proposed at index " + std::to_string(index) + ")\n";
            } else {
                response = "ERROR: Not Leader\n";
            }
        } 
        else if (msg_type == "GET") {
            std::string key;
            iss >> key;
            auto val = db.get(key);
            response = val.has_value() ? val.value() + "\n" : "(nil)\n";
        } 
        else {
            response = "ERROR: Unknown RPC type\n";
        }

        client.sendData(response);
    });

    std::cout << "[Node " << node_id << "] Distributed KV & Raft Node booting on port " << port << "...\n";
    server.start(port);

    raft_node.stop();
    return 0;
}