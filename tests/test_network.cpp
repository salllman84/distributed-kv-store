#include "network/socket.hpp"
#include <iostream>
#include <thread>
#include <cassert>
#include <chrono>
#include <cstring>

void run_server() {
    network::Socket server;
    
    bool is_bound = server.bindAndListen(8080);
    assert(is_bound && "Server failed to bind and listen");
    std::cout << "[Server] Listening on port 8080...\n";

    int client_fd = server.acceptConnection();
    assert(client_fd >= 0 && "Server failed to accept connection");

    // Wrap the returned raw file descriptor in a new Socket object
    network::Socket client_conn(client_fd);
    std::cout << "[Server] Client connected!\n";

    char buffer[1024] = {0};
    ssize_t bytes_rx = client_conn.receiveData(buffer, sizeof(buffer));
    assert(bytes_rx > 0 && "Server failed to receive data");
    std::cout << "[Server] Received: " << buffer << "\n";

    // Send response back
    std::string response = "PONG";
    client_conn.sendData(response);
}

int main() {
    std::cout << "--- Starting Network Wrapper Tests ---\n";

    // 1. Launch the server in a background thread
    std::thread server_thread(run_server);

    // 2. Give the OS a tiny fraction of a second to bind the port
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 3. Run the client logic on the main thread
    network::Socket client;
    bool is_connected = client.connectTo("127.0.0.1", 8080);
    assert(is_connected && "Client failed to connect to server");
    std::cout << "[Client] Connected to server. Sending PING...\n";

    // 4. Send request
    client.sendData("PING");

    // 5. Receive response
    char buffer[1024] = {0};
    ssize_t bytes_rx = client.receiveData(buffer, sizeof(buffer));
    assert(bytes_rx > 0 && "Client failed to receive data");
    std::cout << "[Client] Received: " << buffer << "\n";

    // 6. Verify data integrity
    assert(std::string(buffer) == "PONG" && "Data mismatch!");

    // Clean up thread
    server_thread.join();
    std::cout << "--- All Network Tests Passed! Socket wrapper is ready. ---\n";
    return 0;
}