#include "network/server.hpp"
#include <iostream>

namespace network {

Server::Server(size_t thread_count) 
    : thread_pool_(thread_count), is_running_(false) {}

Server::~Server() {
    stop();
}

void Server::setHandler(std::function<void(int)> handler) {
    connection_handler_ = std::move(handler);
}

void Server::start(int port) {
    if (!listener_.bindAndListen(port)) {
        std::cerr << "[Server] Failed to bind to port " << port << ".\n";
        return;
    }

    is_running_ = true;
    std::cout << "[Server] Started multi-threaded listener on port " << port << "...\n";

    // Main Accept Loop
    while (is_running_) {
        int client_fd = listener_.acceptConnection();
        
        if (client_fd < 0) {
            // If accept fails because stop() closed the socket, exit cleanly
            if (is_running_) {
                std::cerr << "[Server] Failed to accept connection.\n";
            }
            continue;
        }

        if (connection_handler_) {
            // Push the connection handling task to the thread pool
            thread_pool_.enqueue([this, client_fd]() {
                this->connection_handler_(client_fd);
            });
        } else {
            // If no handler is set, close it immediately to prevent descriptor leaks
            Socket temp_close(client_fd);
        }
    }
}

void Server::stop() {
    is_running_ = false;
    // Closing the listening socket interrupts the blocking accept() call
    listener_.closeSocket(); 
}

} // namespace network