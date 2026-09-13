#include "network/server.hpp"
#include <iostream>
#include <sys/epoll.h> // <--- ADDED: Linux epoll API
#include <fcntl.h>     // <--- ADDED: File control for non-blocking sockets
#include <unistd.h>
#include <cstring>

namespace network {

Server::Server(size_t thread_count) 
    : thread_pool_(thread_count), is_running_(false), epoll_fd_(-1) {}

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

    // 1. Make listener socket non-blocking so it doesn't freeze the event loop
    int flags = fcntl(listener_.getFd(), F_GETFL, 0);
    fcntl(listener_.getFd(), F_SETFL, flags | O_NONBLOCK);

    // 2. Create the epoll instance
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == -1) {
        std::cerr << "[Server] epoll_create1 failed.\n";
        return;
    }

    // 3. Register the listener socket to monitor for incoming connections
    struct epoll_event event{};
    event.events = EPOLLIN; 
    event.data.fd = listener_.getFd();

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listener_.getFd(), &event) == -1) {
        std::cerr << "[Server] Failed to add listener to epoll.\n";
        return;
    }

    is_running_ = true;
    std::cout << "[Server] epoll multiplexer started on port " << port << "...\n" << std::flush;

    const int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];

    // 4. The Core Linux Event Loop
    while (is_running_) {
        // Wait up to 500ms for an event. Allows periodic checking of is_running_ flag
        int n_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 500);

        if (n_events == -1) {
            if (errno == EINTR) continue; // Interrupted by OS signal (like Ctrl+C), resume loop
            break; 
        }

        for (int i = 0; i < n_events; ++i) {
            if (events[i].data.fd == listener_.getFd()) {
                // EVENT: New client(s) connecting
                while (true) {
                    int client_fd = listener_.acceptConnection();
                    if (client_fd < 0) break; // Exhausted pending connections

                    // Add client to epoll. 
                    // EPOLLONESHOT prevents race conditions by ensuring only ONE thread processes this request.
                    struct epoll_event client_ev{};
                    client_ev.events = EPOLLIN | EPOLLONESHOT;
                    client_ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &client_ev);
                }
            } else {
                // EVENT: Client sent data and is ready to be read
                int client_fd = events[i].data.fd;
                
                if (connection_handler_) {
                    // Hand off to worker thread. The epoll loop immediately goes back to waiting!
                    thread_pool_.enqueue([this, client_fd]() {
                        this->connection_handler_(client_fd);
                        
                        // NOTE: connection_handler (in main.cpp) wraps client_fd in network::Socket,
                        // which automatically calls close() upon destruction. 
                        // The Linux kernel automatically removes closed FDs from the epoll watch list. No leaks!
                    });
                } else {
                    close(client_fd);
                }
            }
        }
    }

    if (epoll_fd_ != -1) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void Server::stop() {
    is_running_ = false;
    listener_.closeSocket(); 
}

} // namespace network