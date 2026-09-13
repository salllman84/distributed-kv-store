#ifndef SERVER_HPP
#define SERVER_HPP

#include "network/socket.hpp"
#include "common/thread_pool.hpp"
#include <functional>
#include <atomic>

namespace network {

class Server {
private:
    Socket listener_;
    common::ThreadPool thread_pool_;
    std::atomic<bool> is_running_;
    
    // Callback function to handle incoming connections.
    // It takes ownership of the client file descriptor (int).
    std::function<void(int)> connection_handler_;

public:
    // Initialize server with a default of 4 worker threads
    explicit Server(size_t thread_count = 4);
    ~Server();

    // Prevent copying
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // Inject the function that will process client messages
    void setHandler(std::function<void(int)> handler);

    // Start listening on a port and routing connections to the thread pool
    void start(int port);

    // Stop the server loop gracefully
    void stop();
};

} // namespace network

#endif // SERVER_HPP