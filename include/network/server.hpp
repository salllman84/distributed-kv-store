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
    
    // <--- ADDED: File descriptor for the Linux epoll instance
    int epoll_fd_; 
    
    std::function<void(int)> connection_handler_;

public:
    explicit Server(size_t thread_count = 4);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void setHandler(std::function<void(int)> handler);
    void start(int port);
    void stop();
};

} // namespace network

#endif // SERVER_HPP