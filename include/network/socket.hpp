#ifndef SOCKET_HPP
#define SOCKET_HPP

#include <string>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>

namespace network {

class Socket {
private:
    int fd_;
    bool is_closed_;

public:
    Socket();
    explicit Socket(int fd);
    ~Socket();

    // Disable copy semantics to prevent double-closing file descriptors
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    // Allow move semantics
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    // Server-side methods
    bool bindAndListen(int port);
    int acceptConnection();

    // Client-side methods
    bool connectTo(const std::string& ip, int port);

    // I/O operations
    ssize_t sendData(const std::string& data);
    ssize_t receiveData(char* buffer, size_t size);

    void closeSocket();
    int getFd() const { return fd_; }
};

} // namespace network

#endif // SOCKET_HPP