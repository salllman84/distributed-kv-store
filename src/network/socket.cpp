#include "network/socket.hpp"
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <iostream>
#include <cstring>

namespace network {

Socket::Socket() : is_closed_(false) {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        std::cerr << "Failed to create socket.\n";
        is_closed_ = true;
    }
}

Socket::Socket(int fd) : fd_(fd), is_closed_(false) {
    // Disable Nagle's algorithm for low-latency RPCs
    int flag = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));
}

Socket::~Socket() {
    closeSocket();
}

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_), is_closed_(other.is_closed_) {
    other.fd_ = -1;
    other.is_closed_ = true;
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        closeSocket();
        fd_ = other.fd_;
        is_closed_ = other.is_closed_;
        other.fd_ = -1;
        other.is_closed_ = true;
    }
    return *this;
}

bool Socket::bindAndListen(int port) {
    if (is_closed_) return false;

    int opt = 1;
    // Standard reuse address
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // <--- ADDED: Force port reuse for immediate restarts in WSL2/Linux
#ifdef SO_REUSEPORT
    setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed on port " << port << ".\n";
        return false;
    }

    if (listen(fd_, 128) < 0) {
        std::cerr << "Listen failed.\n";
        return false;
    }

    return true;
}

int Socket::acceptConnection() {
    if (is_closed_) return -1;
    
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    
    int client_fd = accept(fd_, (struct sockaddr*)&client_addr, &client_len);
    return client_fd;
}

bool Socket::connectTo(const std::string& ip, int port) {
    if (is_closed_) return false;

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid IP address: " << ip << "\n";
        return false;
    }

    if (connect(fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        return false;
    }

    // Disable Nagle's algorithm for outbound connections too
    int flag = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));

    return true;
}

ssize_t Socket::sendData(const std::string& data) {
    if (is_closed_) return -1;
    return send(fd_, data.c_str(), data.length(), 0);
}

ssize_t Socket::receiveData(char* buffer, size_t size) {
    if (is_closed_) return -1;
    return recv(fd_, buffer, size, 0);
}

void Socket::closeSocket() {
    if (!is_closed_ && fd_ >= 0) {
        close(fd_);
        is_closed_ = true;
    }
}

} // namespace network