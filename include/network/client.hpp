#ifndef CLIENT_HPP
#define CLIENT_HPP

#include "network/socket.hpp"
#include <string>
#include <optional>

namespace network {

class Client {
public:
    Client() = default;
    ~Client() = default;

    // Send a request string to a remote peer and return its response string
    std::optional<std::string> sendRpc(const std::string& ip, int port, const std::string& payload, int timeout_ms = 500) {
        Socket socket;
        if (!socket.connectTo(ip, port)) {
            return std::nullopt; // Connection failed
        }

        if (socket.sendData(payload) < 0) {
            return std::nullopt; // Send failed
        }

        char buffer[2048] = {0};
        ssize_t bytes_rx = socket.receiveData(buffer, sizeof(buffer) - 1);
        if (bytes_rx <= 0) {
            return std::nullopt; // Receive failed or peer closed connection
        }

        return std::string(buffer, bytes_rx);
    }
};

} // namespace network

#endif // CLIENT_HPP