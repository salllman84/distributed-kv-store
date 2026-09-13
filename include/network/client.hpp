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

    // Smart client wrapper for external commands (SET/GET) that automatically follows -MOVED redirects
    std::optional<std::string> executeCommand(const std::string& ip, int initial_port, const std::string& payload, int max_retries = 3) {
        int current_port = initial_port;

        for (int attempt = 0; attempt <= max_retries; ++attempt) {
            auto response = sendRpc(ip, current_port, payload);
            
            if (!response.has_value()) {
                return std::nullopt; // Network error or server down
            }

            std::string res = response.value();

            // C++20 string feature: check if the server rejected us with a redirect
            if (res.starts_with("-MOVED ")) {
                try {
                    // Extract the port number (skip the 7 characters of "-MOVED ")
                    current_port = std::stoi(res.substr(7));
                    // Loop restarts and tries the new port automatically
                    continue; 
                } catch (const std::exception&) {
                    return std::nullopt; // Invalid port format received
                }
            }

            // If it's a normal response (OK, ERROR, or a GET value), return it
            return res;
        }

        return std::nullopt; // Max redirect loops exceeded (protects against infinite routing loops)
    }
};

} // namespace network

#endif // CLIENT_HPP