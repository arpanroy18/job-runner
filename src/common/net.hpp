#pragma once

#include <cstdint>
#include <string>

// Minimal framed-TCP helpers. A frame is:
//   [u32be frame_len | u8 type | payload]
// where frame_len = 1 + payload.size(). Max frame 64 MB.

namespace jr {

struct Conn {
    int fd = -1;
    std::string peer; // "ip:port" for diagnostics

    Conn() = default;
    explicit Conn(int fd, std::string peer = {}) : fd(fd), peer(std::move(peer)) {}
    ~Conn();
    Conn(Conn&& o) noexcept;
    Conn& operator=(Conn&& o) noexcept;
    Conn(const Conn&) = delete;
    Conn& operator=(const Conn&) = delete;

    bool send_msg(uint8_t type, const std::string& payload) const;
    // Blocks until a full frame arrives. Returns false on EOF/error.
    bool recv_msg(uint8_t& type, std::string& payload) const;
    void close();
    explicit operator bool() const { return fd >= 0; }
};

int tcp_listen(int port);                    // -1 on failure
Conn tcp_accept(int listen_fd);              // fd < 0 on failure
Conn tcp_connect(const std::string& host, int port, int timeout_ms = 3000);

} // namespace jr
