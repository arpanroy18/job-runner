#include "net.hpp"

#include <cstring>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace jr {

static constexpr uint32_t kMaxFrame = 64 << 20;

Conn::~Conn() { close(); }

Conn::Conn(Conn&& o) noexcept : fd(o.fd), peer(std::move(o.peer)) { o.fd = -1; }

Conn& Conn::operator=(Conn&& o) noexcept {
    if (this != &o) {
        close();
        fd = o.fd;
        peer = std::move(o.peer);
        o.fd = -1;
    }
    return *this;
}

void Conn::close() {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

static bool write_all(int fd, const void* buf, size_t n) {
    const char* p = (const char*)buf;
    while (n > 0) {
        ssize_t r = ::send(fd, p, n, MSG_NOSIGNAL);
        if (r <= 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += r;
        n -= r;
    }
    return true;
}

static bool read_all(int fd, void* buf, size_t n) {
    char* p = (char*)buf;
    while (n > 0) {
        ssize_t r = ::recv(fd, p, n, 0);
        if (r <= 0) {
            if (r < 0 && errno == EINTR) continue;
            return false;
        }
        p += r;
        n -= r;
    }
    return true;
}

bool Conn::send_msg(uint8_t type, const std::string& payload) const {
    uint32_t len = htonl((uint32_t)payload.size() + 1);
    std::vector<char> head(5);
    memcpy(head.data(), &len, 4);
    head[4] = (char)type;
    return write_all(fd, head.data(), 5) &&
           (payload.empty() || write_all(fd, payload.data(), payload.size()));
}

bool Conn::recv_msg(uint8_t& type, std::string& payload) const {
    char head[5];
    if (!read_all(fd, head, 5)) return false;
    uint32_t len;
    memcpy(&len, head, 4);
    len = ntohl(len);
    if (len == 0 || len > kMaxFrame) return false;
    type = (uint8_t)head[4];
    payload.resize(len - 1);
    return payload.empty() || read_all(fd, payload.data(), payload.size());
}

int tcp_listen(int port) {
    int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd >= 0) {
        int one = 1, zero = 0;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero)); // dual-stack
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(port);
        if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) == 0 && ::listen(fd, 128) == 0)
            return fd;
        ::close(fd);
    }
    // IPv4-only fallback
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0 || ::listen(fd, 128) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

Conn tcp_accept(int listen_fd) {
    sockaddr_storage ss{};
    socklen_t slen = sizeof(ss);
    int fd = ::accept(listen_fd, (sockaddr*)&ss, &slen);
    if (fd < 0) return Conn{};
    char host[INET6_ADDRSTRLEN] = "?";
    uint16_t port = 0;
    if (ss.ss_family == AF_INET6) {
        auto* a = (sockaddr_in6*)&ss;
        inet_ntop(AF_INET6, &a->sin6_addr, host, sizeof(host));
        port = ntohs(a->sin6_port);
    } else if (ss.ss_family == AF_INET) {
        auto* a = (sockaddr_in*)&ss;
        inet_ntop(AF_INET, &a->sin_addr, host, sizeof(host));
        port = ntohs(a->sin_port);
    }
    return Conn(fd, std::string(host) + ":" + std::to_string(port));
}

Conn tcp_connect(const std::string& host, int port, int timeout_ms) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0)
        return Conn{};

    int fd = -1;
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (timeout_ms > 0) {
            timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        }
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) return Conn{};
    // blocking I/O after connect
    timeval tv{};
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)); // clear send timeout
    return Conn(fd, host + ":" + std::to_string(port));
}

} // namespace jr
