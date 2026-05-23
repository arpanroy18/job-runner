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
    if (len == 0) return false;
    type = (uint8_t)head[4];
    payload.resize(len - 1);
    return payload.empty() || read_all(fd, payload.data(), payload.size());
}

