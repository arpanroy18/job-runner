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

