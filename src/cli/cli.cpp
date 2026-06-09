#include "cli.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <chrono>
#include <unistd.h>

#include "../common/net.hpp"
#include "../common/proto.hpp"
#include "../common/util.hpp"

namespace jr {

Addr parse_addr(const std::string& s) {
    Addr a;
    const char* env = getenv("JR_ADDR");
    std::string v = !s.empty() ? s : (env ? env : "");
    if (v.empty()) return a;
    auto c = v.rfind(':');
    if (c == std::string::npos) {
        a.host = v;
        return a;
    }
    if (c > 0) a.host = v.substr(0, c);
    int p = atoi(v.c_str() + c + 1);
    if (p > 0) a.port = p;
    return a;
}

// One request -> one reply. Prints the reply body; returns exit code.
int cli_request(const Addr& a, uint8_t type, const std::string& payload) {
    Conn c = tcp_connect(a.host, a.port);
    if (!c) {
        fprintf(stderr, "jr: cannot reach scheduler at %s:%d\n", a.host.c_str(), a.port);
        return 1;
    }
    if (!c.send_msg(type, payload)) return 1;
    uint8_t rtype;
    std::string body;
    if (!c.recv_msg(rtype, body)) {
        fprintf(stderr, "jr: connection lost\n");
        return 1;
    }
    if ((Msg)rtype == Msg::Error) {
        fprintf(stderr, "jr: %s\n", body.c_str());
        return 1;
    }
    fputs(body.c_str(), stdout);
    return 0;
}

