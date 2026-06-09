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

static void usage_submit() {
    fputs(
        "usage: jr submit [opts] <cmd> [args...]\n"
        "       jr submit [opts] -- <cmd> [args...]\n"
        "  --cpus N        cpus to reserve (default 1)\n"
        "  --mem|--memory V   MB, or K/M/G suffix (default 256M)\n"
        "  --gpu           shorthand for --gpus 1\n"
        "  --gpus N        gpus to reserve (default 0)\n"
        "  -p|--prio N     priority, higher first (default 0)\n"
        "  -r|--retries N  retries on failure/loss (default 0)\n"
        "  --after J[,J..] run only after these jobs finish done\n"
        "  --require K=V   only run on workers carrying this label (repeatable)\n"
        "  --limit         enforce --mem as a hard address-space limit\n"
        "  --name S        display name\n"
        "  --env K=V       extra env var (repeatable)\n"
        "  --cwd P         working directory on the worker\n"
        "  Flags may precede the command; everything after the command is its argv.\n",
        stderr);
}

