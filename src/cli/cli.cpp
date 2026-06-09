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
