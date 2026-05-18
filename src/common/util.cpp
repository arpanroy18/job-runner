#include "util.hpp"

#include <cstdio>
#include <chrono>
#include <unistd.h>

namespace jr {

int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

std::string hostname() {
    char buf[256];
    if (gethostname(buf, sizeof(buf)) != 0) return "?";
    buf[sizeof(buf) - 1] = 0;
    return buf;
}

