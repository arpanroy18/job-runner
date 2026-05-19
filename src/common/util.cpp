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

std::string fmt_age(int64_t ms) {
    if (ms < 0) ms = 0;
    int64_t s = ms / 1000;
    char buf[32];
    if (s < 60) snprintf(buf, sizeof(buf), "%llds", (long long)s);
    else if (s < 3600) snprintf(buf, sizeof(buf), "%lldm%02llds", (long long)(s / 60), (long long)(s % 60));
    else snprintf(buf, sizeof(buf), "%lldh%02lldm", (long long)(s / 3600), (long long)(s % 3600 / 60));
    return buf;
}

int parse_mem_mb(const std::string& s) {
    if (s.empty()) return -1;
    size_t i = 0;
    long long v = 0;
    while (i < s.size() && isdigit((unsigned char)s[i])) v = v * 10 + (s[i++] - '0');
    if (i == 0 || v <= 0) return -1;
    char suf = i < s.size() ? s[i++] : 'M';
    if (i != s.size() && (i != s.size() - 1 || s[i] != 'B')) return -1;
    switch (suf) {
        case 'K': case 'k': v /= 1024; break;
        case 'M': case 'm': break;
        case 'G': case 'g': v *= 1024; break;
        default: return -1;
    }
    if (v <= 0 || v > (1 << 20)) return -1; // at least 1 MB, at most 1 TB
    return (int)v;
}

