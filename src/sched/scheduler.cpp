#include "scheduler.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <algorithm>
#include <thread>
#include <unistd.h>
#include <errno.h>

#include "../common/proto.hpp"
#include "../common/util.hpp"

namespace jr {

static constexpr int64_t kHeartbeatTimeoutMs = 6000;
static constexpr size_t kMaxJobName = 40;

namespace {
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }
} // namespace

const char* state_name(JobState s) {
    switch (s) {
        case JobState::Queued:    return "queued";
        case JobState::Blocked:   return "blocked";
        case JobState::Running:   return "running";
        case JobState::Done:      return "done";
        case JobState::Failed:    return "failed";
        case JobState::Cancelled: return "cancelled";
    }
    return "?";
}

static bool is_terminal(JobState s) {
    return s == JobState::Done || s == JobState::Failed || s == JobState::Cancelled;
}

static int to_int(const std::string& s, int fallback = -1) {
    if (s.empty()) return fallback;
    char* end = nullptr;
    long v = strtol(s.c_str(), &end, 10);
    return (end && *end == 0) ? (int)v : fallback;
}

