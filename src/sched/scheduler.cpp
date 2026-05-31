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

// ---------------------------------------------------------------- main loop

int Scheduler::run(int port, const std::string& state_dir) {
    if (!journal_.open(state_dir + "/journal")) {
        log_line("sched", "cannot open journal in " + state_dir);
        return 1;
    }
    recover();

    int lfd = tcp_listen(port);
    if (lfd < 0) {
        log_line("sched", "cannot listen on port " + std::to_string(port) + ": " + strerror(errno));
        return 1;
    }

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    log_line("sched", "listening on :" + std::to_string(port) +
                      ", state dir " + state_dir);
    std::thread janitor(&Scheduler::janitor_loop, this);

    while (!g_stop) {
        Conn c = tcp_accept(lfd);
        if (!c) {
            if (errno == EINTR) continue;
            break;
        }
        std::thread([this, c = std::move(c)]() mutable { serve_conn(std::move(c)); }).detach();
    }

    stopping_ = true;
    ::close(lfd);
    janitor.join();
    {
        std::lock_guard<std::mutex> lk(mu_);
        maybe_compact(true);
    }
    log_line("sched", "stopped");
    return 0;
}

