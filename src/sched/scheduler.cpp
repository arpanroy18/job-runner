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

void Scheduler::serve_conn(Conn c) {
    uint8_t type;
    std::string payload;
    if (!c.recv_msg(type, payload)) return;
    auto f = split_fields(payload);

    if ((Msg)type == Msg::Register) {
        on_register(std::move(c), f); // takes ownership, runs until worker drops
        return;
    }

    if ((Msg)type == Msg::SubLogs) {
        std::lock_guard<std::mutex> lk(mu_);
        on_sub_logs(std::move(c), f); // takes ownership until LogEnd
        return;
    }

    std::string reply;
    {
        std::lock_guard<std::mutex> lk(mu_);
        switch ((Msg)type) {
            case Msg::Submit:
                on_submit(c, f);
                return;
            case Msg::CancelJob:
                on_cancel(c, f);
                return;
            case Msg::ListJobs:    reply = job_table(); break;
            case Msg::ListWorkers: reply = worker_table(); break;
            case Msg::JobInfo:     reply = f.empty() ? "usage: info <job>" : job_info(f[0]); break;
            case Msg::Stats:       reply = stats(); break;
            case Msg::JobState: {
                auto it = f.empty() ? jobs_.end() : jobs_.find(f[0]);
                reply = it == jobs_.end()
                        ? "unknown"
                        : join_fields({state_name(it->second.state),
                                       std::to_string(it->second.exit_code)});
                break;
            }
            case Msg::DrainWorker: {
                if (f.size() < 2) { c.send_msg((uint8_t)Msg::Error, "usage: drain <worker> [0|1]"); return; }
                auto it = workers_.find(f[0]);
                if (it == workers_.end() || !it->second.alive) {
                    c.send_msg((uint8_t)Msg::Error, "no live worker: " + f[0]);
                    return;
                }
                it->second.draining = f[1] != "0";
                c.send_msg((uint8_t)Msg::Reply,
                           f[0] + (it->second.draining ? " draining\n" : " undrained\n"));
                dispatch();
                return;
            }
            default:
                c.send_msg((uint8_t)Msg::Error, "unknown request");
                return;
        }
    }
    c.send_msg((uint8_t)Msg::Reply, reply);
}

