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

// ------------------------------------------------------------------ workers

void Scheduler::on_register(Conn c, const std::vector<std::string>& f) {
    if (f.size() < 4) {
        c.send_msg((uint8_t)Msg::Error, "bad register");
        return;
    }
    Worker* w;
    std::string wid;
    {
        std::lock_guard<std::mutex> lk(mu_);
        wid = "w-" + std::to_string(next_worker_++);
        w = &workers_.try_emplace(wid).first->second;
        w->id = wid;
        w->name = f[0].empty() ? wid : f[0];
        w->peer = c.peer;
        w->cpus = to_int(f[1], 1);
        w->mem_mb = to_int(f[2], 0);
        w->gpus = to_int(f[3], 0);
        if (f.size() > 4)
            for (const auto& l : split_list(f[4])) w->labels.insert(l);
        w->last_seen = now_ms();
        w->conn = std::move(c);
        if (!send_to_worker(*w, (uint8_t)Msg::RegAck, wid)) {
            workers_.erase(wid);
            return;
        }
        log_line("sched", "worker " + wid + " (" + w->name + ") up: " +
                          std::to_string(w->cpus) + " cpu, " +
                          std::to_string(w->mem_mb) + " MB, " +
                          std::to_string(w->gpus) + " gpu");
        dispatch();
    }

    for (;;) {
        uint8_t type;
        std::string payload;
        if (!w->conn.recv_msg(type, payload)) break;
        auto f = split_fields(payload);
        std::lock_guard<std::mutex> lk(mu_);
        if (!w->alive) break;
        w->last_seen = now_ms();
        if ((Msg)type == Msg::TaskResult) on_task_result(f);
        else if ((Msg)type == Msg::LogData) on_log_data(f);
        // Heartbeat: timestamp updated above; running-task ids in f[1] are
        // advisory (scheduler is authoritative).
    }

    std::lock_guard<std::mutex> lk(mu_);
    if (w->alive) kill_worker(*w);
    dispatch();
}

void Scheduler::kill_worker(Worker& w) {
    w.alive = false;
    w.conn.close();
    auto lost = std::move(w.tasks); // requeue each task's job
    w.used_cpus = w.used_mem = w.used_gpus = 0;
    log_line("sched", "worker " + w.id + " lost");
    for (const auto& tid : lost) {
        auto it = tasks_.find(tid);
        if (it == tasks_.end()) continue;
        Task t = it->second;
        tasks_.erase(it);
        Job& j = jobs_.at(t.job_id);
        j.worker_id.clear();
        j.task_id.clear();
        fail_or_retry(j, "worker lost");
    }
}

bool Scheduler::send_to_worker(Worker& w, uint8_t type, const std::string& payload) {
    std::lock_guard<std::mutex> lk(w.conn_mu);
    return w.conn.send_msg(type, payload);
}

// ------------------------------------------------------------------ dispatch

// Move blocked jobs forward when their dependencies resolve: all deps done
// -> queued; any dep failed/cancelled -> failed ("dep broken"). Job ids are
// minted at submit time and deps are immutable, so dependency cycles cannot
// be constructed.
void Scheduler::promote_blocked() {
    for (auto& [_, j] : jobs_) {
        if (j.state != JobState::Blocked) continue;
        bool bad_dep = false;
        for (const auto& d : j.after) {
            auto it = jobs_.find(d);
            if (it != jobs_.end() && it->second.state != JobState::Done) {
                if (it->second.state == JobState::Failed ||
                    it->second.state == JobState::Cancelled)
                    bad_dep = true;
            }
        }
        if (bad_dep) {
            j.note = "dependency failed";
            finish_job(j, JobState::Failed);
            log_line("sched", "job " + j.id + " failed (dependency failed)");
        } else {
            bool all_done = true;
            for (const auto& d : j.after) {
                auto it = jobs_.find(d);
                if (it == jobs_.end() || it->second.state != JobState::Done)
                    all_done = false;
            }
            if (all_done) {
                j.state = JobState::Queued;
                j.ready_ts = 0;
                journal_state(j);
                log_line("sched", "job " + j.id + " unblocked");
            }
        }
    }
}

// Best fit: pick the fitting worker with the least free cpus, keeping big
// workers free for big jobs. Jobs are tried in (prio desc, age asc) order;
// a job that doesn't fit doesn't block smaller jobs behind it (backfill).
void Scheduler::dispatch() {
    promote_blocked();

    std::vector<Job*> queued;
    int64_t now = now_ms();
    for (auto& [_, j] : jobs_)
        if (j.state == JobState::Queued && j.ready_ts <= now) queued.push_back(&j);
    std::sort(queued.begin(), queued.end(), [](const Job* a, const Job* b) {
        return a->prio != b->prio ? a->prio > b->prio : a->submit_ts < b->submit_ts;
    });

    for (Job* j : queued) {
        Worker* best = nullptr;
        for (auto& [_, w] : workers_) {
            int free_cpu = w.cpus - w.used_cpus;
            if (!w.alive || w.draining || free_cpu < j->cpus ||
                w.mem_mb - w.used_mem < j->mem_mb ||
                w.gpus - w.used_gpus < j->gpus)
                continue;
            bool labels_ok = true;
            for (const auto& r : j->require)
                if (!w.labels.count(r)) labels_ok = false;
            if (!labels_ok) continue;
            if (!best || free_cpu < best->cpus - best->used_cpus) best = &w;
        }
        if (!best) continue;

        Task t;
        t.id = "t-" + std::to_string(next_task_++);
        t.job_id = j->id;
        t.worker_id = best->id;
        std::string payload = join_fields({t.id, j->id, j->cwd, join_list(j->env),
                                           join_list(j->argv), std::to_string(j->mem_mb),
                                           j->limit ? "1" : "0"});
        if (!send_to_worker(*best, (uint8_t)Msg::Assign, payload)) {
            kill_worker(*best);
            continue;
        }
        best->used_cpus += j->cpus;
        best->used_mem += j->mem_mb;
        best->used_gpus += j->gpus;
        best->tasks.insert(t.id);
        tasks_.emplace(t.id, t);
        j->runs++;
        j->state = JobState::Running;
        j->start_ts = now;
        j->ready_ts = 0;
        j->worker_id = best->id;
        j->task_id = t.id;
        journal_state(*j);
        log_line("sched", "job " + j->id + " -> " + best->id + " (run " +
                          std::to_string(j->runs) + ")");
        auto it = subs_.find(j->id);
        if (it != subs_.end() && !it->second.empty())
            send_to_worker(*best, (uint8_t)Msg::LogSub, t.id);
    }
}

void Scheduler::release_task(const Task& t) {
    auto wit = workers_.find(t.worker_id);
    if (wit != workers_.end()) {
        Worker& w = wit->second;
        auto jit = jobs_.find(t.job_id);
        if (jit != jobs_.end()) {
            w.used_cpus -= jit->second.cpus;
            w.used_mem -= jit->second.mem_mb;
            w.used_gpus -= jit->second.gpus;
        }
        w.tasks.erase(t.id);
    }
    tasks_.erase(t.id);
}

// Terminal transition: stamp, journal, tell log subscribers we're done.
void Scheduler::finish_job(Job& j, JobState s) {
    j.state = s;
    j.end_ts = now_ms();
    j.task_id.clear();
    journal_state(j);
    end_subs(j.id);
    maybe_compact();
}

