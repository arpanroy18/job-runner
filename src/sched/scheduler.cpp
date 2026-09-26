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
        case JobState::Running:   return "running";
        case JobState::Done:      return "done";
        case JobState::Failed:    return "failed";
        case JobState::Cancelled: return "cancelled";
    }
    return "?";
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

// Best fit: pick the fitting worker with the least free cpus, keeping big
// workers free for big jobs. Jobs are tried in (prio desc, age asc) order;
// a job that doesn't fit doesn't block smaller jobs behind it (backfill).
void Scheduler::dispatch() {
    std::vector<Job*> queued;
    for (auto& [_, j] : jobs_)
        if (j.state == JobState::Queued) queued.push_back(&j);
    std::sort(queued.begin(), queued.end(), [](const Job* a, const Job* b) {
        return a->prio != b->prio ? a->prio > b->prio : a->submit_ts < b->submit_ts;
    });

    for (Job* j : queued) {
        Worker* best = nullptr;
        for (auto& [_, w] : workers_) {
            int free_cpu = w.cpus - w.used_cpus;
            if (!w.alive || free_cpu < j->cpus ||
                w.mem_mb - w.used_mem < j->mem_mb ||
                w.gpus - w.used_gpus < j->gpus)
                continue;
            if (!best || free_cpu < best->cpus - best->used_cpus) best = &w;
        }
        if (!best) continue;

        Task t;
        t.id = "t-" + std::to_string(next_task_++);
        t.job_id = j->id;
        t.worker_id = best->id;
        std::string payload = join_fields({t.id, j->id, j->cwd, join_list(j->env), join_list(j->argv)});
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
        j->start_ts = now_ms();
        j->worker_id = best->id;
        j->task_id = t.id;
        journal_state(*j);
        log_line("sched", "job " + j->id + " -> " + best->id + " (run " +
                          std::to_string(j->runs) + ")");
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

void Scheduler::fail_or_retry(Job& j, const std::string& note) {
    j.note = note;
    if (j.runs <= j.max_retries) {
        j.state = JobState::Queued;
        journal_state(j);
        log_line("sched", "job " + j.id + " requeued (" + note + ")");
    } else {
        j.state = JobState::Failed;
        j.end_ts = now_ms();
        journal_state(j);
        log_line("sched", "job " + j.id + " failed (" + note + ")");
    }
    maybe_compact();
}

void Scheduler::on_task_result(const std::vector<std::string>& f) {
    if (f.size() < 2) return;
    auto it = tasks_.find(f[0]);
    if (it == tasks_.end()) return; // stale result (job cancelled / resubmitted)
    Task t = it->second;
    release_task(t);

    Job& j = jobs_.at(t.job_id);
    if (j.state != JobState::Running || j.task_id != t.id) return; // stale

    j.task_id.clear();
    j.exit_code = to_int(f[1], -1);
    if (f.size() > 2) j.output_tail = sanitize(f[2]);

    if (j.exit_code == 0) {
        j.state = JobState::Done;
        j.end_ts = now_ms();
        journal_state(j);
        log_line("sched", "job " + j.id + " done");
    } else {
        fail_or_retry(j, "exit " + std::to_string(j.exit_code));
    }
    maybe_compact();
    dispatch();
}

// --------------------------------------------------------------------- cli

void Scheduler::on_submit(Conn& c, const std::vector<std::string>& f) {
    if (f.size() < 9) {
        c.send_msg((uint8_t)Msg::Error, "bad submit");
        return;
    }
    Job j;
    j.id = "j-" + std::to_string(next_job_++);
    j.name = f[0].substr(0, kMaxJobName);
    j.prio = to_int(f[1], 0);
    j.cpus = to_int(f[2], 1);
    j.mem_mb = to_int(f[3], 256);
    j.gpus = to_int(f[4], 0);
    j.max_retries = to_int(f[5], 0);
    j.cwd = f[6];
    j.env = split_list(f[7]);
    j.argv = split_list(f[8]);
    j.submit_ts = now_ms();

    if (j.argv.empty() || j.argv[0].empty() || j.cpus < 1 || j.mem_mb < 1 || j.gpus < 0) {
        c.send_msg((uint8_t)Msg::Error, "invalid job spec");
        return;
    }
    std::string id = j.id;
    journal_submit(j);
    jobs_.emplace(id, std::move(j));
    c.send_msg((uint8_t)Msg::Reply, "submitted " + id + "\n");
    log_line("sched", "job " + id + " submitted");
    maybe_compact();
    dispatch();
}

void Scheduler::on_cancel(Conn& c, const std::vector<std::string>& f) {
    if (f.empty()) {
        c.send_msg((uint8_t)Msg::Error, "usage: cancel <job>");
        return;
    }
    auto it = jobs_.find(f[0]);
    if (it == jobs_.end()) {
        c.send_msg((uint8_t)Msg::Error, "no such job: " + f[0]);
        return;
    }
    Job& j = it->second;
    if (j.state == JobState::Done || j.state == JobState::Failed ||
        j.state == JobState::Cancelled) {
        c.send_msg((uint8_t)Msg::Reply, f[0] + " already " + state_name(j.state));
        return;
    }
    Worker* w = nullptr;
    std::string task = j.task_id;
    if (j.state == JobState::Running) {
        auto tit = tasks_.find(j.task_id);
        if (tit != tasks_.end()) {
            auto wit = workers_.find(tit->second.worker_id);
            if (wit != workers_.end()) w = &wit->second;
            release_task(tit->second);
        }
    }
    j.state = JobState::Cancelled;
    j.end_ts = now_ms();
    j.task_id.clear();
    journal_state(j);
    if (w) send_to_worker(*w, (uint8_t)Msg::Kill, task);
    c.send_msg((uint8_t)Msg::Reply, "cancelled " + f[0] + "\n");
    log_line("sched", "job " + f[0] + " cancelled");
    dispatch();
}

// ------------------------------------------------------------------ replies

std::string Scheduler::job_table() const {
    std::vector<const Job*> js;
    for (auto& [_, j] : jobs_) js.push_back(&j);
    std::sort(js.begin(), js.end(),
              [](const Job* a, const Job* b) { return a->submit_ts < b->submit_ts; });

    std::string out;
    char line[256];
    snprintf(line, sizeof(line), "%-9s %-10s %-7s %4s %4s %6s  %s\n",
             "JOB", "STATE", "WORKER", "PRIO", "RUNS", "AGE", "COMMAND");
    out += line;
    int64_t now = now_ms();
    for (const Job* j : js) {
        std::string cmd;
        for (const auto& a : j->argv) { if (!cmd.empty()) cmd += ' '; cmd += a; }
        if (cmd.size() > 60) cmd = cmd.substr(0, 57) + "...";
        int64_t end = j->end_ts ? j->end_ts : now;
        snprintf(line, sizeof(line), "%-9s %-10s %-7s %4d %4d %6s  %s\n",
                 j->id.c_str(), state_name(j->state),
                 j->worker_id.empty() ? "-" : j->worker_id.c_str(),
                 j->prio, j->runs, fmt_age(end - j->submit_ts).c_str(), cmd.c_str());
        out += line;
    }
    if (js.empty()) out += "(no jobs)\n";
    return out;
}

std::string Scheduler::worker_table() const {
    std::vector<const Worker*> ws;
    for (auto& [_, w] : workers_) ws.push_back(&w);
    std::sort(ws.begin(), ws.end(),
              [](const Worker* a, const Worker* b) { return a->id < b->id; });

    std::string out;
    char line[320];
    snprintf(line, sizeof(line), "%-7s %-14s %-21s %-4s %-9s %-15s %-5s %5s  %s\n",
             "WORKER", "NAME", "PEER", "UP", "CPU", "MEM(MB)", "GPU", "TASKS", "LAST-BEAT");
    out += line;
    int64_t now = now_ms();
    for (const Worker* w : ws) {
        char cpu[16], mem[24], gpu[16];
        snprintf(cpu, sizeof(cpu), "%d/%d", w->used_cpus, w->cpus);
        snprintf(mem, sizeof(mem), "%d/%d", w->used_mem, w->mem_mb);
        snprintf(gpu, sizeof(gpu), "%d/%d", w->used_gpus, w->gpus);
        snprintf(line, sizeof(line), "%-7s %-14s %-21s %-4s %-9s %-15s %-5s %5zu  %s\n",
                 w->id.c_str(), w->name.c_str(), w->peer.c_str(),
                 w->alive ? "yes" : "no", cpu, mem, gpu, w->tasks.size(),
                 fmt_age(now - w->last_seen).c_str());
        out += line;
    }
    if (ws.empty()) out += "(no workers)\n";
    return out;
}

std::string Scheduler::job_info(const std::string& id) const {
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return "no such job: " + id;
    const Job& j = it->second;
    int64_t now = now_ms();
    std::string out;
    char line[256];
    snprintf(line, sizeof(line), "%s  %-10s  prio %d\n", j.id.c_str(),
             state_name(j.state), j.prio);
    out += line;
    if (!j.name.empty()) out += "  name:      " + j.name + "\n";
    std::string cmd;
    for (const auto& a : j.argv) { if (!cmd.empty()) cmd += ' '; cmd += a; }
    out += "  command:   " + cmd + "\n";
    if (!j.cwd.empty()) out += "  cwd:       " + j.cwd + "\n";
    if (!j.env.empty()) out += "  env:       " + join_list(j.env) + "\n";
    snprintf(line, sizeof(line), "  request:   %d cpu, %d MB, %d gpu\n", j.cpus, j.mem_mb, j.gpus);
    out += line;
    snprintf(line, sizeof(line), "  runs:      %d (max_retries %d)\n", j.runs, j.max_retries);
    out += line;
    if (!j.worker_id.empty()) out += "  worker:    " + j.worker_id + "\n";
    out += "  submitted: " + fmt_age(now - j.submit_ts) + " ago\n";
    if (j.start_ts) out += "  started:   " + fmt_age(now - j.start_ts) + " ago\n";
    if (j.end_ts) out += "  finished:  " + fmt_age(j.end_ts - j.start_ts) + " runtime\n";
    if (j.exit_code >= 0)
        out += "  exit:      " + std::to_string(j.exit_code) + "\n";
    if (!j.note.empty()) out += "  note:      " + j.note + "\n";
    if (!j.output_tail.empty()) out += "--- output tail ---\n" + j.output_tail;
    return out;
}

std::string Scheduler::stats() const {
    int up = 0, down = 0, q = 0, r = 0, d = 0, fl = 0, cx = 0;
    for (auto& [_, w] : workers_) (w.alive ? up : down)++;
    for (auto& [_, j] : jobs_) {
        switch (j.state) {
            case JobState::Queued: q++; break;
            case JobState::Running: r++; break;
            case JobState::Done: d++; break;
            case JobState::Failed: fl++; break;
            case JobState::Cancelled: cx++; break;
        }
    }
    char buf[160];
    snprintf(buf, sizeof(buf),
             "workers: %d up, %d lost | jobs: %d queued, %d running, "
             "%d done, %d failed, %d cancelled\n",
             up, down, q, r, d, fl, cx);
    return buf;
}

// ------------------------------------------------------------------ journal

void Scheduler::journal_submit(const Job& j) {
    journal_.append(join_fields({
        "S", j.id, j.name, std::to_string(j.prio), std::to_string(j.cpus),
        std::to_string(j.mem_mb), std::to_string(j.gpus), std::to_string(j.max_retries),
        std::to_string(j.submit_ts), j.cwd, join_list(j.env), join_list(j.argv)}));
}

void Scheduler::journal_state(const Job& j) {
    journal_.append(join_fields({
        "T", j.id, state_name(j.state), std::to_string(now_ms()),
        std::to_string(j.exit_code), j.note, std::to_string(j.runs)}));
}

void Scheduler::recover() {
    journal_.replay([this](const std::string& line) {
        auto f = split_fields(line);
        if (f.empty()) return;
        if (f[0] == "S" && f.size() >= 12) {
            Job j;
            j.id = f[1];
            j.name = f[2];
            j.prio = to_int(f[3], 0);
            j.cpus = to_int(f[4], 1);
            j.mem_mb = to_int(f[5], 256);
            j.gpus = to_int(f[6], 0);
            j.max_retries = to_int(f[7], 0);
            j.submit_ts = strtoll(f[8].c_str(), nullptr, 10);
            j.cwd = f[9];
            j.env = split_list(f[10]);
            j.argv = split_list(f[11]);
            jobs_[j.id] = std::move(j);
        } else if (f[0] == "T" && f.size() >= 3) {
            auto it = jobs_.find(f[1]);
            if (it == jobs_.end()) return;
            Job& j = it->second;
            for (auto s : {JobState::Queued, JobState::Running, JobState::Done,
                           JobState::Failed, JobState::Cancelled})
                if (f[2] == state_name(s)) j.state = s;
            if (f.size() > 4) j.exit_code = to_int(f[4], -1);
            if (f.size() > 5) j.note = f[5];
            if (f.size() > 6) j.runs = to_int(f[6], 0);
        } else if (f[0] == "W" && f.size() >= 4) {
            next_job_ = strtoull(f[1].c_str(), nullptr, 10);
            next_task_ = strtoull(f[2].c_str(), nullptr, 10);
            next_worker_ = strtoull(f[3].c_str(), nullptr, 10);
        }
    });
    // Tasks in flight when the scheduler died are orphaned on the workers;
    // requeue those jobs — stale results are ignored when they arrive.
    for (auto& [_, j] : jobs_) {
        if (j.state == JobState::Running) {
            j.state = JobState::Queued;
            j.worker_id.clear();
            j.task_id.clear();
            j.note = "requeued after scheduler restart";
        }
    }
    if (!jobs_.empty())
        log_line("sched", "recovered " + std::to_string(jobs_.size()) + " jobs from journal");
}

void Scheduler::maybe_compact(bool force) {
    if (!force && journal_.lines() < 5000) return;
    std::vector<std::string> recs;
    for (auto& [_, j] : jobs_) {
        recs.push_back(join_fields({
            "S", j.id, j.name, std::to_string(j.prio), std::to_string(j.cpus),
            std::to_string(j.mem_mb), std::to_string(j.gpus), std::to_string(j.max_retries),
            std::to_string(j.submit_ts), j.cwd, join_list(j.env), join_list(j.argv)}));
        recs.push_back(join_fields({
            "T", j.id, state_name(j.state), std::to_string(j.end_ts),
            std::to_string(j.exit_code), j.note, std::to_string(j.runs)}));
    }
    recs.push_back(join_fields({"W", std::to_string(next_job_),
                                std::to_string(next_task_), std::to_string(next_worker_)}));
    journal_.rewrite(recs);
}

// ------------------------------------------------------------------ janitor

void Scheduler::janitor_loop() {
    while (!stopping_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = now_ms();
        for (auto& [_, w] : workers_)
            if (w.alive && now - w.last_seen > kHeartbeatTimeoutMs)
                kill_worker(w);
        dispatch();
    }
}

} // namespace jr
