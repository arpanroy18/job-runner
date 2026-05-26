#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "../common/net.hpp"
#include "journal.hpp"

namespace jr {

enum class JobState { Queued, Blocked, Running, Done, Failed, Cancelled };
const char* state_name(JobState s);

struct Job {
    std::string id, name, cwd;
    std::vector<std::string> argv, env;
    std::vector<std::string> after;   // job ids that must reach done first
    std::vector<std::string> require; // worker labels required ("k=v")
    int cpus = 1, mem_mb = 256, gpus = 0;
    int prio = 0, max_retries = 0;
    int runs = 0;
    bool limit = false;               // enforce mem_mb as RLIMIT_AS on the child
    JobState state = JobState::Queued;
    int64_t submit_ts = 0, start_ts = 0, end_ts = 0;
    int64_t ready_ts = 0;             // retry backoff: earliest dispatch time
    int exit_code = -1;
    std::string worker_id;
    std::string task_id;
    std::string output_tail;
    std::string note;
};

struct Worker {
    std::string id, name, peer;
    int cpus = 0, mem_mb = 0, gpus = 0;
    int used_cpus = 0, used_mem = 0, used_gpus = 0;
    std::set<std::string> labels;
    std::set<std::string> tasks;
    int64_t last_seen = 0;
    bool alive = true;
    bool draining = false;            // no new assignments
    Conn conn;
    std::mutex conn_mu; // serializes sends to this worker
};

// A CLI connection subscribed to a job's output stream.
struct Sub {
    Conn conn;
    std::mutex mu;
};

struct Task {
    std::string id, job_id, worker_id;
};

// Threading model: one thread per connection; all state is guarded by mu_.
// Dispatch is O(#queued * #workers) — fine for local/fleet scale (10^4 jobs).
