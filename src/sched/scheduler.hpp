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

