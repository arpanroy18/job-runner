#pragma once

#include <string>

namespace jr {

struct WorkerOpts {
    std::string name;   // default: hostname
    int cpus = 0;       // 0 = hardware_concurrency
    int mem_mb = 0;     // 0 = system total RAM
    int gpus = 0;
};

// Connects to the scheduler, registers, then serves Assign/Kill messages.
// Runs until the connection drops or a signal arrives. Returns exit code.
int run_worker(const std::string& host, int port, const WorkerOpts& opts,
               const std::string& state_dir);

} // namespace jr
