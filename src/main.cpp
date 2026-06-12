// jr — a small distributed job runner.
//
//   jr schedd      run the scheduler (accepts workers + cli)
//   jr worker      run a worker agent
//   jr submit      submit a job (deps, labels, limits, retries)
//   jr list        list jobs
//   jr info <job>  job detail + output tail
//   jr logs <job>  captured output tail (-f streams live)
//   jr wait <job>  block until the job terminates
//   jr cancel <job>
//   jr workers     list workers
//   jr drain <worker> [/undrain]  stop/start new assignments on a worker
//   jr stats       one-line status
//   jr top         live dashboard
//
// Scheduler address: --addr host:port or $JR_ADDR (default 127.0.0.1:7890).

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "cli/cli.hpp"
#include "common/proto.hpp"
#include "common/util.hpp"
#include "sched/scheduler.hpp"
#include "worker/worker.hpp"

using namespace jr;

static std::string state_dir() {
    const char* env = getenv("JR_STATE");
    if (env && *env) return env;
    const char* home = getenv("HOME");
    return std::string(home ? home : ".") + "/.jr";
}

