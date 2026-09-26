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

static int usage() {
    fputs(
        "usage: jr <command> [args]\n"
        "  schedd [--port N] [--state-dir DIR]     run the scheduler\n"
        "  worker [--addr H:P] [--cpus N] [--mem V] [--gpus N] [--name S]\n"
        "         [--label K=V]...                run a worker agent\n"
        "  submit [opts] <cmd> [args...]           (see jr submit -h)\n"
        "  list | workers | stats | top\n"
        "  info <job> | logs <job> [-f] | wait <job> | cancel <job>\n"
        "  drain <worker> | undrain <worker>       pause/resume new assignments\n"
        "  global: --addr host:port (or $JR_ADDR, default 127.0.0.1:7890)\n",
        stderr);
    return 2;
}

int main(int argc, char** argv) {
    std::vector<std::string> args(argv + 1, argv + argc);
    if (args.empty()) return usage();

    std::string addr_arg;
    for (size_t i = 0; i < args.size(); i++) {
        if (args[i] == "--addr" && i + 1 < args.size()) {
            addr_arg = args[i + 1];
            args.erase(args.begin() + i, args.begin() + i + 2);
            break;
        }
    }
    Addr addr = parse_addr(addr_arg);
    if (args.empty()) return usage();

    const std::string& cmd = args[0];
    std::vector<std::string> rest(args.begin() + 1, args.end());

    if (cmd == "schedd") {
        int port = addr.port;
        std::string dir = state_dir();
        for (size_t i = 0; i < rest.size(); i++) {
            if (rest[i] == "--port" && i + 1 < rest.size()) port = atoi(rest[++i].c_str());
            else if (rest[i] == "--state-dir" && i + 1 < rest.size()) dir = rest[++i];
            else return usage();
        }
        Scheduler s;
        return s.run(port, dir);
    }

    if (cmd == "worker") {
        WorkerOpts o;
        for (size_t i = 0; i < rest.size(); i++) {
            auto need = [&](const char* f) -> std::string {
                if (i + 1 >= rest.size()) { fprintf(stderr, "jr: %s needs a value\n", f); exit(2); }
                return rest[++i];
            };
            if (rest[i] == "--cpus") o.cpus = atoi(need("--cpus").c_str());
            else if (rest[i] == "--mem" || rest[i] == "--memory") {
                o.mem_mb = parse_mem_mb(need("--mem"));
                if (o.mem_mb < 0) { fprintf(stderr, "jr: bad --mem\n"); return 2; }
            }
            else if (rest[i] == "--gpus") o.gpus = atoi(need("--gpus").c_str());
            else if (rest[i] == "--gpu") o.gpus = 1;
            else if (rest[i] == "--name") o.name = need("--name");
            else if (rest[i] == "--label") o.labels.push_back(need("--label"));
            else return usage();
        }
        return run_worker(addr.host, addr.port, o, state_dir());
    }

    if (cmd == "submit")  return cli_submit(addr, rest);
    if (cmd == "list" || cmd == "ls") return cli_request(addr, (uint8_t)Msg::ListJobs);
    if (cmd == "workers") return cli_request(addr, (uint8_t)Msg::ListWorkers);
    if (cmd == "stats")   return cli_request(addr, (uint8_t)Msg::Stats);
    if (cmd == "top")     return cli_top(addr);
    if (cmd == "info" || cmd == "cancel") {
        if (rest.size() != 1) return usage();
        return cli_request(addr, cmd == "info" ? (uint8_t)Msg::JobInfo
                                               : (uint8_t)Msg::CancelJob, rest[0]);
    }
    if (cmd == "wait") {
        if (rest.size() != 1) return usage();
        return cli_wait(addr, rest[0]);
    }
    if (cmd == "logs") {
        if (rest.empty() || rest.size() > 2) return usage();
        bool follow = rest.size() == 2 && (rest[1] == "-f" || rest[1] == "--follow");
        if (rest.size() == 2 && !follow) return usage();
        return cli_logs(addr, rest[0], follow);
    }
    if (cmd == "drain" || cmd == "undrain") {
        if (rest.size() != 1) return usage();
        return cli_request(addr, (uint8_t)Msg::DrainWorker,
                           join_fields({rest[0], cmd == "drain" ? "1" : "0"}));
    }
    return usage();
}
