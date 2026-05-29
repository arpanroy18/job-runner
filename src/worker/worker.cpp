#include "worker.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <map>
#include <mutex>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/sysinfo.h>
#include <sys/resource.h>
#include <set>

#include "../common/net.hpp"
#include "../common/proto.hpp"
#include "../common/util.hpp"

namespace jr {

static constexpr int kHeartbeatMs = 2000;
static constexpr size_t kTailBytes = 48 << 10;

namespace {

// Shared worker state: the scheduler connection (all sends serialized by
// send_mu) and the set of running task process-groups.
struct Ctx {
    Conn conn;
    std::mutex send_mu;
    std::mutex run_mu;
    std::map<std::string, pid_t> running; // task_id -> child pgid
    std::set<std::string> active;         // tasks assigned and not yet reaped
    std::set<std::string> streaming;      // tasks with a live log streamer
    std::string id;
    std::string spool_dir;
    std::atomic<bool> alive{true};

    bool send(uint8_t type, const std::string& payload) {
        std::lock_guard<std::mutex> lk(send_mu);
        return conn.send_msg(type, payload);
    }
};

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

int total_mem_mb() {
    struct sysinfo si;
    if (sysinfo(&si) != 0) return 4096;
    return (int)(si.totalram * si.mem_unit / (1024 * 1024));
}

void mkdir_p(const std::string& dir) {
    for (size_t i = 1; i < dir.size(); i++)
        if (dir[i] == '/') ::mkdir(dir.substr(0, i).c_str(), 0755);
    ::mkdir(dir.c_str(), 0755);
}

std::string read_tail(const std::string& path, size_t max) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return {};
    off_t size = lseek(fd, 0, SEEK_END);
    off_t off = size > (off_t)max ? size - max : 0;
    std::string out;
    out.resize(size - off);
    lseek(fd, off, SEEK_SET);
    ssize_t n = ::read(fd, out.data(), out.size());
    ::close(fd);
    if (n < 0) return {};
    out.resize(n);
    return sanitize(std::move(out));
}

// Stream a task's log to the scheduler (which relays to `jr logs -f`
// subscribers). Reads the spool file as it grows; exits after a final flush
// once the task leaves the active set.
void stream_task(Ctx& ctx, const std::string& task_id) {
    std::string path = ctx.spool_dir + "/" + task_id + ".log";
    int fd = -1;
    off_t pos = 0;
    for (;;) {
        bool active;
        {
            std::lock_guard<std::mutex> lk(ctx.run_mu);
            active = ctx.active.count(task_id) > 0;
        }
        if (fd < 0) {
            fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0 && !active) return;
        }
        bool read_any = false;
        if (fd >= 0) {
            char buf[16384];
            lseek(fd, pos, SEEK_SET);
            ssize_t n;
            while ((n = ::read(fd, buf, sizeof(buf))) > 0) {
                pos += n;
                read_any = true;
                if (!ctx.send((uint8_t)Msg::LogData,
                              join_fields({task_id, sanitize(std::string(buf, n))})))
                    return;
            }
        }
        if (!active && !read_any) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void run_task(Ctx& ctx, const std::string& task_id, const std::string& cwd,
              const std::vector<std::string>& env,
              const std::vector<std::string>& argv, int mem_mb, bool limit) {
    std::string log_path = ctx.spool_dir + "/" + task_id + ".log";
    int logfd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

    pid_t pid = fork();
    if (pid == 0) {
        setsid(); // own process group -> Kill can take down the whole tree
        if (logfd >= 0) {
            dup2(logfd, STDOUT_FILENO);
            dup2(logfd, STDERR_FILENO);
        }
        if (limit && mem_mb > 0) {
            rlimit rl{(rlim_t)mem_mb << 20, (rlim_t)mem_mb << 20};
            setrlimit(RLIMIT_AS, &rl);
        }
        for (const auto& kv : env) {
            size_t eq = kv.find('=');
            if (eq != std::string::npos)
                setenv(kv.substr(0, eq).c_str(), kv.substr(eq + 1).c_str(), 1);
        }
        if (!cwd.empty() && ::chdir(cwd.c_str()) != 0)
            dprintf(STDERR_FILENO, "chdir(%s) failed\n", cwd.c_str());
        std::vector<char*> args;
        for (const auto& a : argv) args.push_back(const_cast<char*>(a.c_str()));
        args.push_back(nullptr);
        execvp(args[0], args.data());
        dprintf(STDERR_FILENO, "execvp(%s) failed\n", argv[0].c_str());
        _exit(127);
    }
    if (logfd >= 0) ::close(logfd);

    if (pid < 0) {
        ctx.send((uint8_t)Msg::TaskResult, join_fields({task_id, "127", "fork failed"}));
        return;
    }
    {
        std::lock_guard<std::mutex> lk(ctx.run_mu);
        ctx.running[task_id] = pid; // pid == pgid after setsid
    }

    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}

    {
        std::lock_guard<std::mutex> lk(ctx.run_mu);
        ctx.running.erase(task_id);
        ctx.active.erase(task_id);
    }

    int code = WIFEXITED(status) ? WEXITSTATUS(status)
             : WIFSIGNALED(status) ? 128 + WTERMSIG(status) : -1;
    ctx.send((uint8_t)Msg::TaskResult,
             join_fields({task_id, std::to_string(code), read_tail(log_path, kTailBytes)}));
}

void kill_task(Ctx& ctx, const std::string& task_id) {
    std::lock_guard<std::mutex> lk(ctx.run_mu);
    auto it = ctx.running.find(task_id);
    if (it != ctx.running.end()) ::kill(-it->second, SIGKILL);
}

void heartbeat_loop(Ctx& ctx) {
    while (ctx.alive) {
        std::vector<std::string> ids;
        {
            std::lock_guard<std::mutex> lk(ctx.run_mu);
            for (auto& [id, _] : ctx.running) ids.push_back(id);
        }
        if (!ctx.send((uint8_t)Msg::Heartbeat, join_fields({ctx.id, join_list(ids)})))
            return; // connection dead; main loop notices too
        for (int i = 0; i < 20 && ctx.alive && !g_stop; i++)
            std::this_thread::sleep_for(std::chrono::milliseconds(kHeartbeatMs / 20));
    }
}

} // namespace

int run_worker(const std::string& host, int port, const WorkerOpts& opts,
               const std::string& state_dir) {
    Ctx ctx;
    ctx.spool_dir = state_dir + "/logs";
    mkdir_p(ctx.spool_dir);

    ctx.conn = tcp_connect(host, port);
    if (!ctx.conn) {
        log_line("work", "cannot connect to " + host + ":" + std::to_string(port));
        return 1;
    }

    std::string name = opts.name.empty() ? hostname() : opts.name;
    int cpus = opts.cpus > 0 ? opts.cpus : (int)std::thread::hardware_concurrency();
    int mem = opts.mem_mb > 0 ? opts.mem_mb : total_mem_mb();

    if (!ctx.conn.send_msg((uint8_t)Msg::Register,
                           join_fields({name, std::to_string(cpus),
                                        std::to_string(mem), std::to_string(opts.gpus),
                                        join_list(opts.labels)})))
        return 1;
    uint8_t type;
    std::string payload;
    if (!ctx.conn.recv_msg(type, payload) || (Msg)type != Msg::RegAck) {
        log_line("work", "registration rejected");
        return 1;
    }
    ctx.id = payload;
    log_line("work", "registered as " + ctx.id + " (" + name + "): " +
                     std::to_string(cpus) + " cpu, " + std::to_string(mem) +
                     " MB, " + std::to_string(opts.gpus) + " gpu");

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    std::thread hb(heartbeat_loop, std::ref(ctx));

    while (ctx.alive && !g_stop) {
        if (!ctx.conn.recv_msg(type, payload)) break;
        auto f = split_fields(payload);
        if ((Msg)type == Msg::Assign && f.size() >= 5) {
            std::string tid = f[0], cwd = f[2];
            auto env = split_list(f[3]);
            auto argv = split_list(f[4]);
            int mem_mb = f.size() > 5 ? atoi(f[5].c_str()) : 0;
            bool limit = f.size() > 6 && f[6] == "1";
            if (!argv.empty()) {
                {
                    std::lock_guard<std::mutex> lk(ctx.run_mu);
                    ctx.active.insert(tid);
                }
                std::thread(run_task, std::ref(ctx), tid, cwd, env, argv, mem_mb, limit)
                    .detach();
            }
        } else if ((Msg)type == Msg::Kill && f.size() >= 1) {
            kill_task(ctx, f[0]);
        } else if ((Msg)type == Msg::LogSub && f.size() >= 1) {
            std::lock_guard<std::mutex> lk(ctx.run_mu);
            if (ctx.streaming.insert(f[0]).second)
                std::thread(stream_task, std::ref(ctx), f[0]).detach();
        }
    }

    // Shutdown: stop heartbeats, kill any children still running.
    ctx.alive = false;
    {
        std::lock_guard<std::mutex> lk(ctx.run_mu);
        for (auto& [_, pgid] : ctx.running) ::kill(-pgid, SIGKILL);
    }
    hb.join();
    log_line("work", ctx.id + " stopped");
    return 0;
}

} // namespace jr
