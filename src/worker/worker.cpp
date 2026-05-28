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

