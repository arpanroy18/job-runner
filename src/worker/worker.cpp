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
