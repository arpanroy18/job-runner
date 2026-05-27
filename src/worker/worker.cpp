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

