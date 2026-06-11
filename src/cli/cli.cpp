#include "cli.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <chrono>
#include <unistd.h>

#include "../common/net.hpp"
#include "../common/proto.hpp"
#include "../common/util.hpp"

namespace jr {

Addr parse_addr(const std::string& s) {
    Addr a;
    const char* env = getenv("JR_ADDR");
    std::string v = !s.empty() ? s : (env ? env : "");
    if (v.empty()) return a;
    auto c = v.rfind(':');
    if (c == std::string::npos) {
        a.host = v;
        return a;
    }
    if (c > 0) a.host = v.substr(0, c);
    int p = atoi(v.c_str() + c + 1);
    if (p > 0) a.port = p;
    return a;
}

// One request -> one reply. Prints the reply body; returns exit code.
int cli_request(const Addr& a, uint8_t type, const std::string& payload) {
    Conn c = tcp_connect(a.host, a.port);
    if (!c) {
        fprintf(stderr, "jr: cannot reach scheduler at %s:%d\n", a.host.c_str(), a.port);
        return 1;
    }
    if (!c.send_msg(type, payload)) return 1;
    uint8_t rtype;
    std::string body;
    if (!c.recv_msg(rtype, body)) {
        fprintf(stderr, "jr: connection lost\n");
        return 1;
    }
    if ((Msg)rtype == Msg::Error) {
        fprintf(stderr, "jr: %s\n", body.c_str());
        return 1;
    }
    fputs(body.c_str(), stdout);
    return 0;
}

static void usage_submit() {
    fputs(
        "usage: jr submit [opts] <cmd> [args...]\n"
        "       jr submit [opts] -- <cmd> [args...]\n"
        "  --cpus N        cpus to reserve (default 1)\n"
        "  --mem|--memory V   MB, or K/M/G suffix (default 256M)\n"
        "  --gpu           shorthand for --gpus 1\n"
        "  --gpus N        gpus to reserve (default 0)\n"
        "  -p|--prio N     priority, higher first (default 0)\n"
        "  -r|--retries N  retries on failure/loss (default 0)\n"
        "  --after J[,J..] run only after these jobs finish done\n"
        "  --require K=V   only run on workers carrying this label (repeatable)\n"
        "  --limit         enforce --mem as a hard address-space limit\n"
        "  --name S        display name\n"
        "  --env K=V       extra env var (repeatable)\n"
        "  --cwd P         working directory on the worker\n"
        "  Flags may precede the command; everything after the command is its argv.\n",
        stderr);
}

int cli_submit(const Addr& a, std::vector<std::string> args) {
    std::string name, cwd, mem = "256M";
    std::vector<std::string> env, require, after;
    int cpus = 1, gpus = 0, prio = 0, retries = 0;
    bool limit = false;
    std::vector<std::string> argv;

    for (size_t i = 0; i < args.size(); i++) {
        const std::string& s = args[i];
        auto need = [&](const char* flag) -> std::string {
            if (i + 1 >= args.size()) {
                fprintf(stderr, "jr: %s needs a value\n", flag);
                exit(2);
            }
            return args[++i];
        };
        if (s == "--" )              { argv.assign(args.begin() + i + 1, args.end()); break; }
        else if (s == "--cpus")      cpus = atoi(need("--cpus").c_str());
        else if (s == "--mem" || s == "--memory") mem = need("--mem");
        else if (s == "--gpu")       gpus = 1;
        else if (s == "--gpus")      gpus = atoi(need("--gpus").c_str());
        else if (s == "-p" || s == "--prio") prio = atoi(need("--prio").c_str());
        else if (s == "-r" || s == "--retries") retries = atoi(need("--retries").c_str());
        else if (s == "--name")      name = need("--name");
        else if (s == "--env")       env.push_back(need("--env"));
        else if (s == "--cwd")       cwd = need("--cwd");
        else if (s == "--require")   require.push_back(need("--require"));
        else if (s == "--limit")     limit = true;
        else if (s == "--after") {
            std::string v = need("--after");
            size_t st = 0;
            for (;;) {
                size_t comma = v.find(',', st);
                after.push_back(v.substr(st, comma == std::string::npos ? comma : comma - st));
                if (comma == std::string::npos) break;
                st = comma + 1;
            }
        }
        else if (s == "-h" || s == "--help") { usage_submit(); return 2; }
        else if (s.size() && s[0] == '-') {
            fprintf(stderr, "jr: unknown flag %s (put it after -- if it's the command's)\n", s.c_str());
            return 2;
        } else {
            argv.assign(args.begin() + i, args.end()); // first bare token starts the command
            break;
        }
    }

    if (argv.empty()) {
        usage_submit();
        return 2;
    }
    int mem_mb = parse_mem_mb(mem);
    if (mem_mb < 0 || cpus < 1 || gpus < 0) {
        fprintf(stderr, "jr: bad resource request\n");
        return 2;
    }
    for (const auto& s : {cwd, name})
        if (has_reserved_chars(s)) { fprintf(stderr, "jr: reserved character in argument\n"); return 2; }
    for (const auto& v : argv)
        if (has_reserved_chars(v)) { fprintf(stderr, "jr: reserved character in argv\n"); return 2; }
    for (const auto& v : env)
        if (has_reserved_chars(v) || v.find('=') == std::string::npos) {
            fprintf(stderr, "jr: bad --env %s\n", v.c_str());
            return 2;
        }

    for (const auto& v : require)
        if (has_reserved_chars(v) || v.find('=') == std::string::npos) {
            fprintf(stderr, "jr: bad --require %s (want K=V)\n", v.c_str());
            return 2;
        }

    std::string payload = join_fields({
        name, std::to_string(prio), std::to_string(cpus), std::to_string(mem_mb),
        std::to_string(gpus), std::to_string(retries), cwd,
        join_list(env), join_list(argv), join_list(after), join_list(require),
        limit ? "1" : "0"});
    return cli_request(a, (uint8_t)Msg::Submit, payload);
}

// Stream a job's output: prints the stored tail, then (with -f) live chunks
// relayed through the scheduler until the job terminates.
