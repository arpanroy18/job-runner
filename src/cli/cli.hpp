#pragma once

#include <string>
#include <vector>

namespace jr {

struct Addr {
    std::string host = "127.0.0.1";
    int port = 7890;
};

// Parses "host:port", "host", or ":port". Falls back to JR_ADDR env / defaults.
Addr parse_addr(const std::string& s);

int cli_submit(const Addr& a, std::vector<std::string> args);
int cli_request(const Addr& a, uint8_t type, const std::string& payload = {});
int cli_top(const Addr& a);
int cli_logs(const Addr& a, const std::string& job_id, bool follow);
int cli_wait(const Addr& a, const std::string& job_id);

} // namespace jr
