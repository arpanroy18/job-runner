#pragma once

#include <cstdint>
#include <string>

namespace jr {

int64_t now_ms();
std::string hostname();

// "45000" -> "45s", "3720000" -> "1h02m". Negative -> "0s".
std::string fmt_age(int64_t ms);

// "8G" -> 8192, "512M" -> 512, "64K" -> 0->rejected, "1024" -> 1024 (MB). -1 on error.
int parse_mem_mb(const std::string& s);

// True if s contains bytes reserved by the wire protocol.
bool has_reserved_chars(const std::string& s);

void log_line(const char* component, const std::string& msg);

} // namespace jr
