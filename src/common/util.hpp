#pragma once

#include <cstdint>
#include <string>

namespace jr {

int64_t now_ms();
std::string hostname();

// "45000" -> "45s", "3720000" -> "1h02m". Negative -> "0s".
