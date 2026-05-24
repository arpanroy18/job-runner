#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "../common/net.hpp"
#include "journal.hpp"

namespace jr {

enum class JobState { Queued, Blocked, Running, Done, Failed, Cancelled };
const char* state_name(JobState s);

