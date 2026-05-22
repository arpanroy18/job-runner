#include "net.hpp"

#include <cstring>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace jr {

static constexpr uint32_t kMaxFrame = 64 << 20;

