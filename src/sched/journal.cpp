#include "journal.hpp"

#include <cstdio>
#include <sys/stat.h>

namespace jr {

static void mkdir_p(const std::string& dir) {
    for (size_t i = 1; i < dir.size(); i++)
        if (dir[i] == '/') ::mkdir(dir.substr(0, i).c_str(), 0755);
    ::mkdir(dir.c_str(), 0755);
}

bool Journal::open(const std::string& path) {
    path_ = path;
    size_t slash = path.rfind('/');
    if (slash != std::string::npos) mkdir_p(path.substr(0, slash));
    out_.open(path, std::ios::app);
    return out_.good();
}

void Journal::append(const std::string& record) {
    out_ << record << '\n';
    out_.flush();
    lines_++;
}

bool Journal::replay(const std::function<void(const std::string&)>& fn) const {
    std::ifstream in(path_);
    if (!in.good()) return true; // empty history is fine
    std::string line;
    while (std::getline(in, line))
        if (!line.empty()) fn(line);
    return true;
}

