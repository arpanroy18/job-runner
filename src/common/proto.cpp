#include "proto.hpp"

namespace jr {

static constexpr char FS = '\x1f';
static constexpr char RS = '\x1e';

static std::string join(const std::vector<std::string>& parts, char sep) {
    std::string out;
    for (size_t i = 0; i < parts.size(); i++) {
        if (i) out += sep;
        out += parts[i];
    }
    return out;
}

static std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    size_t start = 0;
    for (;;) {
        size_t pos = s.find(sep, start);
        if (pos == std::string::npos) {
            out.push_back(s.substr(start));
            return out;
        }
        out.push_back(s.substr(start, pos - start));
        start = pos + 1;
    }
}

std::string join_fields(const std::vector<std::string>& fields) { return join(fields, FS); }
std::vector<std::string> split_fields(const std::string& payload) {
    return payload.empty() ? std::vector<std::string>{} : split(payload, FS);
}

std::string join_list(const std::vector<std::string>& items) { return join(items, RS); }
std::vector<std::string> split_list(const std::string& s) {
    return s.empty() ? std::vector<std::string>{} : split(s, RS);
}

