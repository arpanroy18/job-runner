#pragma once

#include <fstream>
#include <functional>
#include <string>
#include <vector>

// Append-only line journal. The scheduler writes one FS-separated record
// per line; on startup it replays the file to rebuild state. Compaction
// rewrites the file as the minimal set of records describing live state.

namespace jr {

class Journal {
public:
    bool open(const std::string& path); // creates parent dir + file if missing
    void append(const std::string& record);
    // Calls fn(record) for each stored line, oldest first.
    bool replay(const std::function<void(const std::string&)>& fn) const;
    // Atomically replaces the journal with the given records.
    bool rewrite(const std::vector<std::string>& records);
    int lines() const { return lines_; }
    const std::string& path() const { return path_; }

private:
    std::string path_;
    std::ofstream out_;
    int lines_ = 0;
};

} // namespace jr
