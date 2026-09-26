#pragma once

#include <cstdint>
#include <string>
#include <vector>

// Wire protocol: frames are [u32be payload_len | u8 type | payload].
// Payloads are fields joined by FS ('\x1f'); inner lists are joined by
// RS ('\x1e'). Field values must not contain either byte (checked at
// submit time; worker scrubs them from captured output).

namespace jr {

enum class Msg : uint8_t {
    // worker -> scheduler
    Register = 1,   // name, cpus, mem_mb, gpus
    Heartbeat,      // worker_id, RS-joined running task ids
    TaskResult,     // task_id, exit_code, output_tail

    // cli -> scheduler
    Submit = 16,    // name, prio, cpus, mem_mb, gpus, max_retries, cwd, env(RS), argv(RS)
    ListJobs,       // (empty)
    JobInfo,        // job_id
    CancelJob,      // job_id
    ListWorkers,    // (empty)
    Stats,          // (empty)

    // scheduler -> worker
    RegAck = 32,    // worker_id
    Assign,         // task_id, job_id, cwd, env(RS), argv(RS)
    Kill,           // task_id

    // scheduler -> cli
    Reply = 48,     // free-form text
    Error,          // error text
};

std::string join_fields(const std::vector<std::string>& fields);
std::vector<std::string> split_fields(const std::string& payload);

std::string join_list(const std::vector<std::string>& items);
std::vector<std::string> split_list(const std::string& s);

std::string sanitize(std::string s); // replaces FS/RS with spaces

} // namespace jr
