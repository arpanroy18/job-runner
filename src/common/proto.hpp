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
    Register = 1,   // name, cpus, mem_mb, gpus, labels(RS)
    Heartbeat,      // worker_id, RS-joined running task ids
    TaskResult,     // task_id, exit_code, output_tail
    LogData,        // task_id, chunk

    // cli -> scheduler
    Submit = 16,    // name, prio, cpus, mem_mb, gpus, max_retries, cwd,
                    // env(RS), argv(RS), after(RS), require(RS), limit
    ListJobs,       // (empty)
    JobInfo,        // job_id
    CancelJob,      // job_id
    ListWorkers,    // (empty)
    Stats,          // (empty)
    JobState,       // job_id -> reply "state<FS>exit_code"
    DrainWorker,    // worker_id, "1" (drain) or "0" (undrain)
    SubLogs,        // job_id, follow("1"/"0")

    // scheduler -> worker
    RegAck = 32,    // worker_id
    Assign,         // task_id, job_id, cwd, env(RS), argv(RS), mem_mb, limit
    Kill,           // task_id
    LogSub,         // task_id (start streaming this task's log)

    // scheduler -> cli
    Reply = 48,     // free-form text
    Error,          // error text
    LogChunk = 51,  // raw output bytes
    LogEnd,         // stream finished
};

