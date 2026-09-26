# jr — local distributed job runner

A small compute-cluster-in-a-binary: one scheduler, N workers, one CLI.

```
                 ┌────────────┐
   jr submit ──▶ │  scheduler │ ◀── heartbeats ──┐
     jr top  ──▶ │  (schedd)  │                  │
                 └─────┬──────┘                  │
              assign   │   assign                │
                 ┌─────▼─────┐   ┌─────────┐  ┌──┴──────┐
                 │ worker a  │   │ worker b │  │ worker … │
                 └───────────┘   └─────────┘  └─────────┘
```

Zero dependencies: C++20, POSIX sockets, threads. Builds with CMake or plain `make`.

## Build

```sh
cmake -B build && cmake --build build   # or: make
```

## Usage

```sh
# scheduler (journal lives in $JR_STATE or ~/.jr)
./jr schedd --port 7890 --state-dir ~/.jr

# workers — on this machine or any machine that can reach the scheduler
./jr worker --addr host:7890                      # auto-detect cpus/mem
./jr worker --addr host:7890 --cpus 8 --mem 16G --gpus 1 --name gpu-node

# submit
./jr submit -- ./train.py --epochs 10
./jr submit --gpus 1 --memory 8G --prio 10 --retries 2 -- ./train.py
./jr submit --env CUDA_VISIBLE_DEVICES=0 --cwd /data -- ./prep.sh

# inspect
./jr list          # job table
./jr workers       # worker table
./jr info j-3      # detail + captured output tail
./jr cancel j-3
./jr stats
./jr top           # live dashboard
```

Scheduler address: `--addr host:port` or `$JR_ADDR` (default `127.0.0.1:7890`).

## Design

**Wire protocol.** Length-prefixed TCP frames: `u32 len | u8 type | payload`.
Payloads are `\x1f`-separated fields; lists use `\x1e`. No serialization library.

**Scheduler** (`src/sched/`). Thread-per-connection; all state behind one mutex.
Dispatch is a sorted scan — queued jobs in (priority, submit-time) order get
best-fit placement onto workers (least free cpus that still fits). A job that
doesn't fit doesn't block smaller ones behind it.

- Heartbeats every 2 s; a worker silent for 6 s is marked lost and its tasks'
  jobs are requeued (subject to `--retries`).
- `queued → running → done|failed|cancelled`; a nonzero exit requeues while
  `runs <= max_retries`.
- Kill is a process-group SIGKILL (`setsid` on the child), so cancelled jobs
  take their whole subtree down.

**Persistence** (`src/sched/journal.*`). Append-only line journal —
`S` (submit), `T` (state transition incl. runs/exit/note), `W` (id watermark).
Replayed on startup; running jobs are requeued after a scheduler restart.
Compacted on clean shutdown and every ~5000 records.

**Worker** (`src/worker/`). Registers with capacity, receives Assign/Kill.
Each task is `fork`+`execvp` with stdout/stderr to `$JR_STATE/logs/<task>.log`;
the last 48 KB are reported back with the result for `jr info`. On shutdown or
connection loss the worker kills its children; the scheduler requeues the jobs.

## Scope / non-goals

Trusted-network protocol (no auth/encryption yet — use on localhost or a private
LAN). Dispatch is O(#queued × #workers), fine to ~10⁴ jobs. Output tail is
capped at 48 KB (full log stays on the worker's disk). No job packing constraints
beyond cpu/mem/gpu counts.
