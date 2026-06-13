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
./jr worker --addr host:7890 --cpus 8 --mem 16G --gpus 1 --name gpu-node \
            --label gpu=v100 --label zone=us-east

# submit
./jr submit -- ./train.py --epochs 10
./jr submit --gpus 1 --memory 8G --prio 10 --retries 2 -- ./train.py
./jr submit --env CUDA_VISIBLE_DEVICES=0 --cwd /data -- ./prep.sh

# pipelines: dependencies + placement constraints + real limits
./jr submit --name download -- sh -c 'fetch data'
./jr submit --name train --after j-1 --require gpu=v100 --limit --mem 8G -- ./train.py

# inspect
./jr list          # job table
./jr workers       # worker table
./jr info j-3      # detail + captured output tail
./jr logs j-3      # last output
./jr logs j-3 -f   # stream live output (relay through the scheduler)
./jr wait j-3      # block until terminal; exit code mirrors the job's
./jr cancel j-3
./jr drain w-2     # stop new assignments on w-2 (undrain to resume)
./jr stats
./jr top           # live dashboard
```

Scheduler address: `--addr host:port` or `$JR_ADDR` (default `127.0.0.1:7890`).

## Design

**Wire protocol.** Length-prefixed TCP frames: `u32 len | u8 type | payload`.
