#!/usr/bin/env bash
# Scripted demo: boots a local cluster (schedd + 2 workers) and runs a small
# job pipeline. Used to produce demo.gif via asciinema + agg:
#   asciinema rec -c "bash scripts/demo.sh" --cols 100 --rows 30 demo.cast
#   agg demo.cast demo.gif
set -u
cd "$(dirname "$0")/.."
export JR_ADDR=127.0.0.1:7899
export JR_STATE="$(mktemp -d)/jr"
JR=./build/jr

say() { printf '\n\033[1m$ %s\033[0m\n' "$1"; sleep 0.9; }
run() { say "$1"; eval "$1"; sleep 1; }
runq() { printf '\n\033[1m$ %s\033[0m\n' "$1"; eval "$1"; sleep 0.4; }

$JR schedd --port 7899 >/dev/null 2>&1 &
$JR worker --cpus 2 --mem 2G --name node-a --label zone=a >/dev/null 2>&1 &
$JR worker --cpus 4 --mem 4G --gpus 1 --name node-b --label zone=b --label gpu=v100 >/dev/null 2>&1 &
sleep 1.2

run "$JR workers"
runq "$JR submit --name download -- sh -c 'sleep 4; echo data ready'"
runq "$JR submit --name preprocess --after j-1 --cpus 2 -- sh -c 'echo cleaning; sleep 1'"
runq "$JR submit --name train --after j-2 --require gpu=v100 -- sh -c 'for e in 1 2 3; do echo epoch \$e; sleep 0.7; done'"
run "$JR list"
sleep 4
run "$JR logs j-3 -f"
run "$JR wait j-3"
run "$JR stats"

kill %1 %2 %3 2>/dev/null
printf '\ndone.\n'
sleep 1
