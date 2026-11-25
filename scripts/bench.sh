#!/usr/bin/env bash
# Compare minidis with a real Redis on the same machine.
set -euo pipefail
python -m minidis --port 6390 >/dev/null 2>&1 & M=$!
redis-server --port 6391 --save '' --appendonly no >/dev/null & R=$!
trap 'kill $M $R 2>/dev/null || true' EXIT
sleep 1
for port in 6390 6391; do
  [ $port = 6390 ] && echo "== minidis" || echo "== redis-server"
  redis-benchmark -p $port -t set,get,incr,lpush -n 50000 -c 20 -q
  echo "-- pipelined, 16 commands per round trip"
  redis-benchmark -p $port -t set,get -n 200000 -c 20 -P 16 -q
done
