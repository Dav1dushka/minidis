#!/usr/bin/env bash
# Start minidis and talk to it with the real redis-cli.
set -euo pipefail
PORT=${PORT:-6399}
AOF=$(mktemp -d)/smoke.aof
python -m minidis --port "$PORT" --aof "$AOF" --fsync always &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
for _ in $(seq 50); do redis-cli -p "$PORT" ping >/dev/null 2>&1 && break; sleep 0.1; done

cli() { redis-cli -p "$PORT" "$@"; }
check() { local want=$1; shift; local got; got=$(cli "$@"); [ "$got" = "$want" ] || { echo "FAIL: $* -> '$got' (want '$want')"; exit 1; }; echo "ok   $*  ->  $got"; }

check PONG     ping
check OK       set greeting "hello world"
check "hello world" get greeting
check 42       incrby counter 42
check 3        rpush queue a b c
check a        lpop queue
check list     type queue
check 1        expire greeting 100
check 100      ttl greeting

# Restart and check that the AOF brought everything back.
kill $PID; wait $PID 2>/dev/null || true
python -m minidis --port "$PORT" --aof "$AOF" &
PID=$!
for _ in $(seq 50); do redis-cli -p "$PORT" ping >/dev/null 2>&1 && break; sleep 0.1; done
check "hello world" get greeting
check 42       get counter
check 2        llen queue
echo "smoke test passed"
