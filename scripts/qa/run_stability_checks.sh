#!/usr/bin/env bash
#
# run_stability_checks.sh — ASAN/UBSAN stability loop for peadb-server.
#
# Purpose
#   Catches memory errors (use-after-free, buffer overflows, leaks) and
#   undefined behaviour by building peadb-server with AddressSanitizer +
#   UndefinedBehaviorSanitizer and then hammering it with a variety of
#   Redis commands.
#
# What it does
#   1. Runs CMake to produce a Debug build with ASAN+UBSAN flags in the
#      ``build-asan/`` directory.
#   2. Starts the sanitised peadb-server on port 6450 with
#      ``ASAN_OPTIONS=detect_leaks=1:halt_on_error=1``.
#   3. Enters a timed loop (default 15 seconds) where each iteration
#      spawns a Python snippet that opens a TCP connection and sends a
#      broad mix of commands: strings, lists, hashes, sets, sorted sets,
#      transactions (MULTI/EXEC), INFO, DUMP, and more.
#   4. After the loop, kills the server and scans ``/tmp/peadb-asan.log``
#      for sanitizer error markers (ERROR:, WARNING:.*Sanitizer,
#      LeakSanitizer).  If any are found, prints the last 30 lines of the
#      log and exits 1.
#
# How to run
#   From the repo root:
#
#       scripts/qa/run_stability_checks.sh
#       scripts/qa/run_stability_checks.sh --seconds 60
#       scripts/qa/run_stability_checks.sh --dry-run
#
# CLI options
#   --seconds N   Duration of the stress loop in seconds (default: 15).
#   --dry-run     Print the commands that would be run, then exit.
#   --help        Show embedded usage text.
#
# Prerequisites
#   - A C/C++ compiler that supports ``-fsanitize=address,undefined``.
#   - cmake, python3.
#   - Port 6450 must be free.
#
# Interpreting the output
#   Build progress is printed, followed by iteration count:
#       === Completed 42 iterations ===
#   Then the result:
#       PASS: stability checks completed (42 iterations, no sanitizer errors)
#       (exit code 0)
#   Or on failure:
#       FAIL: Sanitizer errors detected in /tmp/peadb-asan.log
#       <last 30 lines of the log>
#       (exit code 1)
#
# Exit codes
#   0   No sanitizer errors detected.
#   1   Sanitizer errors found, or build/start failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$ROOT_DIR"

NPROC=$(nproc 2>/dev/null || echo 4)

SECONDS_TO_RUN=15
DRY_RUN=0
PORT=6450

show_help() {
  cat <<'EOF'
Usage: scripts/qa/run_stability_checks.sh [--seconds N] [--dry-run]

Builds peadb-server with ASAN/UBSAN via cmake and runs a timed command loop
against it, checking for crashes, leaks, and undefined behaviour.

Options:
  --seconds N  Runtime for the stress loop (default: 15)
  --dry-run    Print commands without executing.
  --help       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --seconds" >&2
        exit 1
      fi
      SECONDS_TO_RUN="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --help) show_help; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] cmake -B build-asan -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer' ..."
  echo "[dry-run] cmake --build build-asan"
  echo "[dry-run] ./build-asan/peadb-server --port $PORT &"
  echo "[dry-run] stress loop for ${SECONDS_TO_RUN}s"
  echo "[dry-run] kill server, check exit code"
  exit 0
fi

echo "=== Building peadb-server with ASAN/UBSAN ==="
cmake -S "$ROOT_DIR" -B "$ROOT_DIR/build-asan" \
  -DCMAKE_CXX_FLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_C_FLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined" \
  -DCMAKE_BUILD_TYPE=Debug \
  >/dev/null 2>&1
cmake --build "$ROOT_DIR/build-asan" -j"$NPROC" 2>&1 | tail -3

echo "=== Starting ASAN server on port $PORT ==="
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  "$ROOT_DIR/build-asan/peadb-server" --port "$PORT" --bind 127.0.0.1 --loglevel error >/tmp/peadb-asan.log 2>&1 &
SERVER_PID=$!
trap 'kill ${SERVER_PID:-0} >/dev/null 2>&1 || true; wait ${SERVER_PID:-0} 2>/dev/null || true' EXIT
sleep 1

echo "=== Running stress loop for ${SECONDS_TO_RUN}s ==="
end=$(( $(date +%s) + SECONDS_TO_RUN ))
ITER=0
while [[ $(date +%s) -lt $end ]]; do
  python3 - "$PORT" <<'PY'
import socket, random, string
import sys

def cmd(sock, args):
    d = f"*{len(args)}\r\n".encode()
    for x in args:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(d)
    return sock.recv(4096)

PORT = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", PORT), timeout=2)
rk = ''.join(random.choices(string.ascii_lowercase, k=6))

cmd(s, ["SET", f"stab:{rk}", "v" * random.randint(1, 500)])
cmd(s, ["GET", f"stab:{rk}"])
cmd(s, ["INCR", "stab:counter"])
cmd(s, ["APPEND", f"stab:{rk}", "extra"])
cmd(s, ["STRLEN", f"stab:{rk}"])
cmd(s, ["GETRANGE", f"stab:{rk}", "0", "5"])

cmd(s, ["LPUSH", "stab:list", "a", "b", "c"])
cmd(s, ["RPUSH", "stab:list", "d", "e"])
cmd(s, ["LRANGE", "stab:list", "0", "-1"])
cmd(s, ["LPOP", "stab:list"])
cmd(s, ["RPOP", "stab:list"])
cmd(s, ["LLEN", "stab:list"])

cmd(s, ["HSET", "stab:hash", "f1", "v1", "f2", "v2"])
cmd(s, ["HGET", "stab:hash", "f1"])
cmd(s, ["HGETALL", "stab:hash"])
cmd(s, ["HDEL", "stab:hash", "f1"])

cmd(s, ["SADD", "stab:set", "m1", "m2", "m3"])
cmd(s, ["SMEMBERS", "stab:set"])
cmd(s, ["SCARD", "stab:set"])
cmd(s, ["SREM", "stab:set", "m1"])

cmd(s, ["ZADD", "stab:zset", "1", "a", "2", "b", "3", "c"])
cmd(s, ["ZRANGE", "stab:zset", "0", "-1", "WITHSCORES"])
cmd(s, ["ZSCORE", "stab:zset", "a"])
cmd(s, ["ZREM", "stab:zset", "a"])

cmd(s, ["TTL", f"stab:{rk}"])
cmd(s, ["EXPIRE", f"stab:{rk}", "60"])
cmd(s, ["TYPE", f"stab:{rk}"])
cmd(s, ["EXISTS", f"stab:{rk}"])
cmd(s, ["DEL", f"stab:{rk}"])

cmd(s, ["MULTI"])
cmd(s, ["SET", "stab:tx", "value"])
cmd(s, ["GET", "stab:tx"])
cmd(s, ["EXEC"])

cmd(s, ["PING"])
cmd(s, ["DBSIZE"])
cmd(s, ["INFO", "server"])
cmd(s, ["DUMP", "stab:counter"])

s.close()
PY
  ITER=$((ITER + 1))
done

echo "=== Completed $ITER iterations ==="

echo "=== Stopping ASAN server ==="
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
trap - EXIT

if grep -qi 'ERROR:\|WARNING:.*Sanitizer\|LeakSanitizer' /tmp/peadb-asan.log; then
  echo "FAIL: Sanitizer errors detected in /tmp/peadb-asan.log" >&2
  tail -30 /tmp/peadb-asan.log >&2
  exit 1
fi

echo "PASS: stability checks completed ($ITER iterations, no sanitizer errors)"
