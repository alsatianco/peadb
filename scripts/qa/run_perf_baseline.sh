#!/usr/bin/env bash
#
# run_perf_baseline.sh — Baseline performance benchmark for peadb-server.
#
# Purpose
#   Produces a repeatable throughput baseline for the six most common Redis
#   commands (SET, GET, LPUSH, LPOP, INCR, SADD).  Results are written as
#   CSV so they can be tracked across commits or compared against upstream
#   Redis.
#
# What it does
#   1. Starts a local peadb-server on port 6451 (hardcoded).
#   2. Waits for the server to accept connections.
#   3. Runs micro-benchmarks for each command:
#        - Prefers ``redis-benchmark`` (10 000 ops, 10 clients, quiet mode)
#          for more realistic pipelining behaviour.
#        - Falls back to a simple Python socket driver (5 000 sequential ops
#          per command) if ``redis-benchmark`` is not installed.
#   4. Writes results to a CSV file (``metric,value``) and prints them.
#   5. Stops the server.
#
# How to run
#   From the repo root:
#
#       scripts/qa/run_perf_baseline.sh
#       scripts/qa/run_perf_baseline.sh --output results/perf.csv
#       scripts/qa/run_perf_baseline.sh --dry-run
#
# CLI options
#   --output FILE   Path to the output CSV (default: artifacts/perf-baseline.csv).
#   --dry-run       Write a CSV with all-zero placeholder values and exit
#                   immediately (useful for CI plumbing tests).
#   --help          Show embedded usage text.
#
# Environment variables
#   PEADB_BIN   Path to the peadb-server binary (default: ./build/peadb-server).
#
# Prerequisites
#   - A built peadb-server binary.
#   - python3 (for the readiness probe and the fallback benchmark).
#   - (Optional) ``redis-benchmark`` for higher-quality throughput numbers.
#   - Port 6451 must be free.
#
# Interpreting the output
#   The CSV has one header row and six data rows:
#       metric,value
#       set_ops_per_sec,48231
#       get_ops_per_sec,51002
#       ...
#   Numbers are requests per second.  The Python fallback typically yields
#   lower numbers than ``redis-benchmark`` due to per-command round-trip
#   overhead (no pipelining).
#
# Exit codes
#   0   Benchmark completed and CSV written.
#   1   Server binary not found or server failed to start.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

OUT="artifacts/perf-baseline.csv"
DRY_RUN=0
PORT=6451
PEADB_BIN="${PEADB_BIN:-./build/peadb-server}"

show_help() {
  cat <<'EOF'
Usage: scripts/qa/run_perf_baseline.sh [--output FILE] [--dry-run]

Runs a baseline benchmark for peadb-server (SET/GET/LPUSH/LPOP/INCR/SADD)
and writes CSV output. Uses redis-benchmark if available, falls back to Python.

Options:
  --output FILE  Output CSV path (default: artifacts/perf-baseline.csv)
  --dry-run      Emit placeholder metrics without running benchmark traffic.
  --help         Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --output" >&2
        exit 1
      fi
      OUT="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --help) show_help; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT" != /* ]]; then
  OUT="$ROOT_DIR/$OUT"
fi
if [[ "$PEADB_BIN" != /* ]]; then
  PEADB_BIN="$ROOT_DIR/$PEADB_BIN"
fi

mkdir -p "$(dirname "$OUT")"
echo "metric,value" > "$OUT"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "set_ops_per_sec,0" >> "$OUT"
  echo "get_ops_per_sec,0" >> "$OUT"
  echo "lpush_ops_per_sec,0" >> "$OUT"
  echo "lpop_ops_per_sec,0" >> "$OUT"
  echo "incr_ops_per_sec,0" >> "$OUT"
  echo "sadd_ops_per_sec,0" >> "$OUT"
  exit 0
fi

[[ -x "$PEADB_BIN" ]] || { echo "Missing PeaDB binary at $PEADB_BIN. Build first." >&2; exit 1; }

echo "=== Starting peadb-server on port $PORT ==="
"$PEADB_BIN" --port "$PORT" --bind 127.0.0.1 --loglevel error >/tmp/peadb-perf.log 2>&1 &
SERVER_PID=$!
trap 'kill ${SERVER_PID:-0} >/dev/null 2>&1 || true; wait ${SERVER_PID:-0} 2>/dev/null || true' EXIT

python3 - "$PORT" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
end = time.time() + 5
while time.time() < end:
  try:
    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
      break
  except OSError:
    time.sleep(0.05)
else:
  raise SystemExit("server did not become ready")
PY

if command -v redis-benchmark >/dev/null 2>&1; then
  echo "=== Using redis-benchmark ==="
  for CMD in SET GET LPUSH LPOP INCR SADD; do
    RPS=$(redis-benchmark -h 127.0.0.1 -p "$PORT" -t "$(echo "$CMD" | tr '[:upper:]' '[:lower:]')" \
      -n 10000 -c 10 -q 2>/dev/null | tr '\r' '\n' | grep 'requests per second' | awk '{print $2}' | tail -n1 || true)
    RPS="${RPS:-0}"
    echo "${CMD,,}_ops_per_sec,${RPS}" >> "$OUT"
    echo "  $CMD: $RPS ops/sec"
  done
else
  echo "=== redis-benchmark not found, using Python benchmark ==="
  python3 - "$PORT" <<'PY' > /tmp/peadb-perf-values.txt
import socket, time
import sys

PORT = int(sys.argv[1])
N = 5000

def make_conn():
    return socket.create_connection(("127.0.0.1", PORT), timeout=5)

def cmd(sock, args):
    d = f"*{len(args)}\r\n".encode()
    for x in args:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(d)
    return sock.recv(4096)

def bench(op, n=N):
    s = make_conn()
    t0 = time.time()
    for i in range(n):
        if op == "set":
            cmd(s, ["SET", f"perf:{i}", "v" * 64])
        elif op == "get":
            cmd(s, ["GET", f"perf:{i}"])
        elif op == "lpush":
            cmd(s, ["LPUSH", "perf:list", f"v{i}"])
        elif op == "lpop":
            cmd(s, ["LPOP", "perf:list"])
        elif op == "incr":
            cmd(s, ["INCR", "perf:counter"])
        elif op == "sadd":
            cmd(s, ["SADD", "perf:set", f"m{i}"])
    dt = max(time.time() - t0, 1e-9)
    s.close()
    return int(n / dt)

for op in ["set", "get", "lpush", "lpop", "incr", "sadd"]:
    rps = bench(op)
    print(f"{op}_ops_per_sec,{rps}")
PY

  while IFS= read -r line; do
    echo "$line" >> "$OUT"
    echo "  $line"
  done < /tmp/peadb-perf-values.txt
fi

echo "=== Stopping server ==="
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
trap - EXIT

echo "=== Results written to $OUT ==="
cat "$OUT"
