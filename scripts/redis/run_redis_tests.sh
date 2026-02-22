#!/usr/bin/env bash
#
# run_redis_tests.sh — Run selected upstream Redis Tcl tests against PeaDB.
#
# Purpose
#   Exercises PeaDB with the official Redis test suite (the Tcl-based
#   ``runtest`` harness) to verify protocol and command compatibility.  Only
#   a curated subset of suites is run by default — those that target
#   functionality PeaDB already implements.
#
# What it does
#   1. Downloads a Redis release tarball (default 7.2.5) into a cache
#      directory (``$PEADB_CACHE_DIR`` or ``~/.cache/peadb/``).
#   2. Builds Redis from source so that ``runtest`` and ``redis-cli`` are
#      available.
#   3. Optionally starts a local peadb-server (controlled by START_PEADB).
#   4. Runs each suite in the SUITES array via:
#          ./runtest --host HOST --port PORT --clients 1 --single SUITE --dont-clean --verbose
#   5. Captures per-suite output in ``$OUT_DIR/<suite>.log``.
#   6. On any failure, writes ``failed_suites.txt`` (one suite per line) and
#      ``repro_commands.txt`` (copy-pasteable commands) under ``$OUT_DIR``.
#
# Default suites
#   unit/keyspace, unit/type/string, unit/expire, unit/multi, unit/scripting
#
# How to run
#   From the repo root:
#
#       scripts/redis/run_redis_tests.sh              # default port 6389
#       scripts/redis/run_redis_tests.sh 6400         # custom port
#       scripts/redis/run_redis_tests.sh 6400 0.0.0.0 # custom host
#
#   To test against an already-running server (skip auto-start):
#
#       START_PEADB=0 scripts/redis/run_redis_tests.sh 6379
#
# Arguments (positional)
#   $1   PORT  (default: 6389)
#   $2   HOST  (default: 127.0.0.1)
#
# Environment variables
#   REDIS_VERSION     Redis version to fetch/build (default: 7.2.5).
#   PEADB_CACHE_DIR   Cache dir for downloaded Redis tarballs/builds
#                     (default: ~/.cache/peadb/).
#   REDIS_DIR         Override the full path to the Redis source dir
#                     (skip download/build if already present).
#   PEADB_BIN         Path to peadb-server binary (default: ./peadb-server).
#   START_PEADB       Set to "0" to skip starting peadb-server (test
#                     against an already-running instance).
#   OUT_DIR           Output directory for logs and artifacts
#                     (default: artifacts/redis-stage-a).
#   SUITE_TIMEOUT     Per-suite timeout in seconds (default: 120).
#                     Suites that exceed this are killed and marked
#                     TIMEOUT in the output.
#
# Prerequisites
#   - tar, make, tclsh (for building and running the Redis test harness).
#   - curl or wget (for downloading the Redis tarball).
#   - (If START_PEADB=1) a built peadb-server binary.
#
# Interpreting the output
#   Per-suite progress:
#       === Running unit/keyspace ===
#   On success:
#       Redis Stage-A suites passed
#       (exit code 0)
#   On failure:
#       FAILED unit/keyspace (log: artifacts/redis-stage-a/unit_keyspace.log)
#       Redis Stage-A suites failed: unit/keyspace unit/expire
#       Repro commands saved to artifacts/redis-stage-a/repro_commands.txt
#       (exit code 1)
#   The repro_commands.txt file contains ready-to-run commands to re-execute
#   individual failing suites for debugging.
#
# Exit codes
#   0   All selected suites passed.
#   1   One or more suites failed, or prerequisites are missing.
set -euo pipefail

PORT_ARG="${1:-}"
PORT="${1:-6389}"
HOST="${2:-127.0.0.1}"
REDIS_VERSION="${REDIS_VERSION:-7.2.5}"
PEADB_CACHE_DIR="${PEADB_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/peadb}"
REDIS_DIR="${REDIS_DIR:-$PEADB_CACHE_DIR/redis-$REDIS_VERSION}"
PEADB_BIN="${PEADB_BIN:-./peadb-server}"
START_PEADB="${START_PEADB:-1}"
PEADB_PID=""
OUT_DIR="${OUT_DIR:-artifacts/redis-stage-a}"
SUITE_TIMEOUT="${SUITE_TIMEOUT:-120}"
SUITES=(
  "unit/keyspace"
  "unit/type/string"
  "unit/expire"
  "unit/multi"
  "unit/scripting"
)

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"
if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$ROOT_DIR/$OUT_DIR"
fi

if [[ "$PEADB_BIN" != /* ]]; then
  PEADB_BIN="$ROOT_DIR/$PEADB_BIN"
fi

port_is_available() {
  local host="$1"
  local port="$2"
  python3 - "$host" "$port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind((host, port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

find_available_port() {
  local host="$1"
  local start="$2"
  local end="$3"
  local p
  for ((p=start; p<=end; p++)); do
    if port_is_available "$host" "$p"; then
      printf "%s" "$p"
      return 0
    fi
  done
  return 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

ensure_redis_harness() {
  need_cmd tar
  if command -v curl >/dev/null 2>&1; then
    FETCH=(curl -fsSL)
  elif command -v wget >/dev/null 2>&1; then
    FETCH=(wget -qO-)
  else
    echo "Need curl or wget to fetch Redis sources for tests." >&2
    exit 1
  fi
  need_cmd make
  need_cmd tclsh

  if [[ ! -d "$REDIS_DIR" ]]; then
    mkdir -p "$PEADB_CACHE_DIR"
    echo "=== Fetching Redis $REDIS_VERSION into $REDIS_DIR ==="
    tmpdir="$(mktemp -d)"
    "${FETCH[@]}" "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz" \
      | tar -xz -C "$tmpdir"
    mv "$tmpdir/redis-$REDIS_VERSION" "$REDIS_DIR"
    rm -rf "$tmpdir"
  fi

  if [[ ! -x "$REDIS_DIR/runtest" || ! -x "$REDIS_DIR/src/redis-cli" ]]; then
    echo "=== Building Redis (test harness prerequisites) ==="
    (cd "$REDIS_DIR" && make -j)
  fi
}

ensure_redis_harness

cleanup() {
  if [[ -n "$PEADB_PID" ]]; then
    kill "$PEADB_PID" >/dev/null 2>&1 || true
    wait "$PEADB_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$OUT_DIR"

if [[ "$START_PEADB" == "1" ]]; then
  if [[ ! -x "$PEADB_BIN" ]]; then
    echo "Missing PeaDB binary at $PEADB_BIN" >&2
    exit 1
  fi

  if ! port_is_available "$HOST" "$PORT"; then
    if [[ -z "$PORT_ARG" ]]; then
      if alt_port="$(find_available_port "$HOST" 6390 6499)"; then
        echo "Port $PORT is busy; using free port $alt_port for Redis Tcl tests"
        PORT="$alt_port"
      else
        echo "Port $PORT is busy and no free fallback port found in range 6390-6499" >&2
        exit 1
      fi
    else
      echo "Requested port $PORT is already in use on $HOST" >&2
      echo "Choose a different port, or run with START_PEADB=0 to target an existing server" >&2
      exit 1
    fi
  fi

  "$PEADB_BIN" --port "$PORT" --bind "$HOST" --loglevel error >"$OUT_DIR/peadb-start.log" 2>&1 &
  PEADB_PID="$!"
  if ! python3 - "$HOST" "$PORT" "$PEADB_PID" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
pid = int(sys.argv[3])

deadline = time.time() + 5.0
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            sys.exit(0)
    except OSError:
        pass
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8"):
            pass
    except OSError:
        sys.exit(1)
    time.sleep(0.05)
sys.exit(2)
PY
  then
    rc=$?
    if [[ "$rc" -eq 1 ]]; then
      echo "peadb-server exited before becoming ready (log: $OUT_DIR/peadb-start.log)" >&2
    else
      echo "peadb-server did not become ready on $HOST:$PORT (log: $OUT_DIR/peadb-start.log)" >&2
    fi
    exit 1
  fi
fi
FAILED=()
REPRO_FILE="$OUT_DIR/repro_commands.txt"
: > "$REPRO_FILE"

cd "$REDIS_DIR"
for suite in "${SUITES[@]}"; do
  log_name="${suite//\//_}.log"
  log_path="$OUT_DIR/$log_name"
  echo "=== Running $suite ==="
  rc=0
  timeout --signal=TERM --kill-after=10 "$SUITE_TIMEOUT" \
    ./runtest \
      --host "$HOST" \
      --port "$PORT" \
      --clients 1 \
      --single "$suite" \
      --dont-clean \
      --verbose >"$log_path" 2>&1 || rc=$?
  if [[ $rc -ne 0 ]]; then
    if [[ $rc -eq 124 ]]; then
      echo "TIMEOUT $suite after ${SUITE_TIMEOUT}s (log: $log_path)" >&2
    else
      echo "FAILED $suite (log: $log_path)" >&2
    fi
    FAILED+=("$suite")
    echo "cd $REDIS_DIR && ./runtest --host $HOST --port $PORT --clients 1 --single $suite --dont-clean --verbose" >> "$REPRO_FILE"
  fi
done

if [[ ${#FAILED[@]} -gt 0 ]]; then
  printf "%s\n" "${FAILED[@]}" > "$OUT_DIR/failed_suites.txt"
  echo "Redis Stage-A suites failed: ${FAILED[*]}" >&2
  echo "Repro commands saved to $OUT_DIR/repro_commands.txt" >&2
  exit 1
fi

echo "Redis Stage-A suites passed"
