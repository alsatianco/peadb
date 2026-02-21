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

PORT="${1:-6389}"
HOST="${2:-127.0.0.1}"
REDIS_VERSION="${REDIS_VERSION:-7.2.5}"
PEADB_CACHE_DIR="${PEADB_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/peadb}"
REDIS_DIR="${REDIS_DIR:-$PEADB_CACHE_DIR/redis-$REDIS_VERSION}"
PEADB_BIN="${PEADB_BIN:-./peadb-server}"
START_PEADB="${START_PEADB:-1}"
PEADB_PID=""
OUT_DIR="${OUT_DIR:-artifacts/redis-stage-a}"
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

if [[ "$START_PEADB" == "1" ]]; then
  if [[ ! -x "$PEADB_BIN" ]]; then
    echo "Missing PeaDB binary at $PEADB_BIN" >&2
    exit 1
  fi
  "$PEADB_BIN" --port "$PORT" --bind "$HOST" --loglevel error &
  PEADB_PID="$!"
  sleep 1
fi

mkdir -p "$OUT_DIR"
FAILED=()
REPRO_FILE="$OUT_DIR/repro_commands.txt"
: > "$REPRO_FILE"

cd "$REDIS_DIR"
for suite in "${SUITES[@]}"; do
  log_name="${suite//\//_}.log"
  log_path="$OUT_DIR/$log_name"
  echo "=== Running $suite ==="
  if ! ./runtest \
      --host "$HOST" \
      --port "$PORT" \
      --clients 1 \
      --single "$suite" \
      --dont-clean \
      --verbose >"$log_path" 2>&1; then
    FAILED+=("$suite")
    echo "FAILED $suite (log: $log_path)" >&2
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
