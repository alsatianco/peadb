#!/usr/bin/env bash
#
# run_sentinel_validation.sh — Validate replication and promotion basics.
#
# Purpose
#   Verifies that PeaDB's replication subsystem works end-to-end: data
#   written to a primary propagates to a replica, and the replica can be
#   promoted to an independent master via REPLICAOF NO ONE.
#
# What it does
#   1. Starts a **primary** peadb-server on port 6470.
#   2. Starts a **replica** peadb-server on port 6471.
#   3. Configures the replica: ``REPLICAOF 127.0.0.1 6470``.
#   4. Writes test keys to the primary (strings + a list).
#   5. Disables the primary's diskless admission delay for this smoke and waits
#      up to 2 seconds for exact replicated values, then rechecks every assertion.
#   6. Promotes the replica: ``REPLICAOF NO ONE``.
#   7. Writes a new key on the (now-independent) promoted replica and
#      verifies it succeeds.
#   8. Checks ``INFO replication`` on the promoted replica to confirm its
#      role is ``master``.
#   9. Cleans up both server processes on exit (via trap).
#
# How to run
#   From the repo root:
#
#       scripts/qa/run_sentinel_validation.sh
#       scripts/qa/run_sentinel_validation.sh --dry-run   # print commands only
#
# CLI options
#   --dry-run   Print what would be executed without actually starting servers.
#   --help      Show embedded usage text.
#
# Environment variables
#   PEADB_BIN   Path to the peadb-server binary (default: ./peadb-server).
#
# Prerequisites
#   - ``python3`` and ``redis-cli`` in PATH (typically from the ``redis-tools`` package).
#   - A built ``peadb-server`` binary.
#   - Ports 6470 and 6471 must be free.
#
# Interpreting the output
#   Each step is printed with a ``===`` header.  On success:
#       PASS: Sentinel validation (replication + promotion) succeeded
#       (exit code 0)
#
#   On failure:
#       FAIL: <description of what went wrong>
#       (exit code 1)
#
# Exit codes
#   0   All checks passed.
#   1   A validation step failed, a prerequisite is missing, or an unknown
#       argument was provided.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

show_help() {
  cat <<'EOF'
Usage: scripts/qa/run_sentinel_validation.sh [--dry-run]

Validates replication and manual promotion flow with peadb-server instances.
Starts a primary, a replica with REPLICAOF, verifies data sync, then
promotes the replica via REPLICAOF NO ONE and confirms independence.

Requires:
  - redis-cli (for issuing commands)
  - peadb-server binary (from cmake build)

Options:
  --dry-run   Print the commands that would be executed.
  --help      Show this help.
EOF
}

PEADB_BIN="${PEADB_BIN:-./peadb-server}"
PRIMARY_PORT=6470
REPLICA_PORT=6471
DRY_RUN=0
PRIMARY_PID=""
REPLICA_PID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      show_help
      exit 0
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

VALIDATION_DIR=""

cleanup() {
  [[ -n "$PRIMARY_PID" ]] && kill "$PRIMARY_PID" 2>/dev/null || true
  [[ -n "$REPLICA_PID" ]] && kill "$REPLICA_PID" 2>/dev/null || true
  wait "$PRIMARY_PID" 2>/dev/null || true
  wait "$REPLICA_PID" 2>/dev/null || true
  [[ -z "$VALIDATION_DIR" ]] || rm -rf "$VALIDATION_DIR"
}
trap cleanup EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }

cli() {
  local port="$1"; shift
  redis-cli -h 127.0.0.1 -p "$port" "$@" 2>/dev/null
}

if [[ "$PEADB_BIN" != /* ]]; then
  PEADB_BIN="$ROOT_DIR/$PEADB_BIN"
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] Start primary on port $PRIMARY_PORT"
  echo "[dry-run] Start replica on port $REPLICA_PORT with REPLICAOF 127.0.0.1 $PRIMARY_PORT"
  echo "[dry-run] Write test keys on primary"
  echo "[dry-run] Wait for replication, verify keys on replica"
  echo "[dry-run] Promote replica via REPLICAOF NO ONE"
  echo "[dry-run] Verify writes on promoted replica"
  exit 0
fi

command -v redis-cli >/dev/null 2>&1 || fail "redis-cli not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
[[ -x "$PEADB_BIN" ]] || fail "peadb-server binary not found at $PEADB_BIN"

wait_ready() {
  python3 - "$ROOT_DIR" "$1" "$2" <<'PYREADY'
import sys
sys.path.insert(0, sys.argv[1] + '/tests/integration')
from _readiness import ObservedProcess, wait_until_ready
wait_until_ready(int(sys.argv[2]), process=ObservedProcess(int(sys.argv[3])))
PYREADY
}

VALIDATION_DIR="$(mktemp -d "${TMPDIR:-/tmp}/peadb-repl-validation.XXXXXX")"
mkdir "$VALIDATION_DIR/primary" "$VALIDATION_DIR/replica"

echo "=== Starting primary on port $PRIMARY_PORT ==="
"$PEADB_BIN" --port "$PRIMARY_PORT" --bind 127.0.0.1 --loglevel warning \
  --dir "$VALIDATION_DIR/primary" --save "" --repl-diskless-sync-delay 0 &
PRIMARY_PID=$!
wait_ready "$PRIMARY_PORT" "$PRIMARY_PID"

echo "=== Starting replica on port $REPLICA_PORT ==="
"$PEADB_BIN" --port "$REPLICA_PORT" --bind 127.0.0.1 --loglevel warning \
  --dir "$VALIDATION_DIR/replica" --save "" &
REPLICA_PID=$!
wait_ready "$REPLICA_PORT" "$REPLICA_PID"

echo "=== Configuring replica ==="
cli "$REPLICA_PORT" REPLICAOF 127.0.0.1 "$PRIMARY_PORT" | grep -qi ok || fail "REPLICAOF failed"

echo "=== Writing test data to primary ==="
cli "$PRIMARY_PORT" SET sentinel:key1 "hello" | grep -qi ok || fail "SET key1 failed"
cli "$PRIMARY_PORT" SET sentinel:key2 "world" | grep -qi ok || fail "SET key2 failed"
cli "$PRIMARY_PORT" LPUSH sentinel:list a b c >/dev/null || fail "LPUSH failed"

echo "=== Waiting for replication sync ==="
python3 - "$ROOT_DIR" "$REPLICA_PORT" "$REPLICA_PID" <<'PYSYNC'
import socket
import sys
import time
sys.path.insert(0, sys.argv[1] + '/tests/integration')
from _readiness import ObservedProcess, _command
process = ObservedProcess(int(sys.argv[3]))
deadline = time.monotonic() + 2
while time.monotonic() < deadline:
    if process.poll() is not None:
        raise RuntimeError('replica exited before synchronization')
    try:
        with socket.create_connection(('127.0.0.1', int(sys.argv[2])), timeout=0.2) as sock:
            with sock.makefile('rb') as stream:
                if (_command(stream, sock, 'GET', 'sentinel:key1') == b'hello' and
                    _command(stream, sock, 'GET', 'sentinel:key2') == b'world' and
                    _command(stream, sock, 'LLEN', 'sentinel:list') == b':3'):
                    break
    except (OSError, ValueError):
        pass
    time.sleep(max(0, min(0.05, deadline - time.monotonic())))
else:
    raise RuntimeError('replicated payloads did not match within 2 seconds')
PYSYNC

echo "=== Verifying data on replica ==="
V=$(cli "$REPLICA_PORT" GET sentinel:key1)
[[ "$V" == "hello" ]] || fail "key1 mismatch on replica: got '$V'"
V=$(cli "$REPLICA_PORT" GET sentinel:key2)
[[ "$V" == "world" ]] || fail "key2 mismatch on replica: got '$V'"
V=$(cli "$REPLICA_PORT" LLEN sentinel:list)
[[ "$V" == "3" ]] || fail "list length mismatch on replica: got '$V'"

echo "=== Promoting replica (REPLICAOF NO ONE) ==="
cli "$REPLICA_PORT" REPLICAOF NO ONE | grep -qi ok || fail "REPLICAOF NO ONE failed"

echo "=== Writing to promoted replica ==="
cli "$REPLICA_PORT" SET sentinel:promoted "yes" | grep -qi ok || fail "SET on promoted replica failed"
V=$(cli "$REPLICA_PORT" GET sentinel:promoted)
[[ "$V" == "yes" ]] || fail "promoted key mismatch: got '$V'"

echo "=== Verifying INFO replication on promoted replica ==="
ROLE=$(cli "$REPLICA_PORT" INFO replication | awk -F: '/^role:/{gsub("\r","",$2); print $2; exit}')
[[ "$ROLE" == "master" ]] || fail "promoted replica role is '$ROLE', expected 'master'"

echo "PASS: Sentinel validation (replication + promotion) succeeded"
