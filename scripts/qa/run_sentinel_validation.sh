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
#   5. Waits 2 seconds for replication sync, then reads the keys back from
#      the replica and asserts they match.
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
#   PEADB_BIN   Path to the peadb-server binary (default: ./build/peadb-server).
#
# Prerequisites
#   - ``redis-cli`` in PATH (typically from the ``redis-tools`` package).
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

PEADB_BIN="${PEADB_BIN:-./build/peadb-server}"
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

cleanup() {
  [[ -n "$PRIMARY_PID" ]] && kill "$PRIMARY_PID" 2>/dev/null || true
  [[ -n "$REPLICA_PID" ]] && kill "$REPLICA_PID" 2>/dev/null || true
  wait "$PRIMARY_PID" 2>/dev/null || true
  wait "$REPLICA_PID" 2>/dev/null || true
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
[[ -x "$PEADB_BIN" ]] || fail "peadb-server binary not found at $PEADB_BIN"

echo "=== Starting primary on port $PRIMARY_PORT ==="
"$PEADB_BIN" --port "$PRIMARY_PORT" --bind 127.0.0.1 --loglevel error &
PRIMARY_PID=$!
sleep 1

echo "=== Starting replica on port $REPLICA_PORT ==="
"$PEADB_BIN" --port "$REPLICA_PORT" --bind 127.0.0.1 --loglevel error &
REPLICA_PID=$!
sleep 1

echo "=== Configuring replica ==="
cli "$REPLICA_PORT" REPLICAOF 127.0.0.1 "$PRIMARY_PORT" | grep -qi ok || fail "REPLICAOF failed"

echo "=== Writing test data to primary ==="
cli "$PRIMARY_PORT" SET sentinel:key1 "hello" | grep -qi ok || fail "SET key1 failed"
cli "$PRIMARY_PORT" SET sentinel:key2 "world" | grep -qi ok || fail "SET key2 failed"
cli "$PRIMARY_PORT" LPUSH sentinel:list a b c >/dev/null || fail "LPUSH failed"

echo "=== Waiting for replication sync ==="
sleep 2

echo "=== Verifying data on replica ==="
V=$(cli "$REPLICA_PORT" GET sentinel:key1)
[[ "$V" == "hello" ]] || fail "key1 mismatch on replica: got '$V'"
V=$(cli "$REPLICA_PORT" GET sentinel:key2)
[[ "$V" == "world" ]] || fail "key2 mismatch on replica: got '$V'"
V=$(cli "$REPLICA_PORT" LLEN sentinel:list)
[[ "$V" == "3" ]] || fail "list length mismatch on replica: got '$V'"

echo "=== Promoting replica (REPLICAOF NO ONE) ==="
cli "$REPLICA_PORT" REPLICAOF NO ONE | grep -qi ok || fail "REPLICAOF NO ONE failed"
sleep 1

echo "=== Writing to promoted replica ==="
cli "$REPLICA_PORT" SET sentinel:promoted "yes" | grep -qi ok || fail "SET on promoted replica failed"
V=$(cli "$REPLICA_PORT" GET sentinel:promoted)
[[ "$V" == "yes" ]] || fail "promoted key mismatch: got '$V'"

echo "=== Verifying INFO replication on promoted replica ==="
ROLE=$(cli "$REPLICA_PORT" INFO replication | awk -F: '/^role:/{gsub("\r","",$2); print $2; exit}')
[[ "$ROLE" == "master" ]] || fail "promoted replica role is '$ROLE', expected 'master'"

echo "PASS: Sentinel validation (replication + promotion) succeeded"
