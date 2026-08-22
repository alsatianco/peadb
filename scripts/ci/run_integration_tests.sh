#!/usr/bin/env bash
#
# run_integration_tests.sh — Run PeaDB Python integration tests with per-test timeouts.
#
# Purpose
#   Provides a simple, CI-friendly runner for all Python integration tests.
#   Each test file is executed as a standalone program (not via pytest), so
#   tests must be self-contained scripts that exit non-zero on failure.
#
# What it does
#   1. Discovers all files matching `tests/integration/test_*.py` under the
#      repo root.
#   2. Runs each test file via `python3 <file>`, optionally wrapped in a
#      per-test `timeout` to prevent hangs.
#   3. On failure, captures the last 3 lines of stdout/stderr for quick
#      triage and marks the test as FAIL (distinguishing timeouts from
#      other errors).
#   4. After each test file (pass, fail, or timeout), reaps any peadb-server
#      process left behind by that test before starting the next one — see
#      "Straggler reaping" below.
#   5. After all tests, prints a summary:  PASSED: N / FAILED: M.
#
# Straggler reaping
#   Integration tests share a fairly small port range, and each test is
#   responsible for stopping its own peadb-server subprocess(es) in a
#   `finally` block. That mostly works, but `timeout`/`gtimeout` only signals
#   its direct child (`python3 <file>`), not any grandchild peadb-server
#   subprocess that child spawned — so a test that times out can leave its
#   server running, still bound to its port, for the rest of the run. The
#   next test that happens to reuse that port then fails with a confusing
#   connection error instead of a real failure.
#
#   To make that survivable, this script snapshots the PIDs of any
#   peadb-server processes already running before its test loop starts (e.g.
#   a shared instance a caller such as `scripts/run_all_tests.sh` deliberately
#   keeps up on port 6379 for other stages) and never touches those. After
#   every test file it looks for any *other* peadb-server process and, if one
#   is found, terminates it (SIGTERM, then SIGKILL after a bounded wait). In
#   the normal case (no stragglers) this is one cheap `pgrep` call and no
#   sleep at all, since this runs 120+ times per suite.
#
# How it works
#   - Uses GNU coreutils `timeout` if available; on macOS it also checks
#     for `gtimeout` (from Homebrew coreutils).  If neither is found, tests
#     run without a timeout guard.
#   - Each test's combined stdout+stderr is written to a temp file, which
#     is automatically cleaned up on exit.
#
# How to run
#   From the repo root:
#
#       scripts/ci/run_integration_tests.sh
#
#   Override the per-test timeout (default 30 s):
#
#       TIMEOUT_SECS=60 scripts/ci/run_integration_tests.sh
#
# Prerequisites
#   - `python3` in PATH.
#   - A built `peadb-server` binary at the repo root (created by `cmake --build`).
#     Each integration test starts and stops its own `peadb-server` subprocess
#     on its own port; this runner does not own a shared server instance.
#   - (Optional) `timeout` or `gtimeout` for per-test timeout support.
#
# Configuration (env vars)
#   TIMEOUT_SECS   Per-test timeout in seconds (default: 30).  Applies only
#                  when `timeout`/`gtimeout` is available.
#
# Interpreting the output
#   Each failing test produces a block like:
#       FAIL: test_foo (exit 1)
#       <last 3 lines of test output>
#       ---
#   Followed by the aggregated counts:
#       PASSED: 12
#       FAILED: 2
#
#   A timeout failure shows:
#       FAIL: test_bar (timeout after 30s)
#
# Exit codes
#   0   All tests passed.
#   1   One or more tests failed, or no test files were found.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

TIMEOUT_SECS="${TIMEOUT_SECS:-30}"

TIMEOUT_CMD=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD="gtimeout"
fi

# Terminate a peadb-server PID gracefully; SIGKILL after 5 s if it lingers.
stop_server() {
  local pid="$1"
  [[ -z "$pid" ]] && return
  kill -TERM "$pid" 2>/dev/null || return
  local i
  for i in $(seq 1 10); do
    kill -0 "$pid" 2>/dev/null || return
    sleep 0.5
  done
  kill -KILL "$pid" 2>/dev/null || true
}

# PIDs of peadb-server processes already running before this script's test loop
# starts — e.g. a shared instance a caller keeps up on purpose (scripts/run_all_tests.sh
# expects one on 6379 for its later stages). reap_stragglers() below must never
# touch these; it only cleans up peadb-server processes that appeared *during*
# this script's own test loop and outlived the test that started them.
protected_server_pids=" $(pgrep -x peadb-server 2>/dev/null | tr '\n' ' ' || true) "

# Reap any peadb-server process not in $protected_server_pids — i.e. one left
# behind by the test that just finished (see "Straggler reaping" above; the
# usual cause is a timed-out test whose `timeout`-killed python3 never reaped
# its own child). Cheap in the common case: one `pgrep`, no sleep, if nothing
# is left over. Only polls/sleeps when there is actually a straggler to wait
# for, and only for up to ~2s before escalating to SIGKILL.
reap_stragglers() {
  local pid
  local -a strays=()
  for pid in $(pgrep -x peadb-server 2>/dev/null || true); do
    [[ "$protected_server_pids" == *" $pid "* ]] && continue
    strays+=("$pid")
  done
  [[ ${#strays[@]} -eq 0 ]] && return

  kill -TERM "${strays[@]}" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    local -a remaining=()
    for pid in "${strays[@]}"; do
      kill -0 "$pid" 2>/dev/null && remaining+=("$pid")
    done
    [[ ${#remaining[@]} -eq 0 ]] && return
    strays=("${remaining[@]}")
    sleep 0.1
  done
  kill -KILL "${strays[@]}" 2>/dev/null || true
}

passed=0
failed=0
fail_list=""

# Each integration test starts and stops its own peadb-server in a subprocess,
# so this runner has no shared server PID to track. Cleanup is just the temp
# output file; stop_server() remains available for tests that source this file.
cleanup() {
  rm -f "${out_file:-}"
}
trap cleanup EXIT

shopt -s nullglob
out_file="$(mktemp -t peadb_test_out.XXXXXX)"

cd "$ROOT_DIR"

tests=(tests/integration/test_*.py)
if [[ ${#tests[@]} -eq 0 ]]; then
  echo "No integration tests found under tests/integration/" >&2
  exit 1
fi

for t in "${tests[@]}"; do
  name=$(basename "$t" .py)
  if [[ -n "$TIMEOUT_CMD" ]]; then
    if "$TIMEOUT_CMD" "$TIMEOUT_SECS" python3 "$t" > "$out_file" 2>&1; then
      passed=$((passed + 1))
    else
      code=$?
      failed=$((failed + 1))
      msg=$(tail -3 "$out_file" || true)
      if [[ "$code" -eq 124 ]]; then
        fail_list="${fail_list}FAIL: ${name} (timeout after ${TIMEOUT_SECS}s)\n${msg}\n---\n"
      else
        fail_list="${fail_list}FAIL: ${name} (exit ${code})\n${msg}\n---\n"
      fi
    fi
  else
    if python3 "$t" > "$out_file" 2>&1; then
      passed=$((passed + 1))
    else
      code=$?
      failed=$((failed + 1))
      msg=$(tail -3 "$out_file" || true)
      fail_list="${fail_list}FAIL: ${name} (exit ${code})\n${msg}\n---\n"
    fi
  fi
  reap_stragglers
done

printf "%b" "$fail_list"
echo "PASSED: $passed"
echo "FAILED: $failed"

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
