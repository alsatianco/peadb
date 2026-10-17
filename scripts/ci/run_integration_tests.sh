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
#   3. On failure, captures the last 80 lines of stdout/stderr for quick
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
#   To make that survivable, this script tracks two things before its test
#   loop starts: a snapshot of the PIDs of any peadb-server processes already
#   running (e.g. a shared instance a caller such as `scripts/run_all_tests.sh`
#   deliberately keeps up on port 6379 for other stages), and this invocation's
#   own process group ID (own_pgid — a peadb-server spawned by one of this
#   script's tests inherits the script's PGID, verified directly: script
#   pid=22819 pgid=22813, server pid=22825 pgid=22813). After every test file
#   it looks for any *other* peadb-server process and, if one is found,
#   terminates it (SIGTERM, then SIGKILL after a bounded wait) — but only a
#   candidate that clears *both* guards is touched:
#     - it is not one of the PIDs in the start-of-run snapshot (so a
#       pre-existing shared server, such as the one on 6379, is never
#       touched), and
#     - its process group matches own_pgid (so a server started by a
#       *different*, concurrently running invocation of this same script —
#       two CI jobs sharing a runner, or a developer running the suite twice
#       — is never killed just because `pgrep -x peadb-server` also matches
#       it; that match is system-wide by process name alone and has no notion
#       of which invocation started the process).
#   Both guards are kept because they cover different things: the snapshot
#   guards a server that predates this run, the PGID guard scopes reaping to
#   this run's own descendants and so protects a sibling invocation's servers.
#   In the normal case (no stragglers) this is one cheap `pgrep` call and no
#   sleep at all, since this runs 120+ times per suite; the per-candidate PGID
#   check costs one extra `ps` call, paid only when there is actually a
#   candidate (normally there are none).
#
#   If `pgrep` itself is unavailable, straggler reaping (and the start-of-run
#   snapshot) is disabled for the whole run — a single warning is printed once
#   at start-up rather than the reaper silently no-op'ing on every test file.
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
#       <last 80 lines of test output>
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

# This invocation's own process group ID. A peadb-server spawned by one of
# this script's tests inherits the script's PGID (see "Straggler reaping"
# above), so reap_stragglers() uses this to recognize a candidate as this
# run's own descendant rather than a different, concurrently running
# invocation's live server.
own_pgid="$(ps -o pgid= -p $$ | tr -d ' ')"

# Straggler reaping (and its start-of-run snapshot, right below) depends on
# `pgrep`, which is common but not guaranteed to be installed. Check once,
# here at start-up, rather than inside reap_stragglers() — which runs once per
# test file — so a missing `pgrep` produces exactly one warning per run
# instead of the reaper silently no-op'ing 120+ times with no trace.
have_pgrep=1
if ! command -v pgrep >/dev/null 2>&1; then
  have_pgrep=0
  echo "WARNING: pgrep not found - straggler reaping is disabled for this run;" \
       "a peadb-server left behind by a timed-out test will not be cleaned up." >&2
fi

# PIDs of peadb-server processes already running before this script's test loop
# starts — e.g. a shared instance a caller keeps up on purpose (scripts/run_all_tests.sh
# expects one on 6379 for its later stages). reap_stragglers() below must never
# touch these; it only cleans up peadb-server processes that appeared *during*
# this script's own test loop and outlived the test that started them.
protected_server_pids=""
if [[ "$have_pgrep" -eq 1 ]]; then
  protected_server_pids=" $(pgrep -x peadb-server 2>/dev/null | tr '\n' ' ' || true) "
fi

# Reap any peadb-server process not in $protected_server_pids — i.e. one left
# behind by the test that just finished (see "Straggler reaping" above; the
# usual cause is a timed-out test whose `timeout`-killed python3 never reaped
# its own child). Cheap in the common case: one `pgrep`, no sleep, if nothing
# is left over. Only polls/sleeps when there is actually a straggler to wait
# for, and only for up to ~2s before escalating to SIGKILL.
reap_stragglers() {
  [[ "$have_pgrep" -eq 1 ]] || return 0
  local pid pgid
  local -a strays=()
  for pid in $(pgrep -x peadb-server 2>/dev/null || true); do
    [[ "$protected_server_pids" == *" $pid "* ]] && continue
    # A process can exit between pgrep and ps; that is successful cleanup.
    pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || true)"
    [[ "$pgid" == "$own_pgid" ]] || continue
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
      msg=$(tail -80 "$out_file" || true)
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
      msg=$(tail -80 "$out_file" || true)
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
