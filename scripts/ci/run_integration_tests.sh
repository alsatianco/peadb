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
#   4. After all tests, prints a summary:  PASSED: N / FAILED: M.
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
#   - A running `peadb-server` instance (most integration tests connect to
#     one).  See `scripts/dev/quick_start.sh` for easy setup.
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

passed=0
failed=0
fail_list=""

shopt -s nullglob
out_file="$(mktemp -t peadb_test_out.XXXXXX)"
trap 'rm -f "$out_file"' EXIT

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
done

printf "%b" "$fail_list"
echo "PASSED: $passed"
echo "FAILED: $failed"

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
