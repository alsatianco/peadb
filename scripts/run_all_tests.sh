#!/usr/bin/env bash
#
# run_all_tests.sh — Run all PeaDB tests in sequence.
#
# Purpose
#   One-command runner that executes every test suite in the project:
#   unit tests (CTest), integration tests, CI checks, QA scripts, and
#   upstream Redis compatibility tests.  Useful for local pre-push
#   validation.
#
# What it does
#   1. C++ unit tests          — via `ctest` in build/
#   2. Integration tests       — scripts/ci/run_integration_tests.sh
#   3. Command test coverage   — scripts/ci/check_command_test_coverage.py
#   4. Compat delta validation — scripts/ci/check_compat_delta.py
#   5. Fuzz sanity checks      — scripts/qa/run_fuzz_sanity.sh
#   6. Stability checks        — scripts/qa/run_stability_checks.sh
#   7. Perf baseline           — scripts/qa/run_perf_baseline.sh
#   8. Sentinel validation     — scripts/qa/run_sentinel_validation.sh
#   9. Redis Tcl tests         — scripts/redis/run_redis_tests.sh
#
# How to run
#   From the repo root, with a peadb-server already running on port 6379:
#
#       scripts/run_all_tests.sh
#
#   Skip specific stages by name (comma-separated):
#
#       SKIP=perf,redis scripts/run_all_tests.sh
#
# Prerequisites
#   - A running peadb-server instance (for integration / Redis tests).
#   - A successful build in build/ (for unit tests).
#   - python3, cmake, redis-cli in PATH.
#   - See individual scripts for their own prerequisites.
#
# Environment variables
#   SKIP   Comma-separated list of stage names to skip.  Valid names:
#          unit, integration, coverage, compat, fuzz, stability, perf,
#          sentinel, redis.
#
# Exit codes
#   0   All executed stages passed.
#   1   One or more stages failed.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

SKIP="${SKIP:-}"

BUILD_DIR="${BUILD_DIR:-}"
PEADB_BIN="${PEADB_BIN:-}"

resolve_build_dir() {
    if [[ -n "$BUILD_DIR" ]]; then
        if [[ -d "$BUILD_DIR" ]]; then
            printf "%s" "$BUILD_DIR"
            return 0
        fi
        return 1
    fi

    local candidates=("build" "build-asan" "build-debug" "build-release")
    local d

    # First pass: prefer a directory that actually has tests registered.
    for d in "${candidates[@]}"; do
        if [[ -f "$ROOT_DIR/$d/CTestTestfile.cmake" ]]; then
            printf "%s" "$ROOT_DIR/$d"
            return 0
        fi
    done

    # Second pass: fall back to any configured CMake build directory.
    for d in "${candidates[@]}"; do
        if [[ -f "$ROOT_DIR/$d/CMakeCache.txt" ]]; then
            printf "%s" "$ROOT_DIR/$d"
            return 0
        fi
    done
    return 1
}

resolve_peadb_bin() {
    if [[ -n "$PEADB_BIN" ]]; then
        if [[ "$PEADB_BIN" = /* ]]; then
            printf "%s" "$PEADB_BIN"
        else
            printf "%s" "$ROOT_DIR/$PEADB_BIN"
        fi
        return 0
    fi

    if [[ -x "$ROOT_DIR/peadb-server" ]]; then
        printf "%s" "$ROOT_DIR/peadb-server"
        return 0
    fi

    if [[ -n "$CTEST_DIR" && -x "$CTEST_DIR/peadb-server" ]]; then
        printf "%s" "$CTEST_DIR/peadb-server"
        return 0
    fi

    local candidates=(
        "$ROOT_DIR/build/peadb-server"
        "$ROOT_DIR/build-asan/peadb-server"
        "$ROOT_DIR/build-debug/peadb-server"
        "$ROOT_DIR/build-release/peadb-server"
    )
    local b
    for b in "${candidates[@]}"; do
        if [[ -x "$b" ]]; then
            printf "%s" "$b"
            return 0
        fi
    done

    return 1
}

CTEST_DIR=""
if CTEST_DIR="$(resolve_build_dir)"; then
    :
fi

RESOLVED_PEADB_BIN=""
if RESOLVED_PEADB_BIN="$(resolve_peadb_bin)"; then
    export PEADB_BIN="$RESOLVED_PEADB_BIN"
fi

ROOT_BIN_LINK_CREATED=0
if [[ ! -x "$ROOT_DIR/peadb-server" && -n "$RESOLVED_PEADB_BIN" && -x "$RESOLVED_PEADB_BIN" ]]; then
    ln -sf "$RESOLVED_PEADB_BIN" "$ROOT_DIR/peadb-server"
    ROOT_BIN_LINK_CREATED=1
fi

cleanup() {
    if [[ "$ROOT_BIN_LINK_CREATED" -eq 1 ]]; then
        rm -f "$ROOT_DIR/peadb-server"
    fi
}
trap cleanup EXIT

passed=0
failed=0
skipped=0
failed_stages=""

should_skip() {
    local name="$1"
    if [[ ",$SKIP," == *",$name,"* ]]; then
        return 0
    fi
    return 1
}

run_stage() {
    local name="$1"
    shift
    local description="$1"
    shift

    if should_skip "$name"; then
        echo ""
        echo "=== SKIP: $description ==="
        ((skipped++))
        return 0
    fi

    echo ""
    echo "=========================================="
    echo "=== $description"
    echo "=========================================="

    if "$@"; then
        echo "--- PASS: $description ---"
        ((passed++))
    else
        echo "--- FAIL: $description ---"
        ((failed++))
        failed_stages="$failed_stages  - $description"$'\n'
    fi
}

run_ctest_stage() {
    if [[ -z "$CTEST_DIR" ]]; then
        echo "No CMake build directory found. Set BUILD_DIR or create build/." >&2
        return 1
    fi
    (cd "$CTEST_DIR" && ctest --output-on-failure --no-tests=error)
}

# ── 1. C++ unit tests (CTest) ──────────────────────────────────────────
run_stage unit "C++ unit tests (ctest)" \
    run_ctest_stage

# ── 2. Integration tests ───────────────────────────────────────────────
run_stage integration "Integration tests" \
    scripts/ci/run_integration_tests.sh

# ── 3. Command test coverage check ─────────────────────────────────────
run_stage coverage "Command test coverage check" \
    python3 scripts/ci/check_command_test_coverage.py

# ── 4. Compat delta validation ──────────────────────────────────────────
run_stage compat "Compat delta validation" \
    python3 scripts/ci/check_compat_delta.py

# ── 5. Fuzz sanity checks ──────────────────────────────────────────────
run_stage fuzz "Fuzz sanity checks" \
    scripts/qa/run_fuzz_sanity.sh

# ── 6. ASAN/UBSAN stability checks ─────────────────────────────────────
run_stage stability "ASAN/UBSAN stability checks" \
    scripts/qa/run_stability_checks.sh --seconds 15

# ── 7. Performance baseline ────────────────────────────────────────────
run_stage perf "Performance baseline" \
    scripts/qa/run_perf_baseline.sh

# ── 8. Sentinel / replication validation ────────────────────────────────
run_stage sentinel "Sentinel / replication validation" \
    scripts/qa/run_sentinel_validation.sh

# ── 9. Upstream Redis Tcl tests ─────────────────────────────────────────
run_stage redis "Upstream Redis Tcl tests" \
    scripts/redis/run_redis_tests.sh

# ── Summary ─────────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo "=== SUMMARY"
echo "=========================================="
echo "  PASSED:  $passed"
echo "  FAILED:  $failed"
echo "  SKIPPED: $skipped"

if [[ $failed -gt 0 ]]; then
    echo ""
    echo "Failed stages:"
    echo -n "$failed_stages"
    exit 1
fi

echo ""
echo "All tests passed."
exit 0
