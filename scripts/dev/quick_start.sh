#!/usr/bin/env bash
#
# quick_start.sh — Build and start peadb-server with sensible defaults.
#
# Purpose
#   One-command workflow to go from a clean checkout to a running PeaDB
#   instance.  Ideal for first-time setup and day-to-day development.
#
# What it does
#   1. Configures a Release CMake build (tests disabled) in build/.
#   2. Builds only the peadb-server target using all available CPU cores.
#   3. If redis-cli is available, attempts to shut down any existing server
#      already bound to the target port (SHUTDOWN NOSAVE; failure ignored).
#   4. exec's the freshly built binary with the resolved CLI flags, so this
#      script's process is replaced by peadb-server.
#
# How to run
#   From the repo root:
#
#       scripts/dev/quick_start.sh
#       scripts/dev/quick_start.sh --port 6380
#       scripts/dev/quick_start.sh --port 6380 --bind 0.0.0.0 --loglevel debug
#       scripts/dev/quick_start.sh --config my.conf -- --some-extra-flag
#
# CLI options
#   --port PORT        TCP port (default: 6379)
#   --bind ADDR        Bind address (default: 127.0.0.1, loopback-only)
#   --loglevel LEVEL   Log level (default: info)
#   --config PATH      Path to a PeaDB config file
#   --dir DIR          Persistence / working directory (default: ./data)
#   --                 Everything after -- is forwarded verbatim to the server
#   -h, --help         Show usage and exit
#
# Environment variables (override defaults before CLI parsing)
#   PEADB_PORT       Same as --port
#   PEADB_BIND       Same as --bind
#   PEADB_LOGLEVEL   Same as --loglevel
#
# Prerequisites
#   - cmake and a C++ compiler toolchain (see scripts/dev/install_deps_ubuntu.sh)
#   - (Optional) redis-cli, to auto-shutdown a stale server on the same port
#
# Interpreting the output
#   The script prints CMake configure/build progress, then the final exec
#   command line before replacing itself with peadb-server.  After that,
#   all output comes from peadb-server itself.
#
# Exit codes
#   The exit code is whatever peadb-server returns (the script exec's into it).
#   If the build fails, the script exits non-zero before reaching exec.
#
# Notes
#   The script is idempotent: re-running rebuilds only what changed and
#   (re)starts the server.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
DATA_DIR="$PROJECT_ROOT/data"

cd "$PROJECT_ROOT"

# ── Defaults ────────────────────────────────────────────────────────────────
PORT="${PEADB_PORT:-6379}"
BIND="${PEADB_BIND:-127.0.0.1}"
LOGLEVEL="${PEADB_LOGLEVEL:-info}"
CONFIG=""
EXTRA_ARGS=()

# ── Parse CLI overrides ────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port|--bind|--loglevel|--config|--dir)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing value for $1" >&2
        exit 1
      fi
      case "$1" in
        --port) PORT="$2" ;;
        --bind) BIND="$2" ;;
        --loglevel) LOGLEVEL="$2" ;;
        --config) CONFIG="$2" ;;
        --dir) DATA_DIR="$2" ;;
      esac
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--port PORT] [--bind ADDR] [--loglevel LEVEL] [--config PATH] [--dir DIR]"
      echo ""
      echo "Environment variables:  PEADB_PORT, PEADB_BIND, PEADB_LOGLEVEL"
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

mkdir -p "$DATA_DIR"

# Best-effort: raise soft nofile for high connection workloads.
TARGET_NOFILE="${PEADB_NOFILE_SOFT:-65536}"
CURRENT_NOFILE="$(ulimit -n 2>/dev/null || echo 0)"
if [[ "$CURRENT_NOFILE" =~ ^[0-9]+$ ]] && [[ "$TARGET_NOFILE" =~ ^[0-9]+$ ]]; then
  if (( CURRENT_NOFILE < TARGET_NOFILE )); then
    if ulimit -n "$TARGET_NOFILE" 2>/dev/null; then
      echo "==> Raised shell nofile soft limit to ${TARGET_NOFILE}"
    else
      echo "==> WARNING: could not raise nofile soft limit (current=${CURRENT_NOFILE}, target=${TARGET_NOFILE})"
      echo "            High-connection workloads may require: ulimit -n ${TARGET_NOFILE}"
    fi
  fi
fi

echo "==> Configuring (CMake)..."
cmake -S "$PROJECT_ROOT" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF

NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
echo "==> Building peadb-server (${NPROC} jobs)..."
cmake --build "$BUILD_DIR" --target peadb-server -j "$NPROC"

BINARY="$BUILD_DIR/peadb-server"
if [[ ! -x "$BINARY" ]]; then
  echo "ERROR: build succeeded but $BINARY not found" >&2
  exit 1
fi

if command -v redis-cli &>/dev/null; then
  SHUTDOWN_HOST="$BIND"
  if [[ "$BIND" == "0.0.0.0" ]]; then
    SHUTDOWN_HOST="127.0.0.1"
  elif [[ "$BIND" == "::" ]]; then
    SHUTDOWN_HOST="::1"
  fi
  redis-cli -h "$SHUTDOWN_HOST" -p "$PORT" SHUTDOWN NOSAVE 2>/dev/null || true
  sleep 0.3
fi

CMD=("$BINARY" --port "$PORT" --bind "$BIND" --loglevel "$LOGLEVEL" --dir "$DATA_DIR")
[[ -n "$CONFIG" ]] && CMD+=(--config "$CONFIG")
CMD+=("${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}")

echo "==> Starting peadb-server on ${BIND}:${PORT} ..."
echo "    ${CMD[*]}"
exec "${CMD[@]}"
