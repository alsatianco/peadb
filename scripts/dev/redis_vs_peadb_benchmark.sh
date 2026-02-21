#!/usr/bin/env bash
# ============================================================================
# redis_vs_peadb_benchmark.sh — Head-to-head benchmark: Redis vs PeaDB
#
# Starts Redis (Docker), runs a battery of benchmarks, then stops Redis,
# starts PeaDB (Docker), runs the same benchmarks, and prints a comparison.
#
# Usage:
#   scripts/dev/redis_vs_peadb_benchmark.sh [--port PORT] [--output-dir DIR]
#
# Requirements: docker, docker compose (plugin or standalone).
# The script will pull/install redis-benchmark and memtier_benchmark
# automatically if they are not found.
# ============================================================================
set -euo pipefail

# ── Tunables ─────────────────────────────────────────────────────────────────
PORT="${BENCH_PORT:-6379}"
OUTPUT_DIR=""
REDIS_IMAGE="redis:7-alpine"
COOLDOWN_SECS=3
WAIT_READY_TIMEOUT=30

# Container resource limits (set via env to pin, empty = no constraint)
BENCH_CPUSET="${BENCH_CPUSET:-}"        # e.g. "0-3"
BENCH_MEMORY="${BENCH_MEMORY:-}"        # e.g. "4g"
BENCH_NOFILE_SOFT="${BENCH_NOFILE_SOFT:-65536}"
BENCH_NOFILE_HARD="${BENCH_NOFILE_HARD:-65536}"

# Optional long-duration stability test (seconds, 0 = skip)
LONG_RUN_SECS="${BENCH_LONG_RUN:-0}"

# redis-benchmark params
RB_CLIENTS=200
RB_REQUESTS=1000000
RB_PIPELINE=16
RB_TESTS="set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd,mset"

# memtier_benchmark params
MT_CLIENTS=200
MT_THREADS=8
MT_RATIO="1:1"
MT_DATA_SIZE=256
MT_KEY_MAX=1000000
MT_REQUESTS=0        # 0 = use --test-time instead
MT_TEST_TIME=30      # seconds

# ── CLI parsing ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)       PORT="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,/^# ====/{ /^# ====/d; s/^# \?//; p }' "$0"
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/artifacts/benchmark-${TIMESTAMP}}"
mkdir -p "$OUTPUT_DIR"

# ── Logging helpers ──────────────────────────────────────────────────────────
_log()  { printf '\n\033[1;36m>>> %s\033[0m\n' "$*"; }
_ok()   { printf '\033[1;32m    ✔ %s\033[0m\n' "$*"; }
_warn() { printf '\033[1;33m    ⚠ %s\033[0m\n' "$*"; }
_err()  { printf '\033[1;31m    ✖ %s\033[0m\n' "$*" >&2; }

# ── Dependency checks / installation ────────────────────────────────────────
ensure_docker() {
  if ! command -v docker &>/dev/null; then
    _err "docker is not installed. Please install Docker first."
    exit 1
  fi
}

# Install redis-tools (provides both redis-benchmark and redis-cli) once.
ensure_redis_tools() {
  local need_benchmark=false
  local need_cli=false
  command -v redis-benchmark &>/dev/null && _ok "redis-benchmark found: $(command -v redis-benchmark)" || need_benchmark=true
  command -v redis-cli &>/dev/null      && _ok "redis-cli found: $(command -v redis-cli)"           || need_cli=true

  if ! $need_benchmark && ! $need_cli; then
    return
  fi

  _log "Installing redis-tools (provides redis-benchmark and redis-cli)…"
  if command -v apt-get &>/dev/null; then
    _warn "sudo is required to install the redis-tools system package via apt-get."
    sudo apt-get update -qq && sudo apt-get install -y -qq redis-tools
  elif command -v dnf &>/dev/null; then
    _warn "sudo is required to install the redis system package via dnf."
    sudo dnf install -y redis
  elif command -v brew &>/dev/null; then
    brew install redis
  else
    _err "Cannot auto-install redis-tools. Install redis-tools manually."
    exit 1
  fi

  $need_benchmark && { command -v redis-benchmark &>/dev/null || { _err "redis-benchmark still not found"; exit 1; }; }
  $need_cli       && { command -v redis-cli       &>/dev/null || { _err "redis-cli still not found";       exit 1; }; }
  _ok "redis-tools installed"
}

# memtier_benchmark is not widely available in distro repos, so we always
# run it via Docker for portability.
MEMTIER_DOCKER_IMAGE="redislabs/memtier_benchmark:latest"

ensure_memtier() {
  _log "Pulling memtier_benchmark Docker image (${MEMTIER_DOCKER_IMAGE})…"
  docker pull "$MEMTIER_DOCKER_IMAGE"
  _ok "memtier_benchmark will run via Docker"
}

# Wrapper: run memtier_benchmark via Docker.
# Uses --network=host so the tool can reach the server on 127.0.0.1:<PORT>.
# Output-dir is bind-mounted so JSON files land on the host.
memtier() {
  # Translate any --json-out-file=<path> so the container writes inside
  # the mounted volume and the host sees the file.
  local docker_args=()
  local has_json_out=false
  local json_host_path=""
  for arg in "$@"; do
    if [[ "$arg" == --json-out-file=* ]]; then
      json_host_path="${arg#--json-out-file=}"
      docker_args+=("--json-out-file=/output/$(basename "$json_host_path")")
      has_json_out=true
    else
      docker_args+=("$arg")
    fi
  done

  local vol_mount=()
  if $has_json_out && [[ -n "$json_host_path" ]]; then
    vol_mount=(-v "$(dirname "$json_host_path"):/output")
  fi

  docker run --rm --network=host \
    "${vol_mount[@]}" \
    "$MEMTIER_DOCKER_IMAGE" \
    "${docker_args[@]}"
}

# ── Container run helper (applies resource limits) ───────────────────────────
# Builds the common docker-run flags for resource constraints.
_docker_resource_flags() {
  local flags=()
  [[ -n "$BENCH_CPUSET" ]] && flags+=(--cpuset-cpus="$BENCH_CPUSET")
  [[ -n "$BENCH_MEMORY" ]] && flags+=(--memory="$BENCH_MEMORY")
  [[ ${#flags[@]} -gt 0 ]] && printf '%s\n' "${flags[@]}"
  return 0
}

# ── Docker helpers ───────────────────────────────────────────────────────────
container_running() {
  docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$1"
}

wait_for_server() {
  local label="$1"
  _log "Waiting for ${label} to be ready on port ${PORT}…"
  local elapsed=0
  while ! redis-cli -h 127.0.0.1 -p "$PORT" PING 2>/dev/null | grep -q PONG; do
    sleep 0.5
    elapsed=$((elapsed + 1))
    if (( elapsed > WAIT_READY_TIMEOUT * 2 )); then
      _err "${label} did not become ready within ${WAIT_READY_TIMEOUT}s"
      exit 1
    fi
  done
  _ok "${label} is ready"
}

stop_container() {
  local name="$1"
  if container_running "$name"; then
    _log "Stopping container ${name}…"
    docker stop "$name" >/dev/null 2>&1 || true
    docker rm -f "$name" >/dev/null 2>&1 || true
    _ok "Stopped ${name}"
  fi
}

flush_server() {
  redis-cli -h 127.0.0.1 -p "$PORT" FLUSHALL >/dev/null 2>&1 || true
}

# Warm up the server so first-run allocation / JIT / page-fault costs
# don't bias the real benchmark.
warmup() {
  _log "Warming up server on port ${PORT}…"
  redis-benchmark -h 127.0.0.1 -p "$PORT" \
    -c 100 -n 200000 -t set,get -q > /dev/null 2>&1 || true
  flush_server
  _ok "Warm-up complete"
}

# ── Start/stop Redis ────────────────────────────────────────────────────────
start_redis() {
  stop_container "bench-redis"
  _log "Pulling Redis image (${REDIS_IMAGE})…"
  docker pull "$REDIS_IMAGE" 2>/dev/null
  _log "Starting Redis container…"
  local res_flags
  mapfile -t res_flags < <(_docker_resource_flags)
  docker run -d --name bench-redis \
    --network=host \
    --ulimit "nofile=${BENCH_NOFILE_SOFT}:${BENCH_NOFILE_HARD}" \
    "${res_flags[@]}" \
    --rm \
    "$REDIS_IMAGE" \
    redis-server \
      --port "$PORT" \
      --save "" \
      --appendonly no \
      --protected-mode no \
      --maxclients 100000 \
      --tcp-backlog 65535 >/dev/null
  wait_for_server "Redis"
}

stop_redis() {
  stop_container "bench-redis"
}

# ── Start/stop PeaDB ────────────────────────────────────────────────────────
# PeaDB has appendonly=false and no automatic RDB snapshots by default,
# so persistence is already disabled — symmetric with the Redis flags above.
start_peadb() {
  stop_container "bench-peadb"
  if ! docker image inspect peadb:bench &>/dev/null; then
    _log "Building PeaDB Docker image…"
    docker build -t peadb:bench -f "${REPO_ROOT}/Dockerfile" "$REPO_ROOT" 2>&1 \
      | tail -5
  else
    _ok "Reusing existing peadb:bench image (set BENCH_REBUILD=1 to force rebuild)"
    if [[ "${BENCH_REBUILD:-}" == "1" ]]; then
      _log "Rebuilding PeaDB Docker image (BENCH_REBUILD=1)…"
      docker build -t peadb:bench -f "${REPO_ROOT}/Dockerfile" "$REPO_ROOT" 2>&1 \
        | tail -5
    fi
  fi
  _log "Starting PeaDB container…"
  local res_flags
  mapfile -t res_flags < <(_docker_resource_flags)
  docker run -d --name bench-peadb \
    --network=host \
    --ulimit "nofile=${BENCH_NOFILE_SOFT}:${BENCH_NOFILE_HARD}" \
    "${res_flags[@]}" \
    --rm \
    peadb:bench \
    --port "$PORT" --bind 0.0.0.0 >/dev/null
  wait_for_server "PeaDB"
}

stop_peadb() {
  stop_container "bench-peadb"
}

# ── Benchmark runners ───────────────────────────────────────────────────────

# 1. redis-benchmark — built-in commands
run_redis_benchmark() {
  local label="$1"  # "redis" or "peadb"
  local outfile="${OUTPUT_DIR}/${label}_redis_benchmark.txt"
  local csvfile="${OUTPUT_DIR}/${label}_redis_benchmark.csv"

  _log "[${label}] redis-benchmark: ${RB_TESTS} (${RB_REQUESTS} reqs, pipeline=${RB_PIPELINE}, clients=${RB_CLIENTS})"

  flush_server

  # Run once with --csv; derive human-readable view from the same data
  # to avoid double-execution and cache-state perturbation.
  redis-benchmark \
    -h 127.0.0.1 -p "$PORT" \
    -c "$RB_CLIENTS" \
    -n "$RB_REQUESTS" \
    -P "$RB_PIPELINE" \
    -t "$RB_TESTS" \
    -q \
    --csv \
    2>&1 | tee "$csvfile"

  # Derive a human-readable summary from the CSV
  awk -F',' 'NR>0 && NF>=2 { gsub(/"/, "", $1); gsub(/"/, "", $2); printf "%-30s %s requests per second\n", $1":", $2 }' \
    "$csvfile" > "$outfile"

  _ok "Saved → $(basename "$csvfile"), $(basename "$outfile")"
}

# 2. redis-benchmark — GET/SET with various data sizes
run_redis_benchmark_datasizes() {
  local label="$1"
  local outfile="${OUTPUT_DIR}/${label}_redis_benchmark_datasizes.txt"

  _log "[${label}] redis-benchmark: data-size sweep (64, 256, 1024, 4096 bytes)"

  : > "$outfile"
  for sz in 64 256 1024 4096; do
    flush_server
    echo "=== Data size: ${sz} bytes ===" >> "$outfile"
    redis-benchmark \
      -h 127.0.0.1 -p "$PORT" \
      -c "$RB_CLIENTS" \
      -n "$RB_REQUESTS" \
      -P "$RB_PIPELINE" \
      -t set,get \
      -d "$sz" \
      -q \
      2>&1 | tee -a "$outfile"
    echo "" >> "$outfile"
  done

  _ok "Saved → $(basename "$outfile")"
}

# 3. redis-benchmark — single-key latency (no pipeline)
run_redis_benchmark_latency() {
  local label="$1"
  local outfile="${OUTPUT_DIR}/${label}_redis_benchmark_latency.txt"

  _log "[${label}] redis-benchmark: latency (no pipeline, 50 clients, 100k reqs)"

  flush_server

  redis-benchmark \
    -h 127.0.0.1 -p "$PORT" \
    -c 50 \
    -n 100000 \
    -P 1 \
    -t set,get,incr,lpush,rpush \
    -q \
    2>&1 | tee "$outfile"

  _ok "Saved → $(basename "$outfile")"
}

# 4. memtier_benchmark — mixed workload
run_memtier_benchmark() {
  local label="$1"
  local outfile="${OUTPUT_DIR}/${label}_memtier.txt"
  local jsonfile="${OUTPUT_DIR}/${label}_memtier.json"

  _log "[${label}] memtier_benchmark: clients=${MT_CLIENTS}, threads=${MT_THREADS}, ratio=${MT_RATIO}, data=${MT_DATA_SIZE}B, ${MT_TEST_TIME}s"

  flush_server

  local mt_args=(
    --server=127.0.0.1
    --port="$PORT"
    --protocol=redis
    --clients="$MT_CLIENTS"
    --threads="$MT_THREADS"
    --ratio="$MT_RATIO"
    --data-size="$MT_DATA_SIZE"
    --key-maximum="$MT_KEY_MAX"
    --test-time="$MT_TEST_TIME"
    --hide-histogram
    --json-out-file="$jsonfile"
  )
  if (( MT_REQUESTS > 0 )); then
    mt_args+=(--requests="$MT_REQUESTS")
  fi

  memtier "${mt_args[@]}" 2>&1 | tee "$outfile"

  _ok "Saved → $(basename "$outfile"), $(basename "$jsonfile")"

  # Extract tail-latency summary from memtier JSON
  extract_tail_latency "$label" "mixed" "$jsonfile"
}

# 5. memtier_benchmark — GET-only (read-heavy)
run_memtier_get_only() {
  local label="$1"
  local outfile="${OUTPUT_DIR}/${label}_memtier_get_only.txt"

  _log "[${label}] memtier_benchmark: GET-only (ratio 0:1, ${MT_TEST_TIME}s)"

  # Pre-populate keys (5 000 reqs × 200 clients = 1M keys ≈ key-maximum)
  flush_server
  memtier \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --clients=50 --threads=4 --ratio=1:0 \
    --data-size="$MT_DATA_SIZE" --key-maximum="$MT_KEY_MAX" \
    --requests=5000 --hide-histogram >/dev/null 2>&1 || true

  memtier \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --clients="$MT_CLIENTS" --threads="$MT_THREADS" \
    --ratio=0:1 \
    --data-size="$MT_DATA_SIZE" --key-maximum="$MT_KEY_MAX" \
    --test-time="$MT_TEST_TIME" \
    --hide-histogram \
    2>&1 | tee "$outfile"

  _ok "Saved → $(basename "$outfile")"
}

# 6. memtier_benchmark — SET-only (write-heavy)
run_memtier_set_only() {
  local label="$1"
  local outfile="${OUTPUT_DIR}/${label}_memtier_set_only.txt"

  _log "[${label}] memtier_benchmark: SET-only (ratio 1:0, ${MT_TEST_TIME}s)"

  flush_server

  memtier \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --clients="$MT_CLIENTS" --threads="$MT_THREADS" \
    --ratio=1:0 \
    --data-size="$MT_DATA_SIZE" --key-maximum="$MT_KEY_MAX" \
    --test-time="$MT_TEST_TIME" \
    --hide-histogram \
    2>&1 | tee "$outfile"

  _ok "Saved → $(basename "$outfile")"
}

# ── Tail-latency extraction from memtier JSON ───────────────────────────────
extract_tail_latency() {
  local label="$1" test_name="$2" jsonfile="$3"
  local outfile="${OUTPUT_DIR}/${label}_tail_latency_${test_name}.txt"

  if [[ ! -f "$jsonfile" ]]; then
    _warn "No JSON file for tail-latency extraction: $jsonfile"
    return
  fi

  # Extract p50, p95, p99, p99.9 from memtier JSON
  python3 -c "
import json, sys
with open('$jsonfile') as f:
    data = json.load(f)
totals = data.get('ALL STATS', data).get('Totals', {})
for op in ['Sets', 'Gets', 'Totals']:
    d = totals.get(op, {})
    if not d:
        continue
    latencies = d.get('Latency', {})
    if not latencies:
        continue
    p50  = latencies.get('p50.00',  latencies.get('p50', 'N/A'))
    p95  = latencies.get('p95.00',  latencies.get('p95', 'N/A'))
    p99  = latencies.get('p99.00',  latencies.get('p99', 'N/A'))
    p999 = latencies.get('p99.90',  latencies.get('p99.9', 'N/A'))
    print(f'{op:10s}  p50={p50}  p95={p95}  p99={p99}  p99.9={p999}')
" > "$outfile" 2>/dev/null || _warn "Could not parse tail latencies from $jsonfile"

  if [[ -s "$outfile" ]]; then
    _ok "Tail latencies (${test_name}) → $(basename "$outfile")"
  fi
}

# ── Optional long-duration stability test ────────────────────────────────────
run_long_stability_test() {
  local label="$1"
  if (( LONG_RUN_SECS <= 0 )); then
    return
  fi
  local outfile="${OUTPUT_DIR}/${label}_memtier_long_run.txt"
  local jsonfile="${OUTPUT_DIR}/${label}_memtier_long_run.json"

  _log "[${label}] memtier_benchmark: long-duration stability test (${LONG_RUN_SECS}s)"
  flush_server

  memtier \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --clients="$MT_CLIENTS" --threads="$MT_THREADS" \
    --ratio="$MT_RATIO" \
    --data-size="$MT_DATA_SIZE" --key-maximum="$MT_KEY_MAX" \
    --test-time="$LONG_RUN_SECS" \
    --hide-histogram \
    --json-out-file="$jsonfile" \
    2>&1 | tee "$outfile"

  extract_tail_latency "$label" "long_run" "$jsonfile"
  _ok "Saved → $(basename "$outfile")"
}

# ── Run all benchmarks for a given target ────────────────────────────────────
run_all_benchmarks() {
  local label="$1"
  _log "═══════════════════════════════════════"
  _log "  Running all benchmarks for: ${label^^}"
  _log "═══════════════════════════════════════"

  warmup

  run_redis_benchmark       "$label"
  sleep "$COOLDOWN_SECS"
  run_redis_benchmark_datasizes "$label"
  sleep "$COOLDOWN_SECS"
  run_redis_benchmark_latency "$label"
  sleep "$COOLDOWN_SECS"
  run_memtier_benchmark     "$label"
  sleep "$COOLDOWN_SECS"
  run_memtier_get_only      "$label"
  sleep "$COOLDOWN_SECS"
  run_memtier_set_only      "$label"
  sleep "$COOLDOWN_SECS"
  run_long_stability_test   "$label"
}

# ── Comparison / summary ────────────────────────────────────────────────────
extract_rps() {
  # Parse "TEST: N.NN requests per second..." from redis-benchmark -q output
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "(no data)"
    return
  fi
  grep -oP '^\S.*:\s+\K[\d.]+(?= requests)' "$file" 2>/dev/null || echo "(parse error)"
}

generate_comparison() {
  local report="${OUTPUT_DIR}/comparison_report.txt"
  _log "Generating comparison report…"

  {
    echo "============================================================================"
    echo "  Redis vs PeaDB  —  Benchmark Comparison Report"
    echo "  Date: $(date)"
    echo "  Host: $(hostname) — $(nproc) CPUs, $(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo 'N/A') RAM"
    echo "============================================================================"
    echo ""

    # ── redis-benchmark comparison ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  redis-benchmark  (${RB_REQUESTS} reqs, pipeline=${RB_PIPELINE}, ${RB_CLIENTS} clients)         ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    printf "%-25s %18s %18s %10s\n" "Test" "Redis (req/s)" "PeaDB (req/s)" "Ratio"
    printf "%-25s %18s %18s %10s\n" "-------------------------" "------------------" "------------------" "----------"

    local redis_csv="${OUTPUT_DIR}/redis_redis_benchmark.csv"
    local peadb_csv="${OUTPUT_DIR}/peadb_redis_benchmark.csv"
    if [[ -f "$redis_csv" && -f "$peadb_csv" ]]; then
      # CSV format: "TEST","rps","avg_latency","min_latency","p50","p95","p99","max_latency"
      while IFS=',' read -r test rps _rest; do
        test="${test//\"/}"
        rps="${rps//\"/}"
        [[ "$test" == "test" || -z "$test" ]] && continue
        # Find corresponding PeaDB line
        local prps
        prps=$(grep "\"${test}\"" "$peadb_csv" 2>/dev/null | head -1 | cut -d',' -f2 | tr -d '"')
        if [[ -n "$rps" && -n "$prps" && "$rps" != "0" ]]; then
          local ratio
          ratio=$(awk "BEGIN { printf \"%.2f\", ${prps}/${rps} }")
          printf "%-25s %18s %18s %10sx\n" "$test" "$rps" "$prps" "$ratio"
        else
          printf "%-25s %18s %18s %10s\n" "$test" "${rps:-(n/a)}" "${prps:-(n/a)}" "-"
        fi
      done < "$redis_csv"
    else
      echo "(CSV files not found — skipping tabular comparison)"
    fi

    echo ""

    # ── redis-benchmark latency ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  redis-benchmark latency  (no pipeline, 50 clients, 100k reqs)         ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "--- Redis ---"
    cat "${OUTPUT_DIR}/redis_redis_benchmark_latency.txt" 2>/dev/null || echo "(no data)"
    echo ""
    echo "--- PeaDB ---"
    cat "${OUTPUT_DIR}/peadb_redis_benchmark_latency.txt" 2>/dev/null || echo "(no data)"
    echo ""

    # ── redis-benchmark data-size sweep ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  redis-benchmark data-size sweep  (64, 256, 1024, 4096 bytes)          ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "--- Redis ---"
    cat "${OUTPUT_DIR}/redis_redis_benchmark_datasizes.txt" 2>/dev/null || echo "(no data)"
    echo ""
    echo "--- PeaDB ---"
    cat "${OUTPUT_DIR}/peadb_redis_benchmark_datasizes.txt" 2>/dev/null || echo "(no data)"
    echo ""

    # ── memtier mixed ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  memtier_benchmark  (mixed 1:1, ${MT_DATA_SIZE}B, ${MT_TEST_TIME}s)                       ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "--- Redis ---"
    cat "${OUTPUT_DIR}/redis_memtier.txt" 2>/dev/null || echo "(no data)"
    echo ""
    echo "--- PeaDB ---"
    cat "${OUTPUT_DIR}/peadb_memtier.txt" 2>/dev/null || echo "(no data)"
    echo ""

    # ── memtier GET-only ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  memtier_benchmark  (GET-only, ${MT_DATA_SIZE}B, ${MT_TEST_TIME}s)                        ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "--- Redis ---"
    cat "${OUTPUT_DIR}/redis_memtier_get_only.txt" 2>/dev/null || echo "(no data)"
    echo ""
    echo "--- PeaDB ---"
    cat "${OUTPUT_DIR}/peadb_memtier_get_only.txt" 2>/dev/null || echo "(no data)"
    echo ""

    # ── memtier SET-only ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  memtier_benchmark  (SET-only, ${MT_DATA_SIZE}B, ${MT_TEST_TIME}s)                        ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "--- Redis ---"
    cat "${OUTPUT_DIR}/redis_memtier_set_only.txt" 2>/dev/null || echo "(no data)"
    echo ""
    echo "--- PeaDB ---"
    cat "${OUTPUT_DIR}/peadb_memtier_set_only.txt" 2>/dev/null || echo "(no data)"
    echo ""

    # ── Tail latency (p50/p95/p99/p99.9) ──
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  Tail Latency Summary  (from memtier JSON)                             ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    for tl_test in mixed; do
      for tl_label in redis peadb; do
        local tl_file="${OUTPUT_DIR}/${tl_label}_tail_latency_${tl_test}.txt"
        if [[ -s "$tl_file" ]]; then
          echo "--- ${tl_label^} (${tl_test}) ---"
          cat "$tl_file"
          echo ""
        fi
      done
    done

    # ── Long-run stability (if available) ──
    if [[ -f "${OUTPUT_DIR}/redis_memtier_long_run.txt" || -f "${OUTPUT_DIR}/peadb_memtier_long_run.txt" ]]; then
      echo "╔══════════════════════════════════════════════════════════════════════════╗"
      echo "║  Long-Duration Stability Test  (${LONG_RUN_SECS}s)                                ║"
      echo "╚══════════════════════════════════════════════════════════════════════════╝"
      echo ""
      echo "--- Redis ---"
      cat "${OUTPUT_DIR}/redis_memtier_long_run.txt" 2>/dev/null || echo "(no data)"
      echo ""
      echo "--- PeaDB ---"
      cat "${OUTPUT_DIR}/peadb_memtier_long_run.txt" 2>/dev/null || echo "(no data)"
      echo ""
      for tl_label in redis peadb; do
        local tl_file="${OUTPUT_DIR}/${tl_label}_tail_latency_long_run.txt"
        if [[ -s "$tl_file" ]]; then
          echo "--- ${tl_label^} (long-run tail latency) ---"
          cat "$tl_file"
          echo ""
        fi
      done
    fi

    echo "============================================================================"
    echo "  Raw output files: ${OUTPUT_DIR}/"
    echo "============================================================================"
  } | tee "$report"

  _ok "Full report saved to: ${report}"
}

# ── Cleanup trap ─────────────────────────────────────────────────────────────
cleanup() {
  _log "Cleaning up containers…"
  stop_container "bench-redis"
  stop_container "bench-peadb"
}
trap cleanup EXIT

# Remove leftover containers from a previous crashed run.
docker rm -f bench-redis bench-peadb 2>/dev/null || true

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
main() {
  _log "Redis vs PeaDB Benchmark Suite"
  _log "Output directory: ${OUTPUT_DIR}"

  # ── Prerequisites ──
  _log "Checking / installing prerequisites…"
  ensure_docker
  ensure_redis_tools
  ensure_memtier

  # ── Phase 1: Benchmark Redis ──
  _log "═══════════════════════════════════════"
  _log "  PHASE 1:  Redis"
  _log "═══════════════════════════════════════"
  start_redis
  run_all_benchmarks "redis"
  stop_redis
  sleep "$COOLDOWN_SECS"

  # ── Phase 2: Benchmark PeaDB ──
  _log "═══════════════════════════════════════"
  _log "  PHASE 2:  PeaDB"
  _log "═══════════════════════════════════════"
  start_peadb
  run_all_benchmarks "peadb"
  stop_peadb

  # ── Phase 3: Compare ──
  generate_comparison

  _log "Done! Results in ${OUTPUT_DIR}/"
}

main "$@"
