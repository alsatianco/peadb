# CLAUDE.md — Project Context for AI Assistants

## Project Overview

PeaDB is a high-performance, drop-in replacement for Redis 7.2.5, written in modern C++20. The goal is **behavioral indistinguishability** from Redis — same protocol, same commands, same edge-case semantics. Redis is the executable specification; for undocumented behavior, Redis source code defines truth.

- **Language:** C++20 (GCC 11+ or Clang 14+), with C stubs for Lua libraries
- **Target platform:** Linux (not Windows)
- **License:** MIT
- **Docker image:** `alsatianco/peadb-server` on Docker Hub

## Repository Layout

```
include/           C++ headers (command, config, datastore, errors, logger, lua_engine, protocol, rdb, server)
src/               C++ implementation (~11.5k lines) + main entry point
tests/
  unit/            C++ unit tests (CTest): config_parser_test, wrongtype_framework_test
  integration/     Python integration tests (~95+ files, ~7k lines), organized by milestone (m0–m9) and polish stages (p0–p3)
  fuzz/            RESP parser and command dispatcher fuzz tests
  diff/            Differential tests (PeaDB vs a real Redis instance)
scripts/
  ci/              CI pipelines: run_integration_tests.sh, check_command_test_coverage.py, check_compat_delta.py
  qa/              QA: run_fuzz_sanity.sh, run_stability_checks.sh, run_perf_baseline.sh, run_sentinel_validation.sh
  dev/             Developer helpers: quick_start.sh, install_deps_ubuntu.sh, redis_vs_peadb_benchmark.sh
  redis/           Upstream Redis TCL test runner + RDB import/export bridge scripts
compat/            Compatibility delta tracker (delta.md — known gaps vs Redis 7.2.5)
docs/decisions/    Architecture decision records (ADRs)
artifacts/         Benchmark results and performance baselines (gitignored)
```

## Build Commands

```bash
# Install dependencies (Ubuntu/Debian)
scripts/dev/install_deps_ubuntu.sh
# or: sudo apt-get install build-essential cmake g++ liblua5.1-0-dev

# Release build (default)
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# ASAN/UBSAN debug build
cmake -S . -B build-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
cmake --build build-asan -j$(nproc)
```

The binary is `build/peadb-server`. Run with: `./build/peadb-server --port 6379 --bind 127.0.0.1`

## Test Commands

```bash
# Run ALL test stages (9 stages, requires running peadb-server on port 6379)
scripts/run_all_tests.sh

# C++ unit tests only
cd build && ctest --output-on-failure

# Integration tests only (requires running peadb-server)
scripts/ci/run_integration_tests.sh

# Fuzz / stability checks
scripts/qa/run_fuzz_sanity.sh
scripts/qa/run_stability_checks.sh

# Differential tests (PeaDB vs Redis side-by-side)
python3 tests/diff/run_diff_tests.py

# Upstream Redis TCL test suite
scripts/redis/run_redis_tests.sh

# Check command test coverage
python3 scripts/ci/check_command_test_coverage.py

# Skip specific test stages (comma-separated):
SKIP=perf,redis scripts/run_all_tests.sh
```

Integration tests use `pytest` with the `redis` Python package (`pip install redis>=5.0.0`). Dev tooling uses `ruff>=0.5.0` for linting/formatting (see `scripts/requirements-dev.txt`).

## Architecture Overview

- **Namespace:** All C++ code lives in the `peadb` namespace.
- **Core components:**
  - `Server` (`server.hpp/cpp`) — event loop, connection handling, entry point via `run_server()`
  - `DataStore` (`datastore.hpp/cpp`) — in-memory key-value store with types: String, Hash, List, Set, ZSet, Stream
  - `Command` (`command.hpp/cpp`) — command dispatcher and session state (RESP version, MULTI/EXEC, blocking, replication)
  - `Protocol` (`protocol.hpp/cpp`) — RESP2/RESP3 parser and serializer
  - `Config` (`config.hpp/cpp`) — Redis-compatible config parsing (CLI flags + config file)
  - `LuaEngine` (`lua_engine.hpp/cpp`) — Lua 5.1 scripting (EVAL/EVALSHA, FUNCTION/FCALL)
  - `RDB` (`rdb.hpp/cpp`) — RDB snapshot persistence
  - `Logger` (`logger.hpp/cpp`) — logging subsystem
- **Lua C stubs:** `lua_cjson_stub.c`, `lua_cmsgpack_stub.c`, `lua_bit_stub.c`, `lua_struct_stub.c` — provide Redis-compatible Lua libraries
- **Entry point:** `src/main.cpp` → parses config → calls `peadb::run_server()`

## Key Design Principles

1. **Redis compatibility is paramount.** Every command must match Redis behavior exactly — return types, error messages, error precedence, argument validation order, edge cases. When in doubt, match Redis.
2. **Redis is the spec.** For undocumented behavior, consult Redis source code. If PeaDB diverges, it's a bug unless explicitly documented in `compat/delta.md`.
3. **`OBJECT ENCODING` must return Redis-compatible encoding names** regardless of internal data structures.
4. **Error messages must match Redis exactly** where clients/tests depend on them. Use `wrongtype_error_reply()` from `errors.hpp` for WRONGTYPE errors.
5. **No new data types or commands** beyond what Redis provides (namespaced additive extensions allowed only if they don't break existing parsers).

## Coding Conventions

- **C++20** standard, no extensions (`CMAKE_CXX_EXTENSIONS OFF`)
- Headers in `include/`, implementations in `src/`
- Single namespace: `peadb`
- Header guards: `#pragma once`
- Build system: CMake (min 3.16), library target `peadb_core` linked by `peadb-server` executable and test binaries
- Integration tests: Python 3, pytest-style, one file per feature area, named `test_<milestone>_<feature>.py`
- Milestone naming: m0 (smoke) → m9 (nightly/advanced), p0–p3 (polish stages)

## Known Compatibility Deltas

Current known gaps vs Redis 7.2.5 (see `compat/delta.md` for details):

- **ACL:** Basic AUTH/default user only; full ACL engine deferred
- **RDB:** Native reader/writer for common types; full parity (all opcodes/encodings) in progress
- **AOF:** Core works; edge-case parity ongoing
- **Modules:** Minimal `RedisModule_*` API; RedisJSON/RediSearch certification deferred

## Supported Features (~147 commands)

Strings, hashes, lists, sets, sorted sets, streams, pub/sub, transactions (MULTI/EXEC/WATCH), Lua scripting (EVAL/EVALSHA/FUNCTION/FCALL), RDB/AOF persistence, PSYNC2 replication, cluster (16384 hash slots, MOVED/ASK, gossip), partial Redis Modules API.

## Docker

```bash
# Run published image
docker run -d --name peadb -p 6379:6379 -v peadb-data:/data alsatianco/peadb-server:latest

# Build locally
docker compose up -d
```

Multi-stage Dockerfile (Ubuntu 24.04 base). Env vars: `PEADB_PORT`, `PEADB_LOGLEVEL`, `PEADB_NOFILE_SOFT`, `PEADB_NOFILE_HARD`.

## Common Development Workflows

```bash
# Quick start (build + run)
scripts/dev/quick_start.sh

# Benchmark against Redis
scripts/dev/redis_vs_peadb_benchmark.sh

# Check compatibility delta
python3 scripts/ci/check_compat_delta.py
```

## Important Notes for AI Assistants

- When implementing or fixing commands, always verify behavior matches Redis 7.2.5 exactly.
- Integration tests require a running peadb-server instance. Unit tests (CTest) do not.
- The `peadb_core` library is the shared target for both the server and tests — add new source files there.
- Python test files follow the pattern `tests/integration/test_<milestone>_<feature>.py`.
- Performance baselines are in `artifacts/perf-baseline.csv`.
- ADRs for major design decisions are in `docs/decisions/`.
