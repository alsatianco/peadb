<!-- doc: Documents what lives under scripts/ and how to set up the Python env to run the helper/CI/dev scripts. -->

# Scripts

This folder contains repo-maintenance and developer helper scripts, grouped by purpose.

Every script includes a detailed header comment (shell) or docstring (Python) at the top of the file describing:
- the script's **purpose** — why it exists,
- **what it does** — step-by-step,
- **how to run** it — with concrete command examples,
- **how to interpret its output** — example success/failure output and what to look for,
- **prerequisites** — required tools, running servers, available ports,
- **exit codes** — what 0 vs non-zero means,
- and any **limitations / caveats**.

## Layout

Each script's header documents it exhaustively (see above), so this table maps purpose to folder rather than repeating file lists — `ls` the folder for the current contents.

| Folder | Purpose |
|--------|---------|
| `scripts/run_all_tests.sh` | The full pre-commit gate: all test stages (unit → integration → … → upstream TCL → differential) in one run |
| `scripts/ci/` | CI gates and generators: compat-delta validation, suite ratchet, command-coverage check, scoreboard + command-metadata codegen, integration-test runner |
| `scripts/redis/` | Upstream Redis TCL harness (`run_redis_tests.sh`, result parser, per-suite `skipfiles/`) and RDB bridge/sync utilities |
| `scripts/qa/` | Quality assurance: fuzz sanity, perf baseline, ASAN/UBSAN stability, sentinel/replication validation |
| `scripts/dev/` | Local development helpers: quick start, dependency install, Redis-vs-PeaDB benchmark, memtier |
| `scripts/debug/` | Ad-hoc developer debugging scripts |
| `scripts/examples/` | Optional/experimental scripts (may need extra Python deps) |

## Running scripts

General guidance:
- Prefer invoking scripts from the repo root, e.g. `scripts/qa/run_stability_checks.sh`.
- Many scripts also work when run from other working directories by internally anchoring to the repo root.

Quick reference (most common workflows):

```bash
# First-time setup on Ubuntu/Debian
scripts/dev/install_deps_ubuntu.sh

# Build + start server (one command)
scripts/dev/quick_start.sh

# Run integration tests (each test starts its own servers)
scripts/ci/run_integration_tests.sh

# Run integration tests after building peadb-server with ASAN/UBSAN
PEADB_TEST_SANITIZED=1 TIMEOUT_SECS=90 scripts/ci/run_integration_tests.sh

# Run ASAN/UBSAN stability checks
scripts/qa/run_stability_checks.sh --seconds 30

# Run the adopted upstream Redis Tcl suites, as in nightly CI
SUITE_MODE=native OUT_DIR=artifacts/redis-native scripts/redis/run_redis_tests.sh

# Check that every command has test coverage
python3 scripts/ci/check_command_test_coverage.py
```

Exit codes:
- All scripts follow the Unix convention: exit code `0` means success; non-zero means failure.

Artifacts and logs:
- Several QA/Redis-harness scripts write logs and repro commands under `artifacts/`.
- If you want to change output locations, check the script header for supported flags/env vars.

## Common prerequisites

Depending on the script, you may need:
- Build tools: `cmake`, a C++ compiler toolchain
- `python3`
- Redis CLI/tools: `redis-cli` (package: `redis-tools` on Ubuntu/Debian)
- Redis server binary: `redis-server` (for the RDB import/export helpers)
- `timeout` (GNU coreutils) for per-test timeouts (optional; scripts fall back when unavailable)
- `redis-benchmark` for performance baselines (optional; script can fall back to Python)

## Frequently used environment variables

These are not exhaustive; each script documents its own configuration.

- `TIMEOUT_SECS`: per-test timeout for `scripts/ci/run_integration_tests.sh`.
- `PEADB_TEST_SANITIZED`: set to `1` for sanitizer builds to extend heavy fixture I/O budgets and leave the process RSS bound to Release tests; allocator accounting and eviction assertions still run.
- `PEADB_BIN`: path to the `peadb-server` binary for scripts that need to start the server.
- `START_PEADB`: for `scripts/redis/run_redis_tests.sh`, set to `0` to test against an already-running server.
- `REDIS_VERSION`: Redis version to fetch/build for running upstream Redis Tcl tests.
- `REDIS_SERVER`: path to `redis-server` if it's not in `PATH` (RDB import/export scripts).

## Python environment (recommended)

Create a dedicated virtual environment inside `scripts/`:

```bash
cd scripts
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

Optional (lint/dev tools):

```bash
python -m pip install -r requirements-dev.txt
```

Notes:

- Most scripts use only the Python standard library.
- Some scripts under `scripts/examples/` use third-party packages (e.g. `redis`).
