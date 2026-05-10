# Commit Review — `2f42edad92afa2e8bf8a95db7df91e6c3e8d186d`

Date: 2026-05-10

## Verdict

Good direction and substantial progress, but **not ready to merge** yet because this commit introduces/keeps critical correctness issues.

---

## Findings (ordered by severity)

### P0 — `SCAN` cursor algorithm is broken (can loop forever)

- Code path: `src/datastore.cpp:1772-1792`, `src/datastore.cpp:1805-1833`
- Problem: the new reverse-binary cursor logic returns cursor values that cycle and do not converge to `0` in practice.
- Repro (local): after inserting keys, repeated `SCAN` returned a cycle like `380 -> 242 -> 378 -> 342 -> 190 -> 241 -> 409 -> 380` and never terminated.
- Impact: clients can hang indefinitely when iterating keys; this is a major compatibility regression.

### P0 — Deferred `WAIT` breaks RESP reply ordering on pipelined commands

- Code path: `src/command.cpp:2030-2056`, `src/server.cpp:564-575`, `src/server.cpp:395-410`
- Problem: `WAIT` now returns an empty immediate reply and sets `wait_state`, but the server continues parsing later pipelined commands from the same socket in the same read loop.
- Repro (local): send `WAIT 1 1000` then `PING` on one connection; first reply is `+PONG`, second reply (after timeout) is `:0`. `WAIT` should not be overtaken by later command replies.
- Impact: protocol-level ordering violation for pipelined clients.

### P0 — New `REPLICAOF` path mutates datastore off the event-loop thread

- Code path: `src/command.cpp:2080-2085`, `src/command.cpp:7035-7039`
- Problem: detached thread runs `rdb_load(...)` directly, which mutates global datastore while the main event loop is active.
- Impact: data races/undefined behavior under concurrent client traffic (reads/writes while full sync loads).

### P1 — Compatibility scoreboard generation is currently invalid

- Code path: `scripts/ci/generate_compat_scoreboard.py:45-53`
- Problem: invocation detection only matches `r.<UPPERCASE>(...)` patterns and misses the repo’s predominant raw-RESP/custom helper style.
- Evidence: generated scoreboard reports `full: 0, partial: 0, stub: 207` (`compat/commands.generated.md:5`) despite broad integration coverage.
- Impact: the new compatibility artifact is misleading and not actionable.

### P1 — Integration cleanup refactor is incomplete

- Code path: `scripts/ci/run_integration_tests.sh:91`, `scripts/ci/run_integration_tests.sh:111-113`
- Problem: `_server_pid` is never assigned, so `stop_server` never actually stops leaked servers.
- Impact: stale processes from failed tests are still possible; fix intent is not fully realized.

### P2 — RDB stream persistence still loses data (warn-only)

- Code path: `src/rdb.cpp:722-727`
- Problem: stream keys are skipped during RDB save; now logged, but save still returns success.
- Impact: silent data loss was reduced to warned data loss, but persistence parity is still not achieved.

### P3 — Command contract test mutates golden file during test execution

- Code path: `tests/unit/command_contract_test.cpp:75-80`
- Problem: on added commands, test rewrites the golden file and passes instead of failing for explicit review.
- Impact: weakens the intended CI gate for command-surface drift.

---

## What this commit does well

1. Fixes config precedence (config file baseline, CLI override) in `src/main.cpp`.
2. Adds CI workflows (`.github/workflows/pr.yml`, `.github/workflows/nightly.yml`).
3. Adds persistent AOF fd + `appendfsync` timer hook (`src/command.cpp`, `src/server.cpp`).
4. Adds background child reap logic for `BGSAVE`/`BGREWRITEAOF` (`src/command.cpp:7047+`).
5. Adds atomic temp-file + rename path for RDB/AOF rewrite outputs.
6. Removes destructive `pkill -9` pattern from integration runner.
7. Improves `ZSET` range/pop complexity using ordered index.
8. Unifies mutation epoch usage to datastore-owned counter.
9. Extracts poller abstraction to `include/poller.hpp`.

---

## Coverage Against `improve-*.md`

## 1) Does this address all issues from:
- `.AI-LLM/improve/improve-auggie.md`
- `.AI-LLM/improve/improve-opus.md`
- `.AI-LLM/improve/improve-codex.md`
- `.AI-LLM/improve/improve-gpt.md`

**No.**

### Addressed (full or mostly full)

- Config precedence fix.
- AOF persistent fd + periodic fsync policy scaffolding.
- BGSAVE/BGREWRITE child tracking and reaping.
- Atomic temp-file + rename for persistence writes.
- Initial CI workflows.
- Removal of global `pkill -9`.
- Command count drift (`147 -> 207`) in README.
- Mutation-epoch dual-counter cleanup.
- Poller extraction.

### Partially addressed

- `WAIT` made non-blocking, but introduces pipelining ordering bug.
- Replication backlog/PSYNC partial-resync scaffolding added, but replication/INFO parity is still incomplete.
- RDB streams: now warned, still not persisted.
- Compatibility scoreboard added, but currently produces incorrect all-stub output.
- Warning gate started (`-Wall -Wextra`) but no strict fail-on-warning policy.

### Still not addressed (major examples)

- Full cluster-bus binary protocol compatibility.
- Accurate, non-placeholder replication observability fields (`INFO replication` remains partially hardcoded).
- Execution-based coverage gate replacing mention-based command coverage script.
- JUnit/structured test report outputs.
- DB-explicit datastore API refactor and larger architectural items (`Entry` variant, sharding safety, full RESP typed values).

---

## Validation run

- `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release` ✅
- `cmake --build build -j8` ✅
- `cd build && ctest --output-on-failure` ✅ (4/4)
- `python3 scripts/ci/check_command_test_coverage.py` ✅
- `python3 scripts/ci/generate_compat_scoreboard.py` ✅ (but output quality issue noted above)

