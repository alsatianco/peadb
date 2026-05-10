# PeaDB Improvement Proposal

Generated: 2026-05-10

## Scope

This proposal is based on a static review of the current repository plus lightweight local validation. It focuses on changes that move PeaDB toward its stated goal: behavioral compatibility with Redis 7.2.5 and credible production readiness.

Local checks run during review:

- `ctest --test-dir build --output-on-failure`: passed, 2/2 tests.
- `python3 scripts/ci/check_command_test_coverage.py`: passed, 207 command names referenced.
- `python3 scripts/ci/check_compat_delta.py`: passed.

Existing working tree note: `CLAUDE.md` is already deleted in the worktree and was not touched.

## Executive Summary

PeaDB has a strong compatibility-oriented foundation: C++20 implementation, broad command surface, pinned Redis 7.2.5 target, Docker support, many integration scripts, RDB/AOF/replication/module work started, and useful QA helpers.

The main issue is that repository claims and production goals are ahead of the actual architecture and release gates. The highest leverage improvements are:

1. Make Redis compatibility measurable and release-blocking, not just stated.
2. Fix persistence, replication, Lua, and test-gate correctness gaps before deeper performance work.
3. Decompose the monolithic command/state implementation so future Redis-parity work is safe.
4. Add real CI and certification matrices for Redis tests, differential tests, client libraries, and modules.
5. Only after correctness gates are stronger, invest in sharding, io_uring, memory layout, and data-structure performance.

## Cross-Analysis Additions Incorporated

After comparing this proposal with Auggie, Codex, and Opus, the strongest missing pieces in my original analysis were mostly concrete implementation tactics and developer-experience gates. The following are now incorporated into the recommendations below:

- From Auggie: persistent AOF file descriptor plus `appendfsync`, pipelined reply coalescing, replication offset byte accounting without double encoding, O(1) write-buffer compaction, `BGSAVE` child reaping/concurrency guard, reverse-binary SCAN cursor direction, active-expire random sampling, `thread_local` audit, Poller extraction, and bundled Lua 5.1.
- From Codex: Redis-like config precedence, compiler-warning gate, measured `Entry` footprint concern, shared integration-test harness with dynamic ports, non-destructive process cleanup, JUnit/structured reports, and explicit success metrics.
- From Opus: `Entry` to `std::variant`, `DataStore` error returns via `expected`-style result types, domain state structs for replication/cluster/module state, sharding prerequisites/invariants, ACL engine scope using existing command flags, and INFO observability counters.

These points materially improve the proposal because they convert several broad themes into small, reviewable PRs.

## Current Repository Snapshot

Observed structure:

- Core C++ sources: `src/` and `include/`.
- Main command dispatcher: `src/command.cpp`, about 6.7K lines.
- Storage engine: `src/datastore.cpp`, about 1.8K lines.
- Networking/event loop: `src/server.cpp`, about 687 lines.
- Lua engine: `src/lua_engine.cpp` plus `lua_*_stub.c` compatibility libraries.
- Persistence: `src/rdb.cpp`, plus AOF logic in `src/command.cpp`.
- Tests: 95 Python integration tests, 2 C++ unit tests, 4 fuzz/stress harnesses, 2 JSON differential cases.
- Registered commands found in `src/command.cpp`: 207.
- No committed GitHub/GitLab CI workflow found.
- Other analyses measured additional engineering debt worth tracking: a warning-focused build currently surfaces compiler warnings, and `Entry` is a fat per-key struct before dynamic allocations.

Strengths:

- Redis compatibility target is explicit and pinned to Redis 7.2.5.
- Broad command table exists, including strings, hashes, lists, sets, sorted sets, streams, scripting, pub/sub, replication, cluster surface, and modules surface.
- Test/QA scripts are well documented and easy to discover.
- Dockerfile and docker-compose are present.
- Existing artifact logs show upstream Redis Stage-A tests are being attempted.

Important mismatch:

- `README.md` says about 147 commands, while implementation and coverage script report 207 command registrations.
- `FAQ.md` is stale in several places. Example: it says Pub/Sub delivery is not implemented and `PUBLISH` always returns 0, while current `src/command.cpp` has subscriber registries and message fanout callbacks.
- `docs/decisions/*` often describes earlier milestone scope rather than current implementation.

## Priority 0: Make Compatibility Truth Measurable

### Problem

The project requirement is Redis behavioral indistinguishability, but current gates do not prove that. The command coverage check only verifies that command names appear somewhere under `tests/`; it can pass when a command appears in comments or only tests one surface case. Differential testing has only two JSON cases. Upstream Redis Tcl testing is partial and existing artifacts show `unit/scripting` failures.

Existing upstream Redis artifact failures:

- `artifacts/redis-stage-a/failed_suites.txt`: `unit/scripting`.
- `artifacts/redis-stage-a/unit_scripting.log` shows failures in JSON numeric decoding and cmsgpack circular-reference behavior.

### Proposal

Create a compatibility scoreboard that is generated, committed, and updated by CI:

- `compat/commands.generated.md`: command inventory from `src/command.cpp`, with arity, flags, key positions, test ownership, and compatibility status.
- `compat/redis-tests.generated.md`: upstream Redis Tcl suites run, passed, failed, skipped, and reason for each skip.
- `compat/differential.generated.md`: differential case coverage by command and feature area.
- `compat/modules.generated.md`: RedisModule API symbols exported and certified module status.

Replace the current coverage gate with stronger gates:

- A command is not "covered" unless a test actually executes it against PeaDB.
- For implemented Redis commands, require at least one Redis-vs-PeaDB differential case or an explicit waiver in `compat/delta.md`.
- Treat upstream Redis test failures as tracked issues with repro commands and owners.

First implementation slice:

- Extend `tests/diff/run_diff_tests.py` to support multiple cases and per-command expected waivers.
- Add JSON cases for strings, hashes, lists, sets, sorted sets, TTL, transactions, and errors.
- Add a script that starts Redis 7.2.5 and PeaDB, runs all diff cases, writes a machine-readable report, and fails on unwaived mismatch.

## Priority 1: Fix Test Infrastructure and CI

### Problem

There is no committed hosted CI workflow. `scripts/run_all_tests.sh` is useful locally but depends on a running or resolvable server and several host tools. `scripts/ci/run_integration_tests.sh` uses `pkill -9 -f peadb-server`, which can kill unrelated developer or CI jobs on the same machine. CMake only registers two unit tests, while fuzz harnesses are built by separate scripts.

### Proposal

Add a real CI pipeline with explicit tiers:

- Tier 1, every PR: configure/build on Linux, `ctest`, command inventory generation, compat delta validation, smoke differential tests.
- Tier 2, every PR if Redis tools are available: Python integration suite with isolated ports and process-group cleanup.
- Tier 3, nightly: upstream Redis Tcl selected suites, ASAN/UBSAN stability, fuzz sanity, perf smoke.
- Tier 4, release: full Redis Tcl matrix, Sentinel validation, client-library certification, RDB/AOF roundtrip corpus, module certification.

Immediate script fixes:

- Update `scripts/ci/run_integration_tests.sh` to resolve `PEADB_BIN` like `scripts/run_all_tests.sh` does.
- Replace global `pkill -9 -f peadb-server` with per-test process tracking and process-group cleanup.
- Make tests allocate free ports dynamically where practical.
- Add skip reporting separate from pass reporting. A skipped Redis-dependent test should not look identical to a pass.
- Register fuzz/stress harnesses in CMake or add a CTest label that invokes `scripts/qa/run_fuzz_sanity.sh` in short mode.
- Add a shared Python integration harness for start/stop, readiness probing, free-port allocation, log capture, and owned-process cleanup. Migrate existing hardcoded-port tests gradually.
- Emit structured reports, preferably JUnit XML, from integration and diff runners so CI failures are searchable and attributable.
- Add a compiler-warning job with `-Wall -Wextra -Wpedantic`. Start non-fatal, fix the backlog, then promote to fatal.
- Add `.clang-format`, `.clang-tidy`, and `.editorconfig` once the formatting baseline is agreed.
- Fix Redis-like config precedence: load config first, then apply CLI overrides. Add tests because operators expect command-line flags to override config-file values.

## Priority 2: Close High-Risk Correctness Gaps

### 2.1 Persistence

Findings:

- `src/rdb.cpp` skips streams during normal RDB save: stream keys are silently omitted from `rdb_build_string()`.
- AOF append opens the AOF file for every write, has no fsync policy, and silently ignores open/write errors.
- `BGREWRITEAOF` runs synchronously despite the background name.
- `BGSAVE` forks but does not track child status or report robust `INFO persistence` state.
- Consecutive `BGSAVE` calls can overlap and write the same RDB target without a Redis-like concurrency guard.
- `DataStore::mutation_epoch()` returns a member that appears separate from the command-layer mutation counter, making `INFO persistence` risk reporting incorrect change counts.
- RDB support is a subset, but README language is stronger than the implementation in places.

Proposal:

- Add regression tests for RDB/AOF roundtrip of every supported type, including streams and consumer groups.
- If stream RDB parity is not ready, fail loudly or document a compatibility delta instead of silently dropping stream data.
- Write RDB/AOF through temp files plus atomic rename for rewrite paths.
- Hold a persistent AOF fd opened with append/close-on-exec semantics instead of opening the file on every mutation.
- Add configurable `appendfsync` semantics: `always`, `everysec`, `no`, with `everysec` driven by the event loop timer.
- Track and expose last save/rewrite status, child pid, start time, duration, and errors.
- Add `BGSAVE` child reaping via `waitpid(..., WNOHANG)` from the loop tick and reject overlapping background saves with Redis-compatible errors.
- Validate or explicitly skip Redis RDB checksums based on a documented policy.

### 2.2 Replication and WAIT

Findings:

- `PSYNC` always does full resync and does not implement partial resync/backlog semantics.
- `SYNC` returns an empty RDB payload.
- Replication events are kept in an unbounded vector, not a Redis-like backlog ring.
- `INFO replication` reports fields such as `connected_slaves:0` even though replica sessions are tracked elsewhere.
- `WAIT` sleeps in the command handler, which blocks the single event loop and therefore all clients.
- `REPLICAOF` uses a detached Python one-shot sync helper for source sync, not an in-process continuous replication client.
- Replication byte offset accounting currently risks duplicated work: command RESP size is calculated separately from event encoding. The append path should be the source of truth for bytes written.

Proposal:

- Replace `g_replication_events` with a bounded replication backlog keyed by byte offsets.
- Implement PSYNC2 partial resync rules around replid, replid2, offsets, and backlog availability.
- Make `WAIT` asynchronous in the event loop: register client wait state, send `REPLCONF GETACK *`, wake on ACK or timeout.
- Expose accurate `INFO replication` fields: connected replicas, offsets, backlog state, master link state, sync progress.
- Replace the Python helper path for `REPLICAOF` with an in-process replication client that performs full sync and then streams commands continuously.
- Make replication append return the encoded byte count and increment offsets from that result, avoiding separate size and encoding paths.
- Add differential/interop tests where Redis replicates from PeaDB and PeaDB replicates from Redis.

### 2.3 Lua Scripting

Findings:

- Existing Redis upstream artifacts fail `unit/scripting` for cjson numeric decoding and cmsgpack circular references.
- Lua compatibility libraries are named `*_stub.c`, which matches the current maturity: useful but not yet Redis-equivalent.
- Scripting touches command dispatch, replication shapes, deterministic time, RESP conversion, ACL, OOM behavior, and error stats. Regressions here are high impact.

Proposal:

- Prioritize fixing the existing upstream `unit/scripting` failures before expanding scripting scope.
- Build a Lua compatibility corpus from Redis 7.2.5 scripting tests, split into pass/fail/waived categories.
- Replace minimal cjson/cmsgpack implementations with vendored Redis-compatible libraries or prove compatibility through upstream tests.
- Bundle Lua 5.1 source rather than relying on platform Lua. This removes the macOS release-build optimization cap and makes scripting behavior more reproducible across developer and CI machines.
- Add tests for script replication output, `redis.setresp`, RESP2/RESP3 conversion, `SCRIPT KILL`, and read-only script enforcement.

### 2.4 Auth and ACL

Findings:

- `AUTH` accepts credentials and returns OK without real enforcement.
- `ACL SETUSER` is stubbed and most ACL semantics are missing.
- Docker runs bound to `0.0.0.0` by default without auth unless users add protection externally.

Proposal:

- Implement minimum real auth first: `requirepass`, default user on/off, password matching, unauthenticated command restrictions, and `HELLO AUTH` behavior.
- Then implement ACL categories, key patterns, command allow/deny rules, user lifecycle, and `ACL LOG`/`ACL LIST`/`ACL GETUSER` behavior.
- Reuse existing `CommandSpec::flag_bits` category metadata for ACL checks rather than inventing a second command-category model.
- Until real auth lands, keep security limitations prominent in README, FAQ, Docker docs, and `compat/delta.md`.

## Priority 3: Refactor the Core for Safe Compatibility Work

### Problem

`src/command.cpp` owns command registration, command implementations, persistence state, AOF, replication state, cluster state, module state, pub/sub registries, stats, and Lua coordination. This makes compatibility fixes risky because unrelated domains share globals and helper functions.

`DataStore` also stores `current_db_` globally. The server switches this global to match the session before each command. That works in the current single-threaded model but is hostile to sharding, worker threads, and deterministic multi-client testing.

### Proposal

Introduce explicit runtime objects and split domains without changing behavior first:

- `ServerContext`: config, stats, persistence manager, replication manager, pub/sub manager, cluster state, module registry, command registry.
- `CommandExecutor`: validates arity, auth, cluster routing, script restrictions, transaction state, and dispatches commands.
- `CommandRegistry`: registers command specs by domain.
- `DataStore`: make DB index explicit in APIs, e.g. `get(db, key)`, `set(db, key, ...)`, rather than mutating `current_db_`.
- `RespValue`: handlers return typed values; encoding happens once at the protocol boundary. This is required for full RESP3 correctness.

Suggested file split:

- `src/commands/string.cpp`, `hash.cpp`, `list.cpp`, `set.cpp`, `zset.cpp`, `stream.cpp`, `keys.cpp`, `server_commands.cpp`, `scripting_commands.cpp`, `replication_commands.cpp`, `cluster_commands.cpp`, `module_commands.cpp`.
- `src/persistence/aof.cpp`, `src/persistence/rdb.cpp`.
- `src/replication/replication.cpp`.
- `src/pubsub/pubsub.cpp`.
- `src/cluster/cluster.cpp`.

Migration approach:

1. Move code by domain with no behavior changes.
2. Add golden tests around command table output and existing integration suite.
3. Replace global state with `ServerContext` one domain at a time.
4. Replace RESP string concatenation with typed response values.
5. Extract the in-file epoll/kqueue `Poller` into a small poller interface before adding io_uring. This makes each event-loop backend testable and reduces risk in future sharding work.
6. Audit current `thread_local` state. Either make the runtime genuinely thread-aware and protect shared globals, or replace misleading `thread_local` usage with explicit per-context script/module state.

## Priority 4: Improve Data Structures and Expiration Semantics

### Findings

Current structures are simple and functional but not production-grade for Redis-compatible scale:

- Hashes: `std::unordered_map<string,string>`.
- Lists: `std::deque<string>`.
- Sets: `std::unordered_set<string>`.
- Sorted sets: `unordered_map<member,score>` sorted on demand for range/pop operations.
- Streams: vector of entries plus minimal consumer group maps.
- Expiration: per-entry optional timestamp plus fixed-budget active scan.
- `RANDOMKEY` builds a vector of all live keys.
- `SCAN` uses an index-style cursor over unordered_map iteration, which is not Redis-like and is unstable under mutation.
- `Entry` stores containers for all data types regardless of the actual key type, creating avoidable per-key memory and cache overhead.
- Many `DataStore` APIs use `bool& wrongtype`, `bool& ok`, and `std::string& err` out-parameters, which makes error handling easy to forget.

Proposal:

- Add memory accounting first. Without reliable memory accounting, maxmemory, eviction, INFO memory, and production safety cannot be correct.
- Replace the fat `Entry` struct with a tagged payload, preferably `std::variant<StringEntry, HashEntry, ListEntry, SetEntry, ZSetEntry, StreamEntry>`, while keeping expiration metadata outside the payload.
- Replace error out-parameters with an `expected`-style result type for C++20. This makes wrong-type and parse/overflow errors explicit at call sites.
- Replace sorted sets with a dual structure: member hash plus ordered score/member index. A skiplist-compatible model is the safest Redis-parity route.
- Before a full sorted-set rewrite, add an invalidated sorted-vector cache as a tactical improvement for read-heavy `ZRANGE`/`ZRANGEBYSCORE` workloads.
- Replace streams with an ID-ordered radix/tree structure and a real PEL model: idle time, delivery count, consumer tracking, claim/autoclaim semantics.
- Add an expiration index: min-heap, timing wheel, or Redis-like sampled expires dictionary. Keep lazy expiration but make active expiration probabilistic and budgeted by elapsed time.
- As a low-risk interim fix, randomize active-expire scan starting points/buckets instead of always re-walking from the front of each unordered map.
- Implement Redis-like SCAN cursor behavior over hash-table buckets or explicitly document deviations until parity is achieved.
- Use Redis's reverse-binary-increment cursor approach as the target for SCAN rather than sequential indexes over current iteration order.
- Do not attempt custom allocators or NUMA work until memory accounting and data-structure semantics are correct.

## Priority 5: Fix Blocking, Pub/Sub, and Event Loop Semantics

### Findings

- Blocking list/zset/stream commands are rechecked on every poll loop, which can add up to about 100 ms wake latency.
- `notify_key_written()` is currently a no-op.
- `WAIT` blocks the event loop by sleeping inside command handling.
- Pub/Sub delivery exists in code but surface tests only verify subscribe/unsubscribe acknowledgements, not message delivery, ordering, subscriber state restrictions, or pattern behavior.
- Output buffers can grow without Redis-like client output buffer policies.
- `WriteBuf::compact()` erases from the front of a string, which can become an O(n) memmove hot path for large or bursty responses.

Proposal:

- Implement per-db/per-key waiter registries. `LPUSH`, `RPUSH`, `ZADD`, `XADD`, etc. should wake relevant blocked clients immediately.
- Move blocking command timeouts into the event loop timer system.
- Make `WAIT` nonblocking as described in the replication section.
- Coalesce all replies produced by one readable event into the connection `WriteBuf` and drain once, instead of repeatedly writing per command under heavy pipelining.
- Replace string-front erase compaction with ring-buffer/head-tail semantics, or compact only when the consumed prefix dominates the remaining payload.
- Add Pub/Sub tests for delivery, pattern delivery, unsubscribe cleanup on disconnect, command restrictions while subscribed, and `PUBLISH` receiver counts.
- Add output buffer limits and disconnection policies similar to Redis client-output-buffer-limit classes.
- Create a networking abstraction that can support epoll, kqueue, and later io_uring without changing command semantics.

## Priority 6: Observability and Operator Semantics

### Findings

- `INFO` contains placeholder or hardcoded values for several operational fields.
- Memory, hit/miss, expired-key, evicted-key, client age/idle, buffer, and per-DB TTL statistics are not reliable enough for standard Redis dashboards.
- Runtime config behavior is partial, and config-file vs CLI precedence can surprise operators.

### Proposal

- Track counters in the owning domains: keyspace hits/misses, expired keys, evicted keys, rejected calls, failed calls, replication bytes, client input/output buffers, blocked clients, and persistence failures.
- Populate `INFO clients`, `INFO memory`, `INFO stats`, `INFO keyspace`, `INFO persistence`, and `INFO replication` from real state, not placeholders.
- Add tests for common Redis monitoring expectations because many clients and operators use `INFO` as an API.
- Make config precedence Redis-like: config file first, CLI overrides second. Document unsupported config keys as ignored, rejected, or stored-only.

## Priority 7: Clarify Cluster, Sentinel, and Modules Claims

### Cluster

Current implementation is cluster-surface level: hash slots, some `CLUSTER` subcommands, simple redirection state, and a cluster bus listener that replies with a plain `PONG`. This is not Redis Cluster bus compatibility.

Proposal:

- Label current state as "cluster command surface" until binary cluster bus, config epochs, node handshakes, fail states, slot ownership, migration/importing, and mixed Redis cluster interop are implemented.
- Add cluster tests against Redis cluster harness only when the protocol is real enough to interoperate.

### Sentinel

Current `scripts/qa/run_sentinel_validation.sh` validates manual PeaDB primary/replica promotion, not actual Redis Sentinel management.

Proposal:

- Rename the current script or add a true Sentinel test using `redis-sentinel` against PeaDB instances.
- Gate any Sentinel claim on monitor, failover, promotion, INFO replication, role reporting, and timeout behavior validated by real Sentinel.

### Modules

Current module support is a minimal ABI/API subset. The project requirements target binary compatibility with existing Redis modules, which is much stronger.

Proposal:

- Maintain a generated `RedisModule_*` symbol inventory.
- Create test modules for each API group: strings, keys, replies, blocked clients, timers, events, data types, persistence hooks, ACL/user APIs.
- Add certification jobs for RedisJSON, RedisBloom, RedisTimeSeries, and RediSearch only when their required API subsets are implemented.
- Avoid claiming binary compatibility until unmodified published modules load and pass basic operation tests.

## Priority 8: Documentation and Release Hygiene

### Problem

Docs are useful but drifted. This is risky because PeaDB is a compatibility project: incorrect claims cause bad adoption decisions.

### Proposal

- Make README claims generated or checked where possible: command count, command list, known deltas, Redis test pass/fail status.
- Update `FAQ.md` to match current implementation. In particular, reconcile Pub/Sub status, command count, RESP3 scope, replication scope, and date.
- Split docs into two classes:
  - User-facing support matrix: what is safe to depend on.
  - Internal roadmap/architecture notes: what is planned or partial.
- Add a release checklist:
  - All generated compat files updated.
  - Upstream Redis suites run and linked.
  - Differential suite clean or waivers updated.
  - Docker smoke tested.
  - RDB/AOF migration tested both directions.
  - Security limitations reviewed.

## Recommended First Implementation PRs

1. Add hosted CI workflow for build, CTest, command inventory, compat delta validation, and smoke diff tests.
2. Fix `scripts/ci/run_integration_tests.sh` to avoid global `pkill`, resolve `PEADB_BIN` robustly, and use a shared harness with dynamic ports.
3. Fix config precedence so config loads before CLI overrides, with tests.
4. Add generated command inventory and update README/FAQ command counts from it.
5. Add a warning/lint baseline job and formatting config.
6. Add regression tests for current upstream scripting failures from `artifacts/redis-stage-a/unit_scripting.log`.
7. Fix cjson numeric decoding and cmsgpack circular-reference behavior, then rerun Redis `unit/scripting`.
8. Add RDB/AOF roundtrip tests for all supported types, especially streams; fix or explicitly delta-track stream loss.
9. Implement persistent AOF fd plus `appendfsync`, and add `BGSAVE` child reaping/concurrency guard.
10. Make `WAIT` nonblocking or temporarily document that it stalls the event loop; add a regression test proving other clients are not stalled.
11. Add Pub/Sub delivery tests and subscribed-mode command restriction tests.
12. Fix `WriteBuf` compaction and coalesce pipelined replies through the write buffer.
13. Split `src/command.cpp` into domain files without behavior changes.
14. Introduce `ServerContext` and begin moving global replication/persistence/pubsub state into explicit managers.

## Longer-Term Architecture Direction

After the correctness and compatibility gates above are in place, pursue the performance architecture promised in `project-requirements.md`:

- Per-shard single-threaded execution with deterministic key routing.
- Explicit cross-shard command planning for multi-key commands, Lua, transactions, and replication.
- io_uring backend on Linux with epoll fallback.
- Cache-conscious data structures and allocator work guided by memory accounting and benchmark profiles.
- Continuous performance tracking against Redis, including p50/p99/p99.9 latency, not only throughput.

Do not start with sharding or allocator work. The current highest risk is semantic compatibility, not raw throughput.

## Definition of Done for "Redis-Compatible" Claims

A feature should be called Redis-compatible only when all are true:

- It passes targeted integration tests.
- It passes Redis-vs-PeaDB differential cases, including error text and error precedence where applicable.
- It passes relevant upstream Redis Tcl tests or has an explicit waiver in `compat/delta.md`.
- Persistence and replication behavior are covered if the feature mutates data.
- RESP2 and RESP3 encodings are verified where the command supports both.
- Documentation and generated compatibility matrices are updated.

Until then, label the feature as "surface", "partial", or "experimental".
