# PeaDB — Improvement Proposals (Opus Analysis)

_Date: 2026-05-10 (v3 — updated after cross-referencing v2 of improve-auggie.md, improve-codex.md, improve-gpt.md)_  
_Analysed revision: `383130c` (branch `main`)_  
_Context: ~13 300 lines of C++20 across 8 source files + 95+ Python integration tests_

---

## Executive Summary

PeaDB is a functionally solid Redis-7.2.5 clone: RESP2/RESP3, ~207 commands (README says 147 — a discrepancy to fix), replication surface, cluster bus, Lua scripting, persistence, pub/sub, and blocking commands are all present and covered by integration tests. The current architecture is **prototype-grade single-threaded** — which is fine for correctness parity — but several structural and correctness issues must be fixed before any production or multi-core claims are made.

The most dangerous problems are event-loop blockers and silent data loss, not architectural gaps:

1. `WAIT` sleeps inside the command handler → freezes *all* clients during its duration
2. Streams are silently dropped from RDB saves → data loss on restart
3. AOF opens + closes the file on every write with no `fsync` → slow and not durable
4. `BGSAVE` forks without `waitpid` → two concurrent BGSAVEs corrupt the dump file
5. PSYNC always does a full resync → partial resync is not implemented despite the flag name

Fix these before any architecture work.

---

## Correction from Initial Analysis

**§6 (Inline command parsing) is removed.** `src/protocol.cpp:75–101` already implements `parse_inline`, dispatched when the first byte is printable and not `*`. That gap does not exist.

**Pub/Sub status discrepancy:** `src/command.cpp:155–265` has a working subscriber registry (`g_channel_subscribers`, `g_pattern_subscribers`, `pubsub_publish` with `g_pubsub_send` delivery via `fnmatch`). However, `FAQ.md` lines 31 and 247 and `compat/delta.md` still describe pub/sub as a stub where `PUBLISH` always returns 0. Either the docs are stale or the server-side wiring in `g_pubsub_send` is incomplete. One PR should reconcile this.

**README says ~147 commands; `command_table()` registers 207.** The coverage gate passes because it checks name text presence, not execution. Fix both counts and the gate.

---

## Critical Bugs (Fix Before Anything Else)

### C1. `WAIT` Blocks the Entire Event Loop

`src/command.cpp` WAIT handler sleeps inside `handle_command()` — the single-threaded event loop thread. Every connected client is frozen for the entire sleep duration. This is not a latency issue; it is an availability issue.

**Fix:** Register a wait state in `SessionState` (like `BlockedState`) with a deadline and a target offset. From the server loop, after each replication ACK arrives, check all sessions in WAIT state, count how many replicas have acked ≥ target offset, and unblock if satisfied or timed out. ~80 lines, same pattern as BLPOP.

### C2. Streams Silently Dropped from RDB

`src/rdb.cpp` skips stream keys during `rdb_build_string()`. Any key of type `Stream` is silently omitted from the snapshot. A `SAVE` + restart drops all stream data with no error message. This contradicts the README's persistence claims.

**Fix:** Implement stream RDB encoding in `rdb.cpp`. Until that is ready, `BGSAVE`/`SAVE` must log a warning and `INFO persistence` must report stream keys as unsaved.

### C3. AOF: File Opened and Closed Per Command, No fsync

`src/command.cpp:627–633` opens the AOF file with `std::ofstream`, writes one RESP-encoded command, and closes it — on every mutating command. There is no `fsync`/`fdatasync` anywhere.

**Fix:** Hold a persistent `int aof_fd` opened once with `O_APPEND|O_CLOEXEC`. Introduce `appendfsync = always|everysec|no` (default `everysec` to match Redis). A 1 Hz timer in the event loop calls `fdatasync()` for the `everysec` mode. ~80 lines, no API changes.

### C4. BGSAVE Forks Without `waitpid` — Concurrent Saves Corrupt Dump

The BGSAVE handler `fork()`s but never calls `waitpid`. Two consecutive `BGSAVE` invocations spawn two children writing the same `dump.rdb` simultaneously, corrupting the file. `INFO persistence` cannot report correct status.

**Fix:** Add `std::atomic<pid_t> g_bgsave_child_pid{0}`. On each event-loop tick, call `waitpid(g_bgsave_child_pid, …, WNOHANG)`. Reject overlapping `BGSAVE` with `-ERR Background save already in progress` (Redis-canonical). Expose `rdb_bgsave_in_progress`, `rdb_last_bgsave_status`, `rdb_last_bgsave_time_sec` in `INFO persistence`.

### C5. `BGREWRITEAOF` Is Synchronous

Despite the background name, `BGREWRITEAOF` runs synchronously, blocking the event loop during the full rewrite. Add a `fork()` + temp-file + atomic rename pattern, matching BGSAVE.

### C6. `REPLICAOF` Shells Out to Python — Fragile and a Security Risk

`REPLICAOF` calls `std::system("python3 scripts/redis/sync_from_redis.py …")` for the initial full sync. The host/port parameters from the command are composed into a shell string, which is a potential command injection vector if argument sanitization is incomplete. Beyond the security angle: it requires Python on the server host, breaks if the script path changes, and the replication stream after the snapshot is incomplete (no continuous command streaming).

**Fix:** Implement an in-process replication client that connects to the master, sends `PSYNC`, reads the RDB payload into the local store, then enters a streaming loop consuming replication commands — no Python, no shell, no injection surface.

### C7. Atomic Rename Missing for RDB/AOF Writes — Crash Corrupts Snapshots

The RDB save path and `BGREWRITEAOF` write directly to the destination file. A crash or power loss mid-write leaves a truncated `dump.rdb` or `appendonly.aof` that the next startup will reject or partially replay, causing data loss or corruption.

**Fix:** Write to `dump.rdb.tmp` (same directory, same filesystem), call `fsync` on the temp file, then `rename(2)` over the destination, then `fsync` the directory fd. ~30 lines; eliminates an entire class of corruption bugs.

### C8. PSYNC Always Does Full Resync

Despite the `PSYNC2` label, the server always performs a full resync (no backlog, no partial resync path). The replication event buffer is an unbounded `std::vector<std::string>` — it grows without limit and cannot support offset-based partial resync.

**Fix:** Replace `g_replication_events` with a bounded circular backlog keyed by byte offsets. Implement PSYNC2 partial resync logic: check replid, replid2, requested offset against backlog availability; reply `+CONTINUE` or `+FULLRESYNC` accordingly.

---

## 1. Split `command.cpp` — Highest Development-Velocity Impact

**Problem.** `src/command.cpp` is 6 720 lines containing every Redis command implementation, ~40 file-scope globals, the replication event buffer, pub/sub state, Lua integration, module loading, cluster slot routing, and AOF appending — all in one anonymous namespace. A single whitespace edit forces a full recompile of 6 700 lines.

**Proposed structure:**
```
src/
  commands/
    string_commands.cpp
    hash_commands.cpp
    list_commands.cpp
    set_commands.cpp
    zset_commands.cpp
    stream_commands.cpp
    pub_sub_commands.cpp
    script_commands.cpp      # EVAL EVALSHA FUNCTION FCALL …
    transaction_commands.cpp # MULTI EXEC DISCARD WATCH …
    cluster_commands.cpp
    server_commands.cpp      # INFO CONFIG DEBUG COMMAND …
    replication_commands.cpp # PSYNC REPLCONF REPLICAOF …
    module_commands.cpp      # MODULE LOAD/LIST/UNLOAD + module registry
  command_dispatch.cpp       # CommandSpec table + handle_command()
  replication.cpp            # replication event buffer, offset tracking
  pub_sub.cpp                # channel/pattern maps, pubsub_publish
  persistence/
    aof.cpp
    rdb.cpp                  # consolidate from src/rdb.cpp
```

**How to migrate safely:** Move command handler lambdas one group at a time; run `scripts/ci/run_integration_tests.sh` after each move. Add each new TU to `CMakeLists.txt` inside the existing `peadb_core` library.

**Sequencing caveat:** Splitting `command.cpp` before resolving the `thread_local` audit (§13) and the `DataStore::current_db_` global (§5) risks locking in the wrong inter-file dependency graph — the split files would still share the same broken shared-state model. Consider doing §5 and §13 first, then splitting. The build-time benefit still arrives; the risk of doing it in the wrong order is that the split needs to be re-done once the state model changes.

**Payoff:** Incremental rebuilds drop from ~30 s to 1–3 s per changed command group; each domain file can get its own unit test.

---

## 2. Replace the `Entry` Fat-Struct with `std::variant` — Memory Efficiency

**Problem.** `include/datastore.hpp:25–43` — measured `sizeof(peadb::Entry) == 304 bytes` before any dynamic allocations. Every key carries six empty containers regardless of its actual type:

```cpp
struct Entry {
  ValueType type;
  std::string value;
  std::unordered_map<std::string, std::string> hash_value;
  std::deque<std::string> list_value;
  std::unordered_set<std::string> set_value;
  std::unordered_map<std::string, double> zset_value;
  std::vector<...> stream_entries;
  std::unordered_map<std::string, StreamGroup> stream_groups;
  bool string_force_raw;   // command-layer concern leaking into datastore — see §4
  …
};
```

On a 100 M-key dataset this wastes several GiB.

**Proposed replacement:**
```cpp
struct StringEntry  { std::string value; };
struct HashEntry    { std::unordered_map<std::string, std::string> fields; };
struct ListEntry    { std::deque<std::string> elements; };
struct SetEntry     { std::unordered_set<std::string> members; };
struct ZSetEntry    { std::unordered_map<std::string, double> scores; };
struct StreamEntry  { /* current stream fields */ };

using EntryValue = std::variant<StringEntry, HashEntry, ListEntry, SetEntry, ZSetEntry, StreamEntry>;

struct Entry {
  EntryValue value;
  std::optional<std::int64_t> expire_at_ms;
};
```

**Payoff:** 50–70% memory reduction for typical workloads; `ValueType` is replaced by `std::holds_alternative<>`; the seven `is_wrongtype_for_*` methods collapse into one template.

---

## 3. Replace `bool& wrongtype` Out-Parameters with `std::expected` — Type Safety

**Problem.** The DataStore API uses 30+ methods with `bool& wrongtype` / `bool& ok` / `std::string& err` output parameters. Forgetting to check `wrongtype` is a silent bug.

**Proposed:** Use C++23 `std::expected<T, RedisError>` (or a thin equivalent for C++20):
```cpp
enum class RedisError { WrongType, OutOfRange, NaN, … };
std::expected<std::int64_t, RedisError> lpush(const std::string& key, const std::vector<std::string>& values);
```

**Payoff:** Errors cannot be ignored at compile time; removes ~60 local `bool wrongtype` declarations.

---

## 4. Typed `RespValue` Return — Encode Once at the Protocol Boundary

**Problem.** Every command handler builds its response by string-concatenating RESP protocol bytes (e.g., `"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"`). This means:
- RESP2 vs RESP3 encoding differences must be handled per-handler
- Replication event encoding re-encodes the same data a second time (double-encode for offset tracking: `src/command.cpp:6540–6542`)
- Any future protocol (inline introspection, custom serialization) requires touching every handler

**Proposed:** Handlers return a typed `RespValue` tree:
```cpp
using RespValue = std::variant<RespNull, RespInt, RespBulk, RespArray, RespMap, RespError>;
```
The server loop calls a single `encode_resp(val, session.resp_version)` at the boundary. Replication gets the canonical byte count from that encoding without re-encoding.

**Payoff:** Correct RESP3 map/set/double/blob-error types for free; eliminates double-encoding; simplifies handler code.

---

## 5. Encapsulate Global State in `command.cpp` — Testability and Sharding Prerequisite

**Problem.** ~40 globals in the anonymous namespace of `command.cpp` are unreachable from unit tests and block sharding. Introduce explicit runtime objects:

```cpp
struct ReplicationState {
  std::string role = "master";
  std::string master_replid;
  std::int64_t master_repl_offset = 0;
  /* bounded circular backlog */ …
};

struct ClusterState {
  std::array<SlotRoute, 16384> slot_routes {};
  std::string redirect_addr;
  std::set<std::string> peers;
};

struct ServerContext {
  ReplicationState replication;
  ClusterState cluster;
  PubSubState pubsub;
  ModuleRegistry modules;
  ScriptState scripts;
  PersistenceState persistence;
};
```

Also: `DataStore::current_db_` is a mutable global switched before each command dispatch, hostile to sharding and deterministic multi-client testing. Make DB index explicit in all APIs: `get(db_index, key)`, `set(db_index, key, …)`.

---

## 5a. Coalesce Pipelined Replies — 2–4× Throughput Win

**Problem.** Under pipelining (a client sends 100 commands in one TCP segment), the server emits up to 100 separate `write()` syscalls or EPOLLOUT cycles. The `WriteBuf` per connection already exists in `server.cpp` to buffer output, but replies are still flushed per-command rather than being accumulated across the entire batch of commands parsed from one `read()` event.

**Fix:** After the parse loop at `server.cpp:621–632`, ensure `enqueue_output` defers the actual write syscall until the loop exits and all replies for that `read()` are batched. This is the same change Redis made and typically yields 2–4× throughput under pipelining with zero semantic change.

---

## 5b. `WriteBuf::compact` Is O(n) — Latency Spike on Large Responses

**Problem.** `server.cpp:161–163` does `data.erase(0, offset)` — an O(n) `memmove` of all remaining bytes whenever the buffer is compacted. Under bursty `LRANGE` / `HGETALL` patterns this dominates per-loop work.

**Fix:** Compact only when `offset > data.size()/2`, or replace with a ring buffer / head-tail index pair that makes compaction O(1). The existing `offset > 65536` threshold is already a partial guard — tightening the condition eliminates the worst-case quadratic path without changing the API.

---

## 6. SCAN Cursor Instability — Correctness Gap

**Problem.** `datastore.cpp scan()` uses a sequential integer index into `std::unordered_map`. Any insert that triggers a rehash invalidates iteration order, causing clients to miss keys or receive duplicates. Applications using `SCAN` against a mutating dataset get incorrect results.

**Fix:** Implement Redis's reverse-binary-increment cursor over the bucket array (~150 lines, described by Antirez in the Redis SCAN design blog post). This is a **correctness** fix for the "drop-in replacement" claim, not an optimization.

---

## 7. Blocking-Command Wakeup — `notify_key_written` Is a No-Op

**Problem.** `notify_key_written` is declared in `command.hpp:68` but is a no-op. The blocked-client check in `server.cpp` polls every blocked session on every loop iteration (100 ms tick), meaning `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`XREADGROUP` can have up to 100 ms wake latency after the key is written.

**Fix:** Maintain a `std::unordered_map<std::string, std::vector<int>> key_waiters` registry. `LPUSH`, `RPUSH`, `ZADD`, `XADD` call `notify_key_written(key, db)` which looks up waiters and marks their sessions ready immediately (within the same iteration). This matches Redis's O(1) wake-up semantics.

---

## 8. Active Expiry Correctness — Always Iterates from `begin()`

**Problem.** `active_expire_cycle(64)` always re-walks the first 64 entries of `unordered_map`. With a steady stream of inserts, expired keys at the tail are reclaimed only on lazy access. This causes unbounded memory growth under a workload of frequent short-TTL inserts with sequential keys.

**Fix:** Use `std::uniform_int_distribution` to select a random starting bucket on each cycle, or maintain a separate expiration min-heap / timing wheel keyed by expiry timestamp — the more correct long-term solution.

---

## 9. ZSet Range Queries — O(N log N) Per Call

**Problem.** `zrange`/`zrangebyscore`/`zrangebylex` copy and sort the entire `std::unordered_map<string,double>` on every call. A sorted set with 100 K members re-sorts 100 K elements on every `ZRANGE` call.

**Immediate fix (no data structure change):** Cache a `std::vector<std::pair<string,double>>` sorted by score per ZSet entry; invalidate it on the entry's `mutation_epoch` change. O(1) reads after the first sort per mutation.

**Correct fix:** Replace with a dual structure: member hash for O(1) score lookup + skip-list or ordered index for range queries. This is the Redis internals design.

---

## 9b. `RANDOMKEY` Is O(N) — Expensive on Large Keyspaces

**Problem.** `DataStore::randomkey()` builds a `std::vector` of all live keys in the current DB before sampling one. On a DB with millions of keys this is a full heap allocation + scan on every call, and it also triggers lazy expiration on every key it touches — making repeated `RANDOMKEY` calls accidentally perform active expiration as a side effect, causing unpredictable latency spikes.

**Fix:** Track a reservoir sample or maintain a secondary random-access index into the bucket array. A simple improvement: walk `unordered_map` buckets starting from a random offset and stop after the first non-expired key — O(1) amortised when the load factor is healthy.

---

## 10. Config Precedence Is Backwards

**Problem.** `src/main.cpp` parses CLI args first, then config-file values overwrite them. Redis behavior is the opposite: config file is baseline, CLI args are overrides (`redis-server redis.conf --port 7000` → port 7000 wins). Existing Redis deployment scripts that pass both will behave unexpectedly.

**Fix:** Parse config file first, apply CLI args second. Add a test for precedence behavior.

---

## 11. Replication Event Buffer Is Unbounded

`g_replication_events` is `std::vector<std::string>` that grows without limit for the lifetime of the process. For a long-running master with many writes this is an OOM vector. Redis uses a fixed-size circular backlog (default 1 MB). Replace with a bounded ring buffer keyed by byte offset; this is also the prerequisite for partial resync (see C7).

---

## 11b. Two Separate `mutation_epoch` Counters — `INFO persistence` Is Wrong

**Problem.** There are two independent mutation counters:
- `DataStore::mutation_epoch_` at `include/datastore.hpp:176` — incremented by DataStore methods
- `g_mutation_epoch` at `src/command.cpp:65` — incremented by the command layer

`INFO persistence`'s `rdb_changes_since_last_save` is computed using one counter while some write paths increment only the other. The result is that save-needed detection and change counts are unreliable: `BGSAVE` may be triggered unnecessarily or not triggered when it should be.

**Fix:** Pick one counter, delete the other, add a unit test asserting it increments exactly once per write command. The DataStore-owned counter is the correct place since it has direct visibility into all mutations.

---

## 12. Memory Accounting Is Missing

Without reliable per-key memory accounting, the following cannot work correctly:
- `maxmemory` enforcement
- All eviction policies (allkeys-lru, volatile-lru, etc.)
- `INFO memory` fields (`used_memory`, `used_memory_rss`, `mem_fragmentation_ratio`)
- Module API calls that report memory usage

**Fix:** Track allocated bytes per key in `DataStore` using a simple size estimator (string length + container overhead constants). Add `std::atomic<std::int64_t> g_used_memory` incremented/decremented on each mutation. This is not allocator-level accounting but is sufficient for policy enforcement and INFO reporting.

---

## 13. `thread_local` on a Single-Threaded Server — Misleading Invariant

`g_in_lua_script`, `g_script_readonly_context`, `g_script_client_resp_version`, `g_script_current_session`, `g_module_dma_buf`, `g_script_allow_oom`, and `g_script_inner_error_seen` are all `thread_local`. The code is single-threaded except for the `REPLICAOF` bootstrap thread. Meanwhile the non-`thread_local` globals (`g_master_repl_offset`, `g_replication_events`, the cmdstats maps) have no thread safety at all.

**Fix:** Decide: either commit to truly single-threaded execution for these (remove `thread_local`, pass context explicitly), or add proper synchronization around all shared globals accessed from the bootstrap thread.

---

## 14. Output Buffer Limits Missing

Clients can receive arbitrarily large responses (`LRANGE key 0 -1` on a million-element list) with no cap. Redis enforces `client-output-buffer-limit` with per-class (normal/pubsub/replica) soft/hard limits and disconnects slow consumers. Without this, a slow client accumulates a `WriteBuf` that grows until OOM.

**Fix:** Track `wbuf.remaining()` per client in the server loop. When it exceeds a configurable hard limit (default 256 MB for normal clients, matching Redis), close the connection with `CLIENT_CLOSE_ASAP`. Add a soft limit that triggers after a sustained period.

---

## 15. Cluster and Sentinel Claims Need Scoping

**Cluster bus:** The current implementation accepts connections on port N+10000 and replies `PONG` to any data. The real Redis cluster bus protocol (CLUSTER_NODE_HANDSHAKE, config epochs, MEET/PING/PONG gossip messages with specific binary format, fail reports) is not implemented. Current state should be labeled "cluster command surface" — `CLUSTER INFO/NODES/SLOTS/KEYSLOT` etc. work, but binary cluster bus interop does not.

**Sentinel:** `scripts/qa/run_sentinel_validation.sh` tests manual primary/replica promotion, not actual Redis Sentinel management. A real Sentinel test would start `redis-sentinel` pointing at PeaDB instances and validate monitor, failover election, and promotion behavior. Gate any Sentinel claim on that.

---

## 16. Compatibility Scoreboard — Generated, Not Stated

**Problem.** Feature claims in `README.md` are written by hand and drift from reality. The command coverage gate (`check_command_test_coverage.py`) checks that a command name appears somewhere in `tests/`, not that it is actually executed. Existing Redis Tcl artifacts show real failures: `artifacts/redis-stage-a/failed_suites.txt` lists `unit/scripting`; `unit_scripting.log` shows cjson numeric decoding and cmsgpack circular-reference failures.

**Proposed generated artifacts committed to `compat/`:**

| File | Content |
|------|---------|
| `compat/commands.generated.md` | All 207 commands with arity, flags, test owner, status (full/partial/stub) |
| `compat/redis-tests.generated.md` | Each upstream Redis Tcl suite: run/pass/fail/skip + reason |
| `compat/differential.generated.md` | Differential case coverage per command |
| `compat/modules.generated.md` | `RedisModule_*` symbols exported + certified module status |

**Immediate fix:** Update the coverage gate to require at least one actual *execution* of each command against PeaDB in the test suite, not just a name mention. Commands with only partial coverage get an explicit entry in `compat/delta.md`.

---

## 17. Existing Scripting Failures — Fix Before Expanding

`artifacts/redis-stage-a/unit_scripting.log` shows specific failures:
- cjson numeric decoding diverges from Redis (likely integer/float distinction)
- cmsgpack circular-reference behavior differs

These should be the first scripting fixes because they are already documented with repro data. The minimal `lua_cjson_stub.c` and `lua_cmsgpack_stub.c` may need to be replaced with the Redis-vendored versions.

---

## 18. No CI Configured

`.github/` is empty. The 9-stage `scripts/run_all_tests.sh` pipeline runs only when a developer invokes it locally. Every new PR is unvalidated automatically.

**Proposed CI tiers:**

| Tier | Trigger | Contents |
|------|---------|---------|
| 1 | Every PR | Build (Linux), CTest, command inventory generation, compat delta check, smoke diff tests |
| 2 | Every PR (if Redis available) | Python integration suite with dynamic ports + process-group cleanup |
| 3 | Nightly | Redis Tcl selected suites, ASAN/UBSAN, fuzz sanity, perf smoke |
| 4 | Release gate | Full Redis Tcl matrix, Sentinel validation, client-lib certification, RDB/AOF roundtrip corpus, module certification |

Also fix: `scripts/ci/run_integration_tests.sh` uses `pkill -9 -f peadb-server`, which kills unrelated processes on shared dev hosts. Replace with per-test PID tracking + process-group signals.

---

## 19. Add More Unit Tests to CTest

Only 2 C++ unit tests exist. Adding unit tests that call `handle_command(args, session, should_close)` against a fresh `DataStore` without a socket:
- Dispatch table arity validation
- MULTI/EXEC abort paths
- EXPIRE option matrix (NX/XX/GT/LT flags)
- OBJECT ENCODING policy
- ZADD flag combinations (NX/XX/GT/LT/INCR interactions)
- Config precedence behavior (§10)
- `mutation_epoch` increments exactly once per write command (§11b)

**Command contract tests (separate):** Snapshot-assert each `CommandSpec`'s arity, flag bits, and key positions. This catches accidental regressions in command metadata without running a live server — and is also the foundation for a generated compat scoreboard (§16). Format: a golden JSON/text file committed alongside the test; CI fails if the snapshot diverges.

Each is <100 lines and runs in <10 ms.

---

## 20. Add `.clang-format` + Compiler Warning Gate

Six source files vary in include order and brace style. Compiler warnings: building with `-Wall -Wextra -Wpedantic` surfaces 12 warnings on the current codebase.

**Fix:**
1. Add `.clang-format` based on the existing dominant style; add a CI lint gate.
2. Add `-Wall -Wextra` to `CMakeLists.txt` target compile options; start with non-fatal reporting, promote to `-Werror` once the 12-warning backlog is cleared.

---

## 21. Bundle Lua 5.1 Source

`CMakeLists.txt` caps optimization at `-O2` on macOS to dodge a system-Lua miscompile. Bundling Lua 5.1.5 (~24K LoC, MIT) removes the cap, removes the apt/brew dependency for new contributors, and makes release builds comparable across platforms.

---

## 22. INFO Observability Gaps

Several INFO subsections produce placeholder values:
- `clients`: `qbuf`, `qbuf-free`, `age`, `idle` hardcoded to 0
- `memory`: `used_memory_rss`, `mem_fragmentation_ratio` missing (blocked until §12)
- `stats`: `expired_keys`, `evicted_keys`, `keyspace_misses`, `keyspace_hits` not tracked
- `keyspace`: only reports `dbsize`, not `keys=N,expires=M,avg_ttl=T` per-db
- `replication`: `connected_slaves:0` even when replica sessions are tracked in `sessions`

Add `std::atomic<std::int64_t>` counters in `DataStore` for expired/evicted/hit/miss events; update them in `expire_if_needed` and `get`. Wire replica count from the server loop into `g_connected_replicas`.

---

## 23. Definition of Done for Compatibility Claims

A feature should not be called "Redis-compatible" in docs unless ALL are true:

1. It passes targeted integration tests.
2. It passes at least one Redis-vs-PeaDB differential case (or has a waiver in `compat/delta.md`).
3. It passes relevant upstream Redis Tcl tests, or has an explicit skip reason.
4. Persistence and replication behavior are covered if the feature mutates data.
5. RESP2 and RESP3 encodings are verified where the command supports both.
6. Generated compatibility matrices are updated.

Until all five are true, label the feature as `surface`, `partial`, or `experimental` in `compat/commands.generated.md`.

---

## 24. io_uring Backend — Architecture Roadmap

**Problem.** `project-requirements.md §8.2` mandates `io_uring` as primary I/O backend. ADR `docs/decisions/d0-decisions.md §D0.3` pins minimum kernel at Linux 5.15 with liburing 2.4+. Currently only epoll/kqueue exists.

**Prerequisite:** Extract the `Poller` struct in `server.cpp:62–148` into `include/poller.hpp` as a strategy template or virtual interface (Auggie §4.3). This is the prerequisite for io_uring and costs ~2 hours.

**Phased approach:**
- Phase A: Extract `Poller` to `include/poller.hpp` (prerequisite, free payoff)
- Phase B: io_uring recv/send for client connections
- Phase C: io_uring for persistence I/O (eliminates BGSAVE event-loop stall)

**CMakeLists.txt addition:**
```cmake
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING liburing>=2.4)
if(LIBURING_FOUND)
  target_link_libraries(peadb_core PUBLIC ${LIBURING_LIBRARIES})
  target_compile_definitions(peadb_core PUBLIC PEADB_HAVE_IO_URING)
endif()
```

---

## 25. Sharded Execution Model — Architecture Goal

`project-requirements.md §8.1` specifies sharded multi-core execution. This is a large undertaking with prerequisites:

**Prerequisites (must land first):** §1 (split command.cpp), §5 (encapsulate state), §2 (Entry variant), §24 Phase A (Poller abstraction), and making `DataStore::current_db_` explicit in all APIs.

**Design:** Listener thread distributes connections by `fd % N_SHARDS`. Each shard is a self-contained event loop + DataStore slice. Cross-shard commands go through a lock-free SPSC queue. MULTI/EXEC and Lua scripts must execute entirely on one shard. The replication stream must be globally ordered.

**Caution:** Do not start sharding until correctness gates from §C1–C7, §6, §7, §8 are solid. The biggest risk to correctness is semantic compatibility, not raw throughput.

---

## Summary Table

| # | Area | Effort | Impact | Risk | Prerequisite |
|---|------|--------|--------|------|-------------|
| C1 | WAIT event-loop block | Low | Critical | Low | — |
| C2 | Streams dropped from RDB | Medium | Critical | Low | — |
| C3 | AOF open/close per cmd + no fsync | Low | High | Low | — |
| C4 | BGSAVE no waitpid | Low | High | Low | — |
| C5 | BGREWRITEAOF synchronous | Low | Medium | Low | — |
| C6 | REPLICAOF shells out to Python (security) | High | High | Medium | — |
| C7 | Atomic rename missing for RDB/AOF | Low | High | Low | — |
| C8 | PSYNC always full resync | High | High | Medium | C6, 11 |
| 1 | Split command.cpp | Medium | High (DX) | Low | — |
| 2 | Entry → std::variant | Medium | High (memory) | Medium | — |
| 3 | std::expected API | Medium | High (safety) | Low | — |
| 4 | Typed RespValue | Medium | High (correctness) | Medium | 1 |
| 5 | Encapsulate global state + DB index | Medium | High (testability) | Low | 1 |
| 5a | Coalesce pipelined replies | Low | High (throughput) | Low | — |
| 5b | WriteBuf O(1) compaction | Low | Low | Low | — |
| 6 | SCAN cursor stability | Low | High (correctness) | Low | — |
| 7 | notify_key_written wakeup | Low | High (latency) | Low | — |
| 8 | Active expiry randomization | Low | Medium | Low | — |
| 9 | ZSet range caching/skiplist | Medium | Medium | Low | — |
| 9b | RANDOMKEY O(N) per call | Low | Medium | Low | — |
| 10 | Config precedence fix | Low | Medium (ops) | Low | — |
| 11 | Bounded replication backlog | Medium | High (correctness) | Medium | — |
| 11b | Two mutation_epoch counters | Low | Medium (correctness) | Low | — |
| 12 | Memory accounting | Medium | High (ops) | Medium | — |
| 13 | thread_local audit | Low | Low | Low | — |
| 14 | Output buffer limits | Low | Medium (safety) | Low | — |
| 15 | Cluster/Sentinel claims scoped | Low | Medium (docs) | Low | — |
| 16 | Compat scoreboard | Medium | High (confidence) | Low | — |
| 17 | Fix scripting Tcl failures | Medium | High (compat) | Low | — |
| 18 | CI workflow | Medium | High (safety net) | Low | — |
| 19 | More unit tests | Low | Medium | Low | — |
| 20 | clang-format + -Werror | Low | Low (hygiene) | Low | — |
| 21 | Bundle Lua 5.1 | Low | Low (DX) | Low | — |
| 22 | INFO observability | Low | Medium (ops) | Low | 12 |
| 23 | Compat Definition of Done | Low | High (process) | Low | 16 |
| 24 | io_uring backend | High | High (perf) | Medium | 24A |
| 25 | Sharded execution | Very High | Very High (perf) | High | 1,2,5,24 |

**Recommended sequencing:**

1. **Week 1 (stop the bleeding):** C1 WAIT, C3 AOF fd+fsync, C4 BGSAVE waitpid, C5 BGREWRITEAOF, C7 atomic rename, §6 SCAN cursor, §10 config precedence, §18 CI workflow skeleton, docs reconciliation (pub/sub 147→207)
2. **Week 2–3 (reliability + signal):** C2 stream RDB (loud-fail first), C6 REPLICAOF in-process, §5a reply coalescing, §5b WriteBuf compaction, §7 notify_key_written, §8 active expiry, §9b RANDOMKEY, §11b mutation_epoch, §14 output buffer limits, §16 compat scoreboard, §17 scripting failures, §22 INFO counters
3. **Next milestone (architecture foundation):** C8 PSYNC partial resync + backlog, §13 thread_local audit, §5 encapsulate state + db-explicit API (these two unlock §1), §1 split command.cpp, §4 typed RespValue, §12 memory accounting, §19 command contract tests
4. **Architecture milestone (performance):** §2 Entry variant, §3 std::expected, §9 ZSet skiplist, §24 io_uring, §25 sharding

**Key sequencing constraint (from Auggie v2):** Split `command.cpp` (§1) only *after* the `thread_local` audit (§13) and `DataStore::current_db_` explicit-db refactor (§5) are done. Splitting first locks in the wrong inter-file dependency graph and the split would need to be re-done once state ownership changes.
