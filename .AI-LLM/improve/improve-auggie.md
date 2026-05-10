# PeaDB — Improvement Proposals (Auggie)

_Date: 2026-05-10 (v2 — incorporates cross-review of `improve-codex.md`, `improve-gpt.md`, `improve-opus.md`)._
_Scope: source tree at HEAD (12.5K LoC across `src/` + `include/`); 207 registered commands; 95 Python integration tests, 2 C++ unit tests._
_Complements the other three docs; does not duplicate Opus's architectural items (split `command.cpp`, `Entry`→`std::variant`, `std::expected`, sharding, io_uring) — those are linked in §6._

---

## Verification notes vs. existing docs

Two claims drift between code and docs; fixing the docs is itself a quick win:

- `src/protocol.cpp:75–101` already implements **inline command parsing** (`parse_inline`, dispatched when the first byte is printable and not `*`). Opus §6 lists this as a missing fix; it isn't. The drift to address is in `FAQ.md` if anything (it correctly notes inline support).
- `src/command.cpp:155–265` implements a **working Pub/Sub registry** (`g_channel_subscribers`, `g_pattern_subscribers`, `pubsub_publish` with `g_pubsub_send` callback delivery, `fnmatch`-based pattern routing). `FAQ.md` lines 31, 247, and `compat/delta.md` still describe Pub/Sub as a "stub — `PUBLISH` always returns 0". Either the FAQ is stale or the wiring from `g_pubsub_send` to per-fd write buffers in `server.cpp` is incomplete — verify and reconcile in one PR.

Action: a single source of truth for compat status. `compat/delta.md` is the natural home; `FAQ.md` and `README.md` should reference it, not restate it. Codex and gpt both observe `README.md` says "~147 commands" while `command_table()` registers 207 — generating the count from the table closes this drift permanently (see §2.7).

---

## Cross-review additions (credit & index)

Concrete items raised by codex/gpt that I missed in v1 — each is folded into the tiers below; this section is just an index.

- `WAIT` sleeps inside the command handler, stalling **all** clients (codex P0, gpt 2.2) → §1.6.
- `src/rdb.cpp` silently drops streams during normal RDB save (gpt 2.1) → §1.7.
- No atomic-rename for RDB/AOF rewrites; power-loss corrupts the file (gpt 2.1) → §1.8.
- Config precedence is reversed: CLI parsed first, config file overwrites it (codex P0, `src/main.cpp`) → §1.9.
- `PSYNC` is always full-resync; `SYNC` returns empty RDB; `g_replication_events` is unbounded (gpt 2.2) → §3.5.
- `INFO replication` reports `connected_slaves:0` despite tracking replicas (gpt 2.2) → §3.5.
- Cluster bus listener replies plain `PONG`, not the binary cluster bus (gpt 6) → §3.6.
- 12 compiler warnings under `-Wall -Wextra -Wpedantic` (codex 1.3); no warning gate → §2.4.
- Pre-existing Redis Tcl `unit/scripting` failures in `artifacts/redis-stage-a/` (cjson numeric decoding, cmsgpack circular refs) (gpt 2.3) → §3.7.
- `DataStore::mutation_epoch_` is separate from the command-layer counter, breaking `INFO persistence` change counts (gpt 2.1) → §3.8.
- `current_db_` global mutated per-command — hostile to sharding/testing (gpt 3) → §5.4.
- No `client-output-buffer-limit` (gpt 5) → §5.5; no memory accounting (gpt 4) → §5.6.
- Destructive `pkill -9 -f peadb-server`, hardcoded ports, text-only coverage gate, no JUnit XML (codex+gpt) → §2.5–§2.7.
- `sizeof(peadb::Entry) == 304` bytes before dynamic allocations (codex 1.4) — the hard number that justifies Opus §2's `std::variant`.

---

## Tier 1 — High-impact, low-risk fixes (days, not weeks)

### 1.1 AOF: persistent fd + `appendfsync` (durability gap)
`src/command.cpp:627–633` opens the AOF file with `std::ofstream`, writes one RESP-encoded command, and closes it — **on every mutating command**. There is no `fsync`/`fdatasync` anywhere. This is both a perf disaster (open+close syscalls per write) and a durability claim issue (`FAQ.md` flags it; the requirements demand Redis parity).

Fix: hold a persistent `int aof_fd` opened with `O_APPEND|O_CLOEXEC`; introduce `appendfsync = always|everysec|no` (default `everysec` to match Redis); a 1 Hz timer in the event loop calls `fdatasync()` for `everysec`. ~80 lines, no API changes.

### 1.2 Coalesced replies per event-loop iteration
`server.cpp` already carries a `WriteBuf` per connection (lines 151–164), but `FAQ.md` (and a quick read of the dispatch loop) confirms responses are written per-command. Append all pipelined replies into the per-fd `WriteBuf`, register `EPOLLOUT`/`EVFILT_WRITE` only when non-empty, and drain once per loop. This is the same one-line change Redis ships with and typically yields 2–4× throughput under pipelining without altering semantics.

### 1.3 Stop double-encoding for replication offset
`src/command.cpp:6540–6542` currently calls `encode_command_resp_size(args)` *and* `append_replication_event(args, …)` (which re-encodes). `FAQ.md:805` calls this out. Make `append_replication_event` return the byte count it wrote and increment the offset from that.

### 1.4 `WriteBuf::compact` is O(n)
`server.cpp:160–163` does `data.erase(0, offset)`. Under bursty `LRANGE` / `HGETALL` patterns this dominates. Replace with a small ring buffer or, simpler, `memmove` only when `offset > data.size()/2` — eliminates the worst-case quadratic behaviour without changing the API.

### 1.5 BGSAVE child reaping & concurrency guard
`src/command.cpp` BGSAVE handler `fork()`s without `waitpid` (FAQ:789–791). Two consecutive BGSAVEs spawn two children writing the same `dump.rdb` concurrently. Add `g_bgsave_child_pid` (atomic) and a non-blocking `waitpid(WNOHANG)` from the loop tick; reject overlapping `BGSAVE` with `-ERR Background save already in progress` (Redis-canonical error).

### 1.6 `WAIT` must not block the event loop _(from codex/gpt)_
Today the `WAIT` handler polls in a sleep loop inside command dispatch — every other connected client blocks until WAIT returns. Convert it to the same machinery as blocking list ops: register a `WaitState{ target_offset, deadline_ms, session* }` in a global registry, return immediately, and let the event-loop tick check `g_count_synced_replicas(target)` and complete the reply when the threshold is reached or the deadline fires. Same Redis-visible semantics, no whole-server stall.

### 1.7 RDB silently drops streams _(from gpt)_
`src/rdb.cpp`'s `rdb_build_string()` skips stream-typed entries entirely on save. The result is silent data loss across `SAVE`/`BGSAVE`/restart. Two acceptable fixes: (a) implement RDB stream encoding to parity, or (b) until then, return `-ERR RDB save: stream keys not yet supported` and document the delta in `compat/delta.md`. Silent loss is the worst option.

### 1.8 Atomic rename for RDB/AOF rewrites _(from gpt)_
`rewrite_aof_file` (`src/command.cpp:635`) and the RDB save path write directly to the destination file. A crash mid-write leaves a truncated `dump.rdb` / `appendonly.aof` that the next startup will reject or partially load. Standard fix: write to `dump.rdb.tmp` (same directory, same filesystem), `fsync` the file, `rename(2)` over the destination, `fsync` the directory. ~30 lines, eliminates a class of corruption bugs.

### 1.9 Config precedence is reversed _(from codex)_
`src/main.cpp` parses CLI args first, then loads the config file which **overwrites** them. Redis does the opposite: `redis-server redis.conf --port 6380` runs on 6380. Operator muscle memory will be wrong here. Swap the order, add a precedence test fixture.

---

## Tier 2 — Test & CI infrastructure (the multiplier)

### 2.1 No CI configured
`.github/` is empty. The whole 9-stage `scripts/run_all_tests.sh` pipeline runs only when a developer invokes it. Add a GitHub Actions workflow (or equivalent) that runs unit + integration + fuzz-sanity on every PR, and the perf baseline + Redis TCL suite + ASAN nightly. This is the single change that most increases the value of all the existing test work.

### 2.2 Only 2 C++ unit tests (`config_parser_test`, `wrongtype_framework_test`)
Every command is currently regression-tested only through Python integration tests that spawn a real server. Each integration test pays connection + RESP cost. Add unit tests that drive `handle_command(args, session, should_close)` directly against a fresh `DataStore` instance — no socket needed. Targets: dispatch table arity validation, MULTI/EXEC abort paths, EXPIRE option matrix, OBJECT ENCODING policy, ZADD flag combinations. Each is <100 lines and runs in <10ms.

### 2.3 Differential tests are opt-in
`tests/diff/run_diff_tests.py` compares against a real Redis but isn't in the standard CI gate. Promote it to a nightly job; archive `last_repro.txt` as a CI artifact on failure.

### 2.4 Compiler-warning gate _(from codex)_
A `-Wall -Wextra -Wpedantic` build produces 12 warnings today. Add a CI job that runs the warning-strict build non-fatal first; once cleared, promote to `-Werror` on the default Linux compiler set. Cheapest defect-detection upgrade in the project.

### 2.5 Stop `pkill -9 -f peadb-server` _(from codex/gpt)_
`scripts/ci/run_integration_tests.sh` and helpers shell out to `pkill -9 -f peadb-server`, which kills *anyone's* PeaDB on a shared dev/CI host. Track child PIDs per test stage; SIGTERM with grace period; SIGKILL only the recorded PIDs after timeout. Same fix unblocks parallel test runs.

### 2.6 Coverage gate is text-only, not execution _(from codex/gpt)_
`scripts/ci/check_command_test_coverage.py` confirms a command name appears under `tests/`. A command mentioned in a comment counts as "covered". Replace with: enumerate command names from `command_table()`, require each to be observed in a successful integration-test invocation log, with an explicit allowlist + reason for exceptions.

### 2.7 JUnit XML + generated reports _(from codex/gpt)_
Emit JUnit XML from the integration runner and the diff runner. Pair with the **generated compatibility scoreboard** described in §5.2: `compat/commands.generated.md`, `compat/redis-tests.generated.md`, `compat/differential.generated.md`. Triage in CI becomes one click; documentation drift becomes mechanically impossible.

---

## Tier 3 — Correctness items I'd bump above Opus's prioritisation

### 3.1 SCAN cursor instability (`src/datastore.cpp` `scan()`)
The cursor is a sequential index into `unordered_map`. A single insert that triggers rehash invalidates iteration order; clients lose or duplicate keys. Implement Redis's reverse-binary-increment cursor over the bucket array (Antirez wrote it up; ~150 LoC port). Without this, PeaDB cannot honestly claim "drop-in replacement" for any application using `SCAN` against a mutating dataset.

### 3.2 Sorted-set range queries are O(N log N) per call
`zrange`/`zrangebyscore` copy + sort the entire `unordered_map<string,double>` every call. As a stop-gap before a proper skiplist, cache a sorted `std::vector<pair<string,double>>` per ZSet and invalidate it on `mutation_epoch_` change for that key. Cheap, transparent, eliminates the worst case for read-heavy workloads.

### 3.3 Active expiry iterates from `begin()`, not random
`active_expire_cycle(64)` always re-walks the front of `unordered_map`. With a steady stream of inserts past the first 64 entries, expired keys at the tail are reclaimed only on lazy access. Use `std::uniform_int_distribution` to pick a starting bucket (FAQ:644 already flags this).

### 3.4 Blocking-command wakeup latency
`notify_key_written` exists in `command.hpp` but is a no-op. The poll-based wakeup (FAQ:756–764) injects up to 100ms latency on `BLPOP`. Wire `LPUSH`/`RPUSH`/`XADD` to call `notify_key_written` → consult a `key → set<SessionState*>` registry → mark the blocked session ready in the same iteration. Removes a known semantic gap with Redis.

### 3.5 Replication: bounded backlog + accurate INFO _(from gpt)_
Replace `g_replication_events` (unbounded `vector`) with a fixed-size byte-offset-keyed ring buffer ("backlog") matching Redis's `repl_backlog`. Implement PSYNC2 partial-resync rules around `replid`, `replid2`, and the offset window. Wire `INFO replication` to the live counters (`connected_slaves`, `master_repl_offset`, `repl_backlog_size`, `repl_backlog_first_byte_offset`) instead of returning hardcoded `0`s. Replace the `std::system("python3 scripts/redis/sync_from_redis.py ...")` bootstrap path in `REPLICAOF` with an in-process replication client (gpt 2.2 — also a security concern, since `system()` invokes a shell).

### 3.6 Cluster bus is not the cluster bus _(from gpt)_
The "cluster bus" listener on port `N+10000` currently replies plain `PONG`. Real Redis cluster bus is a binary protocol (`clusterMsg` / gossip). Until that is implemented, the cluster surface should be labelled "command surface only" in `compat/delta.md` and `README.md` — and any claim of "cluster mode" should be qualified accordingly. Then either implement the binary protocol or document it as a permanent non-goal.

### 3.7 Fix the failing Lua tests already in `artifacts/` _(from gpt)_
`artifacts/redis-stage-a/failed_suites.txt` lists `unit/scripting`. `unit_scripting.log` shows two distinct failures: cjson numeric decoding (`encode → decode → encode` round-trip drift) and cmsgpack circular-reference handling. Both live in `src/lua_cjson_stub.c` / `src/lua_cmsgpack_stub.c`. Vendor the upstream Redis cjson/cmsgpack sources (MIT-compatible) — the word "stub" in the file names is itself a tell. Removes a known regression and shrinks our maintenance burden.

### 3.8 `mutation_epoch` reconciliation _(from gpt)_
`DataStore::mutation_epoch_` (`include/datastore.hpp:176`) and the command-layer `g_mutation_epoch` (`src/command.cpp:65`) are two separate counters. `INFO persistence`'s `rdb_changes_since_last_save` is computed from one but mutated by the other in some paths. Pick one, delete the other, add a unit test asserting it ticks once per write command.

---

## Tier 4 — Hygiene & invariants

### 4.1 `thread_local` on a single-threaded server is misleading
`g_in_lua_script`, `g_script_readonly_context`, `g_script_client_resp_version`, `g_script_current_session`, `g_module_dma_buf`, `g_script_allow_oom`, `g_script_inner_error_seen` are all `thread_local`. The code is single-threaded except for the `REPLICAOF` bootstrap helper. Either commit to multi-threaded execution (and audit all the *non*-`thread_local` globals — `g_master_repl_offset`, `g_replication_events`, the cmdstats maps — for races) or remove `thread_local`. Pretending both is the worst of both worlds.

### 4.2 `Entry::string_force_raw` leaks command-layer state into the datastore
`include/datastore.hpp:34`. The `force_raw` bit exists for a Lua reply concern. The datastore should not know about that. Move it to a side-table keyed in `command.cpp` or recompute from value content.

### 4.3 Finish the `Poller` abstraction
`server.cpp:62–148` already wraps epoll/kqueue behind a `Poller` struct, but it's an in-file `namespace {}` aggregate. Extract to `include/poller.hpp` as a small interface (or strategy template). This is also the prerequisite for Opus §7 (io_uring) — doing it now means the future io_uring change is a single new TU.

### 4.4 Bundle Lua 5.1 source
`CMakeLists.txt:13–18` caps optimisation at `-O2` on macOS to dodge a system-Lua miscompile. Bundling Lua 5.1.5 (~24K LoC, MIT) removes the cap, removes the apt/brew dependency for new contributors, and makes release builds genuinely comparable across platforms. Higher leverage than it looks.

### 4.5 Repo conventions
No `.clang-format`, `.clang-tidy`, `.editorconfig`. Six source files vary in include order and brace style. Add a `.clang-format` based on the existing dominant style and a CI lint gate.

---

## Tier 5 — Compatibility & architectural prerequisites _(adopted from gpt)_

### 5.1 Definition of Done for "Redis-compatible"
A feature is Redis-compatible only when **all** of these hold:
1. Targeted integration test passes.
2. Redis-vs-PeaDB differential test passes (including error text and error precedence).
3. Relevant upstream Redis Tcl tests pass, or there is an explicit waiver entry in `compat/delta.md`.
4. Persistence + replication coverage if the feature mutates data.
5. RESP2 and RESP3 encodings both verified for commands that support both.
6. `compat/commands.generated.md` row updated.

Until then the feature is labelled `surface`, `partial`, or `experimental`. README/FAQ may not advertise stronger language than the scoreboard supports.

### 5.2 Generated compatibility scoreboard
Replace prose claims with machine-generated artifacts committed by CI:
- `compat/commands.generated.md` — every command from `command_table()` with arity, flags, key spec, owning test, status.
- `compat/redis-tests.generated.md` — Redis Tcl suites: passed / failed / skipped + reason.
- `compat/differential.generated.md` — diff-test coverage by command.
- `compat/modules.generated.md` — exported `RedisModule_*` symbols + per-module certification status.

This single change subsumes the README "147 commands" / 207 actual mismatch, the FAQ's stale Pub/Sub claim, and the "stub vs implemented" ambiguity in `compat/delta.md`.

### 5.3 `RespValue` typed return values _(from gpt)_
Today every handler returns a pre-encoded RESP `std::string`. Full RESP3 (maps, sets, doubles, booleans, big numbers, push) is unreachable from this shape. Introduce a small `RespValue` sum type and let the protocol layer encode it for the negotiated `RespVersion`. Same change makes `WRONGTYPE` / `OOM` paths uniform and lets unit tests assert on structured values.

### 5.4 Make `db` explicit in DataStore APIs _(from gpt)_
`DataStore::current_db_` is mutated by `server.cpp` to match `session.db_index` before each command. Sharding, deterministic multi-client tests, and concurrent-Lua all break this assumption. Change the API to `get(db, key)`, `set(db, key, …)`, etc.; remove the `current_db_` field. Mechanical refactor; ~200 callsite touches; required prerequisite for Opus §8 (sharding).

### 5.5 Client output buffer limits _(from gpt)_
Redis enforces `client-output-buffer-limit normal/replica/pubsub` and disconnects clients that exceed thresholds. PeaDB has no such policy — a slow client can grow `WriteBuf` until the server OOMs. Add the three classes with Redis-compatible defaults; disconnect on overrun; expose hits in `INFO stats`.

### 5.6 Memory accounting before maxmemory _(from gpt)_
`INFO MEMORY` currently returns `mem_fragmentation_ratio:1.00` and other fields hardcoded (FAQ:654). Without real `used_memory` accounting (per-entry size estimate, plus container overhead), `maxmemory`, eviction, and any production safety claim are fictional. Add a `MemoryAccountant` that hooks into mutation paths and `expire_if_needed`. Prerequisite for any future eviction work — do this *before* implementing `allkeys-lru` etc.

---

## Tier 6 — Defer

These are reasonable Opus suggestions I would defer until Tiers 1–5 land:

- **Opus §2 `std::variant<EntryValue>`** — high upside (per codex's `sizeof(Entry)==304`), but invasive. Land §5.4 (db-explicit API) and §5.3 (`RespValue`) first; the variant change becomes mechanical afterwards.
- **Opus §1 splitting `command.cpp`** — desirable for build times, but pointless until the global state in §4.1 (`thread_local` audit) and §5.4 are decided. Splitting first locks in the wrong dependency graph.
- **Opus §7 io_uring + §8 sharding** — both depend on §4.3 (Poller extraction), §5.4 (db-explicit), §5.3 (`RespValue`), and the bounded replication backlog in §3.5. Trying them earlier wastes effort.

---

## Suggested sequencing (revised)

1. **Week 1 — stop the bleeding:** §1.6 WAIT non-blocking, §1.7 RDB streams (loud-fail at minimum), §1.8 atomic rename, §1.9 config precedence, §2.1 CI workflow, §2.5 `pkill` cleanup, docs reconciliation (Pub/Sub claim, 147→207 count).
2. **Week 2–3 — reliability + signal:** §1.1 AOF fd+fsync, §1.4 WriteBuf, §1.5 BGSAVE reaping, §1.2 reply coalescing, §1.3 offset double-encode, §2.4 warning gate, §2.6 coverage gate upgrade, §2.7 JUnit + scoreboard skeleton, §3.7 fix the existing Lua failures.
3. **Next milestone — correctness parity:** §3.1 SCAN cursor, §3.2 ZSET cached order, §3.3 active expiry random-start, §3.4 BLPOP wakeup, §3.5 replication backlog + INFO, §3.8 `mutation_epoch` reconciliation, §2.2 unit tests, §5.2 generated scoreboard live, §5.5 output buffer limits.
4. **Architectural prerequisites:** §4.1 `thread_local` audit, §4.3 Poller extraction, §4.4 bundled Lua, §5.3 `RespValue`, §5.4 db-explicit API, §5.6 memory accounting, §3.6 decide cluster-bus posture.
5. **Architecture milestone:** hand off to Opus's §2 (`std::variant`), §1 (split `command.cpp`), §7 (io_uring), §8 (sharding) — now safe to attempt.

The Tier 1–2 work is the prerequisite for any honest performance comparison or production-readiness claim. Tier 3–5 is the prerequisite for the architectural rewrites in `improve-opus.md` to land without breaking compatibility. Tier 6 is where you go fast — once you can.
