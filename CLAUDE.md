# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current work

The active plan and the verified next-work list live in `AI-TEMP/improve-plan.md` (local and gitignored; read its Part 0 first). A verified review on 2026-10-03 found silent data-loss defects; they are registered as `compat/delta.md` items 24–71 (item 24 is fixed in part; the others are not fixed yet), and the statements below marked "not yet fixed" come from that review and name their item.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j$(nproc)
```

Requires: `cmake`, `g++` (C++20). Lua 5.1.5 is **bundled** (`third_party/lua-5.1.5`) on every platform; no system Lua is used. It is stock 5.1.5, not Redis's patched `deps/lua` (no read-only-table or string-hash patches), and cjson/cmsgpack/bit/struct are PeaDB's own code (`src/lua_*_stub.c`), so table order for long string keys, `cjson.encode` number text and some `struct` formats differ from Redis (delta items 62–63, not yet fixed).

On macOS `-O2` is kept for Release builds so benchmark baselines stay comparable (see `CMakeLists.txt`).

`VERSION` (a plain `MAJOR.MINOR.PATCH` file at the repo root) is the single source of truth for PeaDB's own version, reported by `INFO`'s `peadb_version` field. It reaches the binary as a compile definition, `PEADB_VERSION` (`CMakeLists.txt`: `project(peadb VERSION ${PEADB_VERSION_RAW} ...)` reads `VERSION`, then `target_compile_definitions(peadb_core PUBLIC PEADB_VERSION="${PROJECT_VERSION}")`), not a generated header — deliberately, because `scripts/qa/run_fuzz_sanity.sh` compiles `src/command.cpp` by hand outside CMake with only `-Iinclude`, and a header written into a CMake build directory would be invisible to that build (it passes `-DPEADB_VERSION="$(cat VERSION)"` instead, reading the same `VERSION` file). `include/peadb_version.hpp` `#error`s if `PEADB_VERSION` is undefined, so a build that forgets to define it fails loudly at compile time instead of silently shipping a wrong version. Full rationale: `docs/decisions/p5-cluster-addressing-and-version-provenance.md`.

## Coding Conventions

- **Standard**: C++20. `CMakeLists.txt` sets `-Wall -Wextra -Wno-unused-parameter` on the `peadb_core` library target, `PRIVATE` — there is no `-Werror`, and because the flags are `PRIVATE` (not `INTERFACE`/`PUBLIC`), `src/main.cpp` (the `peadb-server` executable) and every unit-test executable, which only link `peadb_core`, get none of these flags either — they build with whatever the compiler's own defaults are.
- **Style**: Google-based `.clang-format` (`IndentWidth: 2`, `ColumnLimit: 100`, attached braces) — CI enforces `clang-format --dry-run --Werror` over `src include` (`.github/workflows/pr.yml`). Note that the enforced tool and the stated convention aren't always the same thing: `clang-format` only checks *formatting*, not line length as typed before wrapping, and `src/command.cpp` has about 500 lines longer than 100 columns (499 at `cb7194a`, counted in characters) with CI green — clang-format doesn't reflow existing long lines it didn't create (e.g. inside string literals or where breaking would violate another rule), so treat the column limit as a target for new code, not a guarantee about the whole file. CI's clang-format version is not pinned (it follows the `ubuntu-latest` image) and recent local versions disagree with it, so never run `clang-format -i` over whole files.
- **Shell scripts** must run on macOS's bash 3.2: no `mapfile`, `${var,,}` or `${var^^}`.
- **Naming** (conventions, not enforced by any tool — there is no `.clang-tidy` in this repo, and never has been):
  - Namespaces `lower_case` (`peadb::storage`); Classes/Structs `CamelCase` (`DataStore`); Functions `camelBack` for methods, though `src/command.cpp`'s free helper functions are overwhelmingly `lower_case` (`handle_command`, `emit_keyspace_notifications`) — follow the surrounding file rather than this rule when they conflict; Variables `lower_case` (`partition_id`); Constants `UPPER_CASE` or `kCamelCase` for `constexpr`; Member variables `lower_case_` (trailing underscore)
- **Commits**: after each completed, coherent change, commit it without being asked — one concise **single-line** [Conventional Commits](https://www.conventionalcommits.org/) subject (`feat:`, `fix:`, `refactor:`, `docs:`, `test:`, …), imperative mood, no body unless essential. Only commit complete, non-broken states (not mid-task checkpoints). Don't `git push` unless asked. Don't create or switch branch. We want a linear git history.

## Tests

```bash
# Unit tests (fast, no server needed)
cd build && ctest --output-on-failure

# Integration tests (starts/stops peadb-server automatically)
cd build && ../scripts/ci/run_integration_tests.sh

# Single integration test
python3 tests/integration/test_m2_strings_expire.py

# Upstream Redis 7.2.5 TCL suite in native mode (the harness spawns peadb-server as a
# redis-server stand-in). Adopted suites + ratchet baselines: compat/redis_suite_ledger.json.
SUITE_MODE=native scripts/redis/run_redis_tests.sh
python3 scripts/redis/parse_tcl_results.py artifacts/redis-native   # per-suite pass counts
python3 scripts/ci/check_suite_ratchet.py                           # monotonic ratchet gate

# Each integration test starts its own server on a fixed port; new tests need an unused one
python3 scripts/ci/check_port_collisions.py   # stage `portcheck`: fails on new port collisions
```

Tests find `redis-server` through `tests/integration/_redis_path.py` (the pinned 7.2.5 build first, `PATH` last), so a newer Redis on `PATH` (for example Homebrew 8.x) is only a fallback. For ad-hoc experiments, start servers on an unused `--port` with a scratch `--dir` outside the repo, and stop only the PIDs you started: `pkill -f peadb-server` also kills servers that a running test suite owns.

Check persistence and replication changes against the pinned real Redis (`~/.cache/peadb/redis-7.2.5/src/redis-server`, `redis-check-rdb`): load each other's `dump.rdb` and replicate in both directions. PeaDB-only round trips cannot catch format bugs; they hid the wrong RDB CRC64.

**The parity ratchet**: adopted upstream suites must stay at 0 `[err]`; growing a skipfile (`scripts/redis/skipfiles/`) or lowering a ledger count requires updating `compat/delta.md` in the same change. See the "Verification" section of `compat/delta.md`.

### Adopting an upstream TCL suite

The parity work adopts one upstream suite (or a small batch) per commit:

1. **Baseline** the suite natively: `cd ~/.cache/peadb/redis-7.2.5 && SUITE_MODE=native REDIS_SERVER_BIN=<repo>/peadb-server ./runtest --single unit/<suite> --clients 1 --tags "-tls -cluster -modules -needs:pfdebug -leaks" --dont-clean [--skipfile <repo>/scripts/redis/skipfiles/<suite>.skip]`. The TCL control server uses a fixed port (21079) — only one `runtest` at a time; kill leftovers with `pkill -f test_helper.tcl`.
2. A suite counts as adopted only when it **runs to completion** (`exit 0` / "All tests passed") — not merely `err=0`; a mid-suite hang records a truncated-but-green ok count.
3. The pinned **Redis C source** (`~/.cache/peadb/redis-7.2.5/src/`) is the spec for exact error strings and semantics; `assert_match` is case-sensitive.
4. **Skipfile only what's genuinely out of reach**; every skip line carries a comment naming its `compat/delta.md` item, added in the same commit (the ratchet enforces this).
5. **Register**: add the suite to `SUITES` in `scripts/redis/run_redis_tests.sh` and a baseline entry to `compat/redis_suite_ledger.json` (canonical counts from a `-leaks`-tagged run; `ignored` = `[ignore]` + `[skip]`).
6. Regenerate + validate: `parse_tcl_results.py`, `check_suite_ratchet.py`, `generate_compat_scoreboard.py`, `check_compat_delta.py`; update `CHANGELOG.md` (`[Unreleased]` wave entry) and `compat/delta.md`; then run both mandatory pre-commit checks below.

## MANDATORY pre-commit checks

**After every set of changes that is ready to commit, you MUST run both of these and confirm they pass before considering the work done:**

```bash
# 1. Full test suite (unit + integration)
bash scripts/run_all_tests.sh

# 2. Performance regression check (must not get worse vs Redis baseline)
bash scripts/dev/redis_vs_peadb_benchmark.sh
```

Do not skip either check. Do not commit if either fails.

`run_all_tests.sh` runs 11 stages and takes about 40 minutes: start it in the background and wait for it, because a tool timeout (about 10 minutes) stops it early and looks like a failure. `redis_vs_peadb_benchmark.sh` needs Docker host networking, so it only works on Linux; on macOS, report it as not run (a PeaDB-only `redis-benchmark` run is a smoke check, not the comparison).

## Keep docs in sync

Docs are part of the change, not a follow-up. **Any change that affects behavior, commands, build/test workflow, architecture, or compatibility MUST update the relevant doc(s) in the same commit.** Where each kind of fact lives:

- **`README.md`** — user-facing overview, quick start, config flags, command list, benchmarks.
- **`docs/compatibility.md`** — drop-in posture, migration/rollback, feature-status table, persistence/durability, ops notes.
- **`docs/architecture.md`** — execution model, data structures, persistence/replication internals, design principles & roadmap.
- **`compat/delta.md`** — canonical compatibility-gap registry; update whenever a gap opens, closes, or changes status (a feature shipping means its delta entry changes).
- **`CLAUDE.md`** (this file) — build/test commands, conventions, and the architecture summary below.
- **`SECURITY.md`** — supported versions, private vulnerability reporting, known security gaps.
- **`docs/RELEASING.md`** — release tagging convention and the multi-arch publish sequence; update whenever the tag scheme, the publish workflow, or the `latest`-promotion timing changes.

Don't restate in prose what the code or `compat/delta.md` already owns (command counts, per-command status) — link to the source of truth so the docs can't drift.

## Architecture

PeaDB is a Redis 7.2.5-compatible server written in C++20. For a fuller contributor-facing overview (execution model, data structures, persistence, replication, roadmap), see [`docs/architecture.md`](docs/architecture.md).

- **`src/command.cpp`** — All Redis command handlers in one file (authoritative command list/count: `compat/commands.generated.md`). The command table is built once in `command_table()` (a static local). `handle_command()` is the main dispatcher.
- **`src/server.cpp`** — Single-threaded epoll/kqueue event loop. Manages client sessions (`SessionState`), write buffers (`WriteBuf`), blocked clients, and deferred WAIT replies. Calls into command.cpp for every client command.
- **`src/datastore.cpp`** — In-memory key-value store (`DataStore`), 16 independent databases, all standard Redis data types (string, hash, list, set, zset, stream). Bucket-based SCAN cursor and random-start active expiry; SCAN and RANDOMKEY walk the whole database on every call, and SCAN can miss keys when the table grows during a scan (delta items 32 and 55, not yet fixed).
- **`src/rdb.cpp`** — RDB binary format reader/writer. Non-stream types use real Redis type bytes, but the trailing CRC64 uses a wrong (non-reflected) table (`crc64_init`), so real Redis refuses PeaDB's RDB files, PeaDB refuses real Redis's, and cross-engine DUMP/RESTORE fails (delta item 27, not yet fixed). Streams are encoded under a **PeaDB-specific type byte (200)** via `DataStore::dump_stream`/`restore_stream` (entries + last-id + consumer groups + full PEL), so `SAVE`/`BGSAVE`/PSYNC handle stream datasets — but a PeaDB RDB containing streams is NOT loadable by real Redis (cross-engine stream migration uses AOF/bridge). See `compat/delta.md` items 2 & 5.
- **`src/lua_engine.cpp`** — Lua 5.1 scripting (EVAL/EVALSHA/FCALL), wired back to `handle_command` for Redis call dispatch.
- **`include/command.hpp`** — `SessionState`, `BlockedState`, `WaitState` structs; all extern globals and callbacks shared between command.cpp and server.cpp.
- **`include/datastore.hpp`** — `DataStore` class and `Entry`. `Entry` is a **tagged union** (`std::variant` over the inline string + five heap-held collection payloads), so only the active type allocates: ~56 bytes + the active payload, down from the old fat struct that carried all six payloads at once (~328 B libc++ / ~456 B libstdc++). `expire_at_ms`, `lru`, and `string_force_raw` stay inline; typed accessors (`str()`/`hash()`/`list()`/`set()`/`zset()`+`zord()`/`stream_*()`) lazily materialize the active alternative, and a hand-written deep-copy ctor clones it (the `unique_ptr` members make the default copy ill-formed). `type()` is `payload.index()`. All payload access is confined to `src/datastore.cpp`.

## Key Conventions

- Anonymous namespace in `command.cpp` contains all helpers and globals. Public functions are defined after `} // namespace` (before `} // namespace peadb`).
- Globals in `command.cpp` anonymous namespace are accessible from public functions in the same file via the same-TU rule.
- `SessionState::fd` must be set before BLPOP/BRPOP block — used by `register_key_waiter`.
- Persistence: AOF uses a persistent fd (`g_aof_fd`) opened at startup. `fsync` policy controlled by `g_appendfsync` ("always"|"everysec"|"no"); failed fsyncs set `g_aof_write_error`. BGSAVE/BGREWRITEAOF fork and are reaped by `reap_background_children()`. Save points (`g_save_params`, parsed by `configure_save`) fire `do_bgsave_fork()` from `background_save_cron()` in the event loop; `SHUTDOWN [SAVE|NOSAVE]` saves on exit, but SIGTERM/SIGINT exit without that final save (delta item 33, not yet fixed). Write commands are rejected with `-MISCONF` when `misconf_should_reject()` (failed bgsave + save points, or AOF write error) and `-OOM` when `oom_should_reject()` (`used_memory() > maxmemory`).
- Eviction: `oom_should_reject()` runs `evict_to_fit()` first when `g_maxmemory_policy != NoEviction` (master only, not during AOF replay), looping `DataStore::evict_one(policy, g_maxmemory_samples)` until under budget; each victim is propagated as `DEL` to backlog + AOF and emits `notify_keyspace_event(NOTIFY_EVICTED, "evicted", …)`. Policy enum is `MaxmemoryPolicy` (datastore.hpp); `apply_maxmemory_policy()` also sets `store().set_eviction_lfu()`. Per-key LRU/LFU lives in `Entry::lru`, touched in `DataStore::get()` and at the dispatch chokepoint (`touch_key_for_eviction`, first key only). `evict_one` samples via the `randomkey()` bucket walk and decrements `used_mem_cache_` so the loop sees memory fall live. Config: `configure_maxmemory_policy`/`configure_maxmemory_samples`, CONFIG GET/SET `maxmemory-policy`/`maxmemory-samples`, `--maxmemory-policy`/`--maxmemory-samples` CLI.
- Memory accounting: `DataStore::used_memory()` caches `estimate_used_memory()`, a full walk of every key and element, recomputed at most every 100 ms; once one walk takes 100 ms or more (about 2M keys) it runs on every write when maxmemory is set, it undercounts collections, and INFO also walks the keyspace (delta items 38 and 55, not yet fixed). `maxmemory` is parsed with units via `parse_memory()` / `configure_maxmemory()`.
- Robustness: server.cpp wraps dispatch in `safe_handle_command()` (one bad command can't kill the loop); `main.cpp` installs fatal-signal crash handlers (backtrace) + `std::set_terminate`; a corrupt AOF is fatal at startup while a truncated tail is tolerated (but never cut off, so the next restart fails — delta item 30, not yet fixed). RDB load verifies the trailing CRC64 (`rdb_load(..., verify_crc=true)`, with the wrong CRC64 noted under `src/rdb.cpp` above); a corrupt native (`REDIS`-magic) dump is fatal in `main.cpp` rather than falling back to legacy. The replication path loads with `verify_crc=false` (foreign-master CRC variants differ).
- Observability: one timing wrapper at the dispatch chokepoint (`command.cpp`, around `spec.handler(...)`) feeds `CmdStats::usec_total` (INFO commandstats), `slowlog_add()`, and `latency_add()`. Keyspace notifications are emitted centrally by `emit_keyspace_notifications()` after a successful write (+ `"expired"` from `run_active_expire_cycle()`/`append_lazy_expire_dels`), gated by `g_notify_flags` (parsed from `notify-keyspace-events`). `MONITOR` uses `g_monitor_fds` (fed by `feed_monitors()`, cleared in `on_client_disconnect()`); `CLIENT KILL` goes through `g_client_kill_cb` (server.cpp owns fd close). INFO RSS via `process_rss_bytes()`, CPU via `getrusage`.
- Replication: REPLICAOF spawns a generation-tagged (`g_repl_generation`) supervisor thread that auto-reconnects with backoff via `replica_do_fullsync(host, port, gen)`; it stops when the generation changes (a newer REPLICAOF / NO ONE). The streaming loop checks the generation each ~1 s recv tick. Active expiry runs only on the master (`run_active_expire_cycle()` no-ops on a replica) and propagates `DEL` to the backlog. `noreplicas_should_reject()` gates writes on the real connected-replica count. Backlog size is `g_repl_backlog.size_` (set by `configure_repl_backlog_size`). The replid is randomized per process (`init_replication_id()`, called in `main.cpp`); `REPLICAOF NO ONE` rotates it into `g_master_replid2`/`g_second_replid_offset` and PSYNC accepts a partial resync against that history. PeaDB's own replicas always send `PSYNC ? -1` (every reconnect is a full resync), and the master builds the full-sync RDB in memory on the event loop (delta items 46–47, not yet fixed). PSYNC increments `g_sync_full`/`g_sync_partial_ok`/`g_sync_partial_err` (INFO Stats). `WAIT` appends `REPLCONF GETACK *` to the backlog so replicas ACK within ~1 RTT; the replica streaming loop replies to GETACK immediately (out-of-band, not via the event-loop queue).
- Lazyfree: `DataStore::unlink()` (UNLINK) detaches big values (>64 elements) via `db.extract` and `flushall_async`/`flushdb_async` (FLUSH `ASYNC`) swap the whole db map out, handing the detached `DB` to a background worker thread (`lazyfree_thread_` + mutex/condvar queue) that destructs it off the event loop. Safe under single-writer because ownership is moved out of the live keyspace before handoff; `~DataStore()` joins the thread. Per-class output-buffer limits live in `g_cobl[3]` (`configure_client_output_buffer_limit`); server.cpp closes overflowing clients (`obuf_overflow_fds`). Idle clients are reaped by `client_timeout_seconds()` (server housekeeping), exempting replicas/blocked/MULTI/`client_has_subscriptions()`.
- Config loading (`src/config.cpp`) follows Redis 7.2.5's `loadServerConfig()`: the file, then stdin (`-`), then `--name value` options turned into config lines (`config_options_from_argv`, a port of Redis's `main()` loop), all parsed as one text, so the last value wins. Names, aliases, types, ranges, enum values, flags (immutable, protected, hidden, sensitive) and defaults come from `include/redis_config_table.generated.hpp` (`scripts/ci/generate_redis_config_table.py`, generated from the pinned `config.c` and `server.h`; re-run it after a Redis pin bump). Anything Redis's loader refuses stops startup with Redis's error text; a `dir` line `chdir()`s at once, as in Redis, so the data file names (`g_snapshot_path`, `g_aof_path`) are relative to the working directory; Redis settings PeaDB does not apply are decided on their final value in `finish_load()` — refused when PeaDB cannot honour them safely (`fixed_settings()` entries with a `refuse` reason), otherwise accepted with one warning each (delta item 24). `CONFIG GET`/`CONFIG SET` (`config_get_reply()`/`config_set_reply()` in `src/command.cpp`) follow Redis's `configGetCommand()`/`configSetCommand()` and share the loader's value checks (`check_redis_config_value()`); a setting PeaDB applies has a getter and setter in `live_configs()`, a fixed one (`peadb_fixed_setting()`) refuses any other value, and any other one is kept in `g_config_values` with a warning (delta items 23–25). `rename-command` is checked while loading, against Redis's command table (`handle_rename()`); `command_table()` applies the checked renames. Test launches must use Redis's loglevel names (`warning`, not `error`). `--config <path>` is PeaDB's only non-Redis option.
- Cluster: four config options — `cluster-enabled` (startup-only, like Redis's own `IMMUTABLE_CONFIG` treatment; refused by `CONFIG SET`), `cluster-announce-ip`/`-port`/`-bus-port` — are parsed into `ServerConfig` (`include/config.hpp`) and read back through `cluster_announced_host()`/`cluster_announced_port()`/`cluster_announced_bus_port()` (`src/command.cpp`); an unset announce-ip falls back to `bind`, and with `cluster-enabled yes` the server refuses to start if that fallback would resolve to a wildcard (`0.0.0.0`, `::`, `*`, or empty). Node ids are `sha1_hex("peadb-cluster-node:" + host + ":" + port)` — deterministic from the announced address rather than random-and-persisted, since there is no `nodes.conf`. Full design rationale (including what is and is not gossiped): `docs/decisions/p5-cluster-addressing-and-version-provenance.md`. Cluster mode is not production-safe yet: routing hashes only the first key (no CROSSSLOT), there is no CLUSTERDOWN, and nodes neither share nor persist slot ownership (delta items 64–67; item 6 is marked `experimental`).
- Security: `handle_command()` gates on `g_requirepass` — when set, unauthenticated sessions (`SessionState::authenticated`) get `-NOAUTH` unless the command is flagged `no_auth` (AUTH/HELLO/QUIT/RESET). Internally replayed/replicated commands bypass the gate via `g_replaying_aof`. `configure_security()` (called from `main.cpp` before the first `command_table()` build) wires `requirepass`/`masterauth`/`protected-mode`/`enable-module-command`/`rename-command`. protected-mode is enforced in the server accept loop (`protected_mode_enabled()`/`requirepass_is_set()`). `MODULE LOAD` requires `enable-module-command`. Only `requirepass` sets a password; config-file `user` and `aclfile` lines stop startup (delta item 24). Protocol limits (`proto-max-bulk-len`, multibulk cap) live in `protocol.cpp`; the server closes the connection when `handle_command` sets `should_close` for a protocol error.

## Compatibility

`compat/delta.md` is the canonical status registry for known gaps vs Redis 7.2.5. `compat/commands.generated.md` is auto-generated by `scripts/ci/generate_compat_scoreboard.py`.

Feature status labels: `full` | `partial` | `surface` | `stub` | `experimental`.
