# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j$(nproc)
```

Requires: `cmake`, `g++` (C++20), `liblua5.1-0-dev` (Lua 5.1 headers + library).

On macOS `-O2` is enforced for Release builds because the non-bundled system Lua (5.2+) miscompiles at `-O3` (see `CMakeLists.txt`).

## Tests

```bash
# Unit tests (fast, no server needed)
cd build && ctest --output-on-failure

# Integration tests (starts/stops peadb-server automatically)
cd build && ../scripts/ci/run_integration_tests.sh

# Single integration test
python3 tests/integration/test_m2_strings_expire.py
```

## MANDATORY pre-commit checks

**After every set of changes that is ready to commit, you MUST run both of these and confirm they pass before considering the work done:**

```bash
# 1. Full test suite (unit + integration)
bash scripts/run_all_tests.sh

# 2. Performance regression check (must not get worse vs Redis baseline)
bash scripts/dev/redis_vs_peadb_benchmark.sh
```

Do not skip either check. Do not commit if either fails.

## Keep docs in sync

Docs are part of the change, not a follow-up. **Any change that affects behavior, commands, build/test workflow, architecture, or compatibility MUST update the relevant doc(s) in the same commit.** Where each kind of fact lives:

- **`README.md`** — user-facing overview, quick start, config flags, command list, benchmarks.
- **`docs/compatibility.md`** — drop-in posture, migration/rollback, feature-status table, persistence/durability, ops notes.
- **`docs/architecture.md`** — execution model, data structures, persistence/replication internals, design principles & roadmap.
- **`compat/delta.md`** — canonical compatibility-gap registry; update whenever a gap opens, closes, or changes status (a feature shipping means its delta entry changes).
- **`CLAUDE.md`** (this file) — build/test commands, conventions, and the architecture summary below.

Don't restate in prose what the code or `compat/delta.md` already owns (command counts, per-command status) — link to the source of truth so the docs can't drift.

## Architecture

PeaDB is a Redis 7.2.5-compatible server written in C++20. For a fuller contributor-facing overview (execution model, data structures, persistence, replication, roadmap), see [`docs/architecture.md`](docs/architecture.md).

- **`src/command.cpp`** — All Redis command handlers in one file (~7.7k LoC; count is authoritative in `compat/commands.generated.md`). The command table is built once in `command_table()` (a static local). `handle_command()` is the main dispatcher.
- **`src/server.cpp`** — Single-threaded epoll/kqueue event loop. Manages client sessions (`SessionState`), write buffers (`WriteBuf`), blocked clients, and deferred WAIT replies. Calls into command.cpp for every client command.
- **`src/datastore.cpp`** — In-memory key-value store (`DataStore`), 16 independent databases, all standard Redis data types (string, hash, list, set, zset, stream). Bucket-based SCAN cursor, random-start active expiry, O(1) RANDOMKEY.
- **`src/rdb.cpp`** — RDB binary format reader/writer. Non-stream types use real Redis type bytes (Redis-loadable). Streams are encoded under a **PeaDB-specific type byte (200)** via `DataStore::dump_stream`/`restore_stream` (entries + last-id + consumer groups + full PEL), so `SAVE`/`BGSAVE`/PSYNC handle stream datasets — but a PeaDB RDB containing streams is NOT loadable by real Redis (cross-engine stream migration uses AOF/bridge). See `compat/delta.md` items 2 & 5.
- **`src/lua_engine.cpp`** — Lua 5.1 scripting (EVAL/EVALSHA/FCALL), wired back to `handle_command` for Redis call dispatch.
- **`include/command.hpp`** — `SessionState`, `BlockedState`, `WaitState` structs; all extern globals and callbacks shared between command.cpp and server.cpp.
- **`include/datastore.hpp`** — `DataStore` class and `Entry`. `Entry` is a **tagged union** (`std::variant` over the inline string + five heap-held collection payloads), so only the active type allocates: ~56 bytes + the active payload, down from the old fat struct that carried all six payloads at once (~328 B libc++ / ~456 B libstdc++). `expire_at_ms`, `lru`, and `string_force_raw` stay inline; typed accessors (`str()`/`hash()`/`list()`/`set()`/`zset()`+`zord()`/`stream_*()`) lazily materialize the active alternative, and a hand-written deep-copy ctor clones it (the `unique_ptr` members make the default copy ill-formed). `type()` is `payload.index()`. All payload access is confined to `src/datastore.cpp`.

## Key Conventions

- Anonymous namespace in `command.cpp` contains all helpers and globals. Public functions are defined after `} // namespace` (before `} // namespace peadb`).
- Globals in `command.cpp` anonymous namespace are accessible from public functions in the same file via the same-TU rule.
- `SessionState::fd` must be set before BLPOP/BRPOP block — used by `register_key_waiter`.
- Persistence: AOF uses a persistent fd (`g_aof_fd`) opened at startup. `fsync` policy controlled by `g_appendfsync` ("always"|"everysec"|"no"); failed fsyncs set `g_aof_write_error`. BGSAVE/BGREWRITEAOF fork and are reaped by `reap_background_children()`. Save points (`g_save_params`, parsed by `configure_save`) fire `do_bgsave_fork()` from `background_save_cron()` in the event loop; `SHUTDOWN [SAVE|NOSAVE]` saves on exit. Write commands are rejected with `-MISCONF` when `misconf_should_reject()` (failed bgsave + save points, or AOF write error) and `-OOM` when `oom_should_reject()` (`used_memory() > maxmemory`).
- Memory accounting: `DataStore::used_memory()` is a bounded-staleness cache over `estimate_used_memory()` (recomputed ≤ every 100 ms) so INFO and per-write maxmemory checks are O(1) amortised. `maxmemory` is parsed with units via `parse_memory()` / `configure_maxmemory()`.
- Robustness: server.cpp wraps dispatch in `safe_handle_command()` (one bad command can't kill the loop); `main.cpp` installs fatal-signal crash handlers (backtrace) + `std::set_terminate`; a corrupt AOF is fatal at startup while a truncated tail is tolerated.
- Observability: one timing wrapper at the dispatch chokepoint (`command.cpp`, around `spec.handler(...)`) feeds `CmdStats::usec_total` (INFO commandstats), `slowlog_add()`, and `latency_add()`. Keyspace notifications are emitted centrally by `emit_keyspace_notifications()` after a successful write (+ `"expired"` from `run_active_expire_cycle()`/`append_lazy_expire_dels`), gated by `g_notify_flags` (parsed from `notify-keyspace-events`). `MONITOR` uses `g_monitor_fds` (fed by `feed_monitors()`, cleared in `on_client_disconnect()`); `CLIENT KILL` goes through `g_client_kill_cb` (server.cpp owns fd close). INFO RSS via `process_rss_bytes()`, CPU via `getrusage`.
- Replication: REPLICAOF spawns a generation-tagged (`g_repl_generation`) supervisor thread that auto-reconnects with backoff via `replica_do_fullsync(host, port, gen)`; it stops when the generation changes (a newer REPLICAOF / NO ONE). The streaming loop checks the generation each ~1 s recv tick. Active expiry runs only on the master (`run_active_expire_cycle()` no-ops on a replica) and propagates `DEL` to the backlog. `noreplicas_should_reject()` gates writes on the real connected-replica count. Backlog size is `g_repl_backlog.size_` (set by `configure_repl_backlog_size`).
- Config precedence: config file is loaded first, CLI flags override.
- Security: `handle_command()` gates on `g_requirepass` — when set, unauthenticated sessions (`SessionState::authenticated`) get `-NOAUTH` unless the command is flagged `no_auth` (AUTH/HELLO/QUIT/RESET). Internally replayed/replicated commands bypass the gate via `g_replaying_aof`. `configure_security()` (called from `main.cpp` before the first `command_table()` build) wires `requirepass`/`masterauth`/`protected-mode`/`enable-module-command`/`rename-command`. protected-mode is enforced in the server accept loop (`protected_mode_enabled()`/`requirepass_is_set()`). `MODULE LOAD` requires `enable-module-command`. Protocol limits (`proto-max-bulk-len`, multibulk cap) live in `protocol.cpp`; the server closes the connection when `handle_command` sets `should_close` for a protocol error.

## Compatibility

`compat/delta.md` is the canonical status registry for known gaps vs Redis 7.2.5. `compat/commands.generated.md` is auto-generated by `scripts/ci/generate_compat_scoreboard.py`.

Feature status labels: `full` | `partial` | `surface` | `stub` | `experimental`.
