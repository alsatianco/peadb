# PeaDB Architecture

How PeaDB works internally and how to develop it. For build/test commands and code conventions, see [`../CLAUDE.md`](../CLAUDE.md); for compatibility status, see [`../compat/delta.md`](../compat/delta.md).

## Execution model

PeaDB runs a **single-threaded event loop** (like Redis). One thread accepts connections, reads and parses requests, dispatches commands, and writes replies. The loop is built on an abstracted `Poller` (`include/poller.hpp`) backed by **epoll** on Linux and **kqueue** on macOS, with an optional **io_uring** backend compiled in when `PEADB_HAVE_IO_URING` is defined. The poller waits with a 100 ms timeout, which also bounds background housekeeping (active expiry, child reaping, blocked-client rechecks, deferred `WAIT`).

The only other thread is a short-lived detached helper used during `REPLICAOF` full-sync; command processing and `DataStore` mutation always happen on the one event-loop thread, so there is no fine-grained locking on the hot path.

## Source layout

| File | Responsibility |
|------|----------------|
| `src/command.cpp` | All 207 command handlers + the `command_table()` registry and `handle_command()` dispatcher (~7.3k LoC) |
| `src/server.cpp` | Event loop, client sessions (`SessionState`), output buffering (`WriteBuf`), pub/sub delivery, blocked clients, deferred `WAIT` |
| `src/datastore.cpp` | In-memory store: 16 databases, all data types, expiry, SCAN cursors |
| `src/rdb.cpp` | RDB binary reader/writer |
| `src/lua_engine.cpp` | Lua 5.1 scripting (`EVAL`/`EVALSHA`/`FCALL`), dispatched back through `handle_command` |
| `src/protocol.cpp` | RESP parsing (operates on `std::string_view`; only argument values are copied) |
| `src/config.cpp` | Config file + CLI flag parsing |
| `include/poller.hpp` | epoll / kqueue / io_uring abstraction |
| `src/shard_loop.cpp` | Sharding scaffolding only — not wired (`PEADB_NUM_SHARDS` defaults to 1) |

## Data structures

Internal representations differ from Redis but preserve observable semantics:

- **Strings / hashes / lists / sets** — STL-backed (hash maps, `std::deque` for lists).
- **Sorted sets** — a dual structure: an `unordered_map<member,score>` for O(1) lookup plus a `std::map` keyed on `{score,member}` as an ordered score index, so `ZRANGE`/`ZRANGEBYSCORE` iterate the index instead of copy-and-sort.
- **Streams** — in-memory entries + consumer-group PEL; core ops work, advanced PEL features are partial.
- `OBJECT ENCODING` synthesizes Redis-style encoding names from element counts/sizes to mimic Redis's encoding-transition thresholds.

## Key expiry

Hybrid **lazy + active**. Lazy: expired keys are dropped on access. Active: `active_expire_cycle()` runs at most every ~100 ms, scanning up to 64 keys per database starting from a **randomly chosen bucket** (no front-bias / tail starvation).

## SCAN

The main-keyspace `SCAN` is bucket-based and advances the cursor with **reverse-binary-increment**, making it resilient to rehashing across calls (a monotonic non-wrapping path covers non-power-of-two bucket counts). Per-collection `HSCAN`/`SSCAN`/`ZSCAN` use simpler ordinal cursors.

## Persistence

- **RDB** — `SAVE` blocks; `BGSAVE` forks a child while the parent keeps serving. The writer fsyncs the file **and** its parent directory and fails the save on any fsync error. `BGSAVE` tracks the child PID, reaps it via `waitpid(WNOHANG)` in the loop, rejects concurrent saves, and reports `rdb_bgsave_in_progress` / `rdb_last_bgsave_status` via `INFO`.
- **AOF** — appends to a persistent fd opened once at startup. `appendfsync` is `always` (fsync per write), `everysec` (default; fsync ~1/s), or `no`. `BGREWRITEAOF` forks a child that writes a temp file; the parent appends the diff buffer and **atomically renames** over the live AOF, surfacing failures via `aof_last_bgrewrite_status`.
- **Streams + RDB** — streams are RDB-encoded under a PeaDB-specific type byte (entries + last-id + consumer groups + full PEL), so `SAVE`/`BGSAVE` and PSYNC full-sync handle stream datasets. The encoding is PeaDB-internal (not Redis's `RDB_TYPE_STREAM_LISTPACKS_3` byte layout), so a PeaDB RDB containing streams is not loadable by a real `redis-server`; cross-engine stream migration uses AOF or the bridge scripts. Streams also persist through AOF (replayed as `XADD`s).

## Replication

`REPLICAOF` runs a **native in-process PSYNC2 client** (`replica_do_fullsync`): TCP connect → `PING` → `REPLCONF` → `PSYNC` → RDB load → continuous command stream, with `+CONTINUE` partial resync served from a **1 MiB circular backlog** (`g_repl_backlog`, tracked by `g_master_repl_offset`). Replicas emit `REPLCONF ACK` ~once per second. `WAIT` is resolved once per loop iteration (so ~100 ms granularity), and `min-replicas-to-write` can reject writes. Automatic reconnect/failover is not yet implemented.

## Request/response handling

The parser slices the per-client input buffer as `string_view`. All replies for the commands parsed in one read are **batched into a single `WriteBuf`** and flushed with one `enqueue_output()`; partial writes register `EPOLLOUT` interest for backpressure. Blocking commands (`BLPOP`/`BRPOP`/`BLMOVE`) use poll-based wakeup — a write sets a pending-wakeup flag and blocked clients are rechecked on the next loop iteration (up to ~100 ms latency), rather than being signalled synchronously.

## Design principles & roadmap

PeaDB treats **Redis as the executable specification**: where behavior is undocumented, Redis source defines truth, and deviations are tracked as explicit deltas in [`../compat/delta.md`](../compat/delta.md). Guiding rules: compatibility over elegance, determinism over cleverness, and continuous differential validation against a real Redis (`tests/diff/`).

The current implementation is the single-threaded baseline. The **target architecture** (not yet built) is a sharded, single-thread-per-shard model with lock-free inter-shard communication, io_uring-based async persistence, and locality-friendly custom allocators — preserving identical external semantics while scaling across cores. The `shard_loop.cpp` scaffolding (`crc16`, `key_hash_slot`, `ShardLoop`) is the first step.
