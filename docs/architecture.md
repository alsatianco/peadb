# PeaDB Architecture

How PeaDB works internally and how to develop it. For build/test commands and code conventions, see [`../CLAUDE.md`](../CLAUDE.md); for compatibility status, see [`../compat/delta.md`](../compat/delta.md).

## Execution model

PeaDB runs a **single-threaded event loop** (like Redis). One thread accepts connections, reads and parses requests, dispatches commands, and writes replies. The loop is built on an abstracted `Poller` (`include/poller.hpp`) backed by **epoll** on Linux and **kqueue** on macOS, with an optional **io_uring** backend compiled in when `PEADB_HAVE_IO_URING` is defined. The poller waits with a 100 ms timeout, which also bounds background housekeeping (active expiry, child reaping, blocked-client rechecks, deferred `WAIT`).

Besides the event-loop thread there are two helper threads: a short-lived detached helper used during `REPLICAOF` full-sync, and a **lazyfree** worker that destructs large detached objects (`UNLINK`, `FLUSHALL`/`FLUSHDB ASYNC`) off the loop. Both preserve the single-writer invariant: command processing and `DataStore` mutation happen only on the event-loop thread, and the lazyfree worker only ever touches data that was first *moved out* of the live keyspace (exclusive ownership transferred under a mutex-guarded queue), so there is no fine-grained locking on the hot path and no race with live data.

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

- **`Entry` is a tagged union.** Each key's value is an `Entry` holding a `std::variant` over the string payload (inline, SSO) plus five heap-held collection payloads (`unique_ptr<HashT/ListT/SetT/ZSetT/StreamT>`), so **only the active type allocates**. The variant alternatives are ordered to match `ValueType`, so `Entry::type()` is just `payload.index()`. Inline fields accessed regardless of type — `expire_at_ms`, an `lru` clock/counter (reserved for eviction), and `string_force_raw` — sit outside the variant. Typed accessors (`str()`, `hash()`, `list()`, `set()`, `zset()`/`zord()`, `stream_*()`) lazily materialize the active alternative on the get-or-create paths; a hand-written deep-copy ctor/assign clones the active payload (a `unique_ptr` member makes the compiler-default copy ill-formed) so `COPY`/`RENAME`/`SWAPDB`/MULTI snapshots keep value semantics. This shrank `sizeof(Entry)` from ~328 B (libc++; ~456 B libstdc++, when every key carried all six empty payloads) to ~56 B, cutting accounted bytes/key ~3.9× and real RSS ~32% for the small-key workload. All payload access lives in `src/datastore.cpp`; `command.cpp`/`rdb.cpp`/`lua_engine.cpp` go through `DataStore` methods.
- **Strings / hashes / lists / sets** — STL-backed (hash maps, `std::deque` for lists).
- **Sorted sets** — a dual structure: an `unordered_map<member,score>` for O(1) lookup plus a `std::map` keyed on `{score,member}` as an ordered score index, so `ZRANGE`/`ZRANGEBYSCORE` iterate the index instead of copy-and-sort.
- **Streams** — in-memory entries + consumer-group PEL; core ops work, advanced PEL features are partial.
- `OBJECT ENCODING` synthesizes Redis-style encoding names from element counts/sizes to mimic Redis's encoding-transition thresholds.

## Key expiry

Hybrid **lazy + active**. Lazy: expired keys are dropped on access. Active: `active_expire_cycle()` runs at most every ~100 ms, scanning up to 64 keys per database starting from a **randomly chosen bucket** (no front-bias / tail starvation).

## Eviction (maxmemory-policy)

When `used_memory()` exceeds `maxmemory` and a policy other than `noeviction` is set, the write gate (`oom_should_reject`) runs `evict_to_fit()` before rejecting: it repeatedly calls `DataStore::evict_one(policy, samples)` until memory is back under budget or nothing is evictable. `evict_one` samples up to `maxmemory-samples` (default 5) candidate keys via the same random-bucket walk `RANDOMKEY` uses — across **all** DBs for `allkeys-*`, restricted to TTL-bearing keys for `volatile-*` — and picks the victim by the policy: oldest access clock (LRU), lowest frequency counter (LFU), soonest TTL (`volatile-ttl`), or random. Per-key LRU/LFU metadata rides in the inline `Entry::lru` word (a coarse ms clock, or an 8-bit log-increment counter), touched on `DataStore::get()` and at the dispatch chokepoint for the command's primary key — an O(1), first-key approximation. Each eviction is propagated like a server-issued deletion (a `DEL` to the replication backlog and AOF, plus an `evicted` keyspace notification) and counted in `evicted_keys`. `evict_one` decrements the cached `used_memory` as it frees so the loop sees memory fall in real time instead of over-evicting against the ~100 ms-stale cache; eviction runs only on a master and never during AOF replay.

## SCAN

The main-keyspace `SCAN` is bucket-based and advances the cursor with **reverse-binary-increment**, making it resilient to rehashing across calls (a monotonic non-wrapping path covers non-power-of-two bucket counts). Per-collection `HSCAN`/`SSCAN`/`ZSCAN` use simpler ordinal cursors.

## Persistence

- **RDB** — `SAVE` blocks; `BGSAVE` forks a child while the parent keeps serving. The writer fsyncs the file **and** its parent directory and fails the save on any fsync error. `BGSAVE` tracks the child PID, reaps it via `waitpid(WNOHANG)` in the loop, rejects concurrent saves, and reports `rdb_bgsave_in_progress` / `rdb_last_bgsave_status` via `INFO`.
- **AOF** — appends to a persistent fd opened once at startup. `appendfsync` is `always` (fsync per write), `everysec` (default; fsync ~1/s), or `no`. `BGREWRITEAOF` forks a child that writes a temp file; the parent appends the diff buffer and **atomically renames** over the live AOF, surfacing failures via `aof_last_bgrewrite_status`.
- **Streams + RDB** — streams are RDB-encoded under a PeaDB-specific type byte (entries + last-id + consumer groups + full PEL), so `SAVE`/`BGSAVE` and PSYNC full-sync handle stream datasets. The encoding is PeaDB-internal (not Redis's `RDB_TYPE_STREAM_LISTPACKS_3` byte layout), so a PeaDB RDB containing streams is not loadable by a real `redis-server`; cross-engine stream migration uses AOF or the bridge scripts. Streams also persist through AOF (replayed as `XADD`s).

## Replication

`REPLICAOF` runs a **native in-process PSYNC2 client** (`replica_do_fullsync`): TCP connect → `PING` → `REPLCONF` → `PSYNC` → RDB load → continuous command stream, with `+CONTINUE` partial resync served from a **configurable circular backlog** (`g_repl_backlog`, tracked by `g_master_repl_offset`). The **replication id is randomized per process** (`init_replication_id`), so unrelated instances never share a replid and can't be wrongly partial-resynced against a foreign stream; on promotion (`REPLICAOF NO ONE`) the old id rotates into `replid2`/`second_repl_offset` so a former sibling can still partial-resync against history. `INFO` reports `sync_full`/`sync_partial_ok`/`sync_partial_err`. Replicas emit `REPLCONF ACK` ~once per second, and a client `WAIT` injects `REPLCONF GETACK *` into the stream so replicas ACK within ~1 RTT instead of waiting for the next heartbeat. `min-replicas-to-write` can reject writes. A generation-tagged supervisor **auto-reconnects** with backoff; automatic failover (Sentinel/Raft) is not yet implemented.

## Request/response handling

The parser slices the per-client input buffer as `string_view`. All replies for the commands parsed in one read are **batched into a single `WriteBuf`** and flushed with one `enqueue_output()`; partial writes register `EPOLLOUT` interest for backpressure. Blocking commands (`BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`XREAD`) are served promptly: every successful write sets a pending-wakeup flag, and the loop rechecks blocked (and `WAIT`ing) clients **again after processing readable fds in the same iteration** — so a write and the unblock it triggers land together (sub-millisecond), instead of slipping to the next ~100 ms poll. (Cross-thread/eventfd wakeup for replica-applied writes and a per-key waiter index are possible refinements, deferred as diminishing-return.)

## Design principles & roadmap

PeaDB treats **Redis as the executable specification**: where behavior is undocumented, Redis source defines truth, and deviations are tracked as explicit deltas in [`../compat/delta.md`](../compat/delta.md). Guiding rules: compatibility over elegance, determinism over cleverness, and continuous differential validation against a real Redis (`tests/diff/`).

The current implementation is the single-threaded baseline. The **target architecture** (not yet built) is a sharded, single-thread-per-shard model with lock-free inter-shard communication, io_uring-based async persistence, and locality-friendly custom allocators — preserving identical external semantics while scaling across cores. The `shard_loop.cpp` scaffolding (`crc16`, `key_hash_slot`, `ShardLoop`) is the first step.
