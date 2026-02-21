# PeaDB — Frequently Asked Questions

> Answers based on the current implementation (2025-02-21), ordered from high-level (for SWEs evaluating or migrating from Redis) through low-level internals (for senior SWEs who know Redis source code).

**Audiences:**

| Icon | Audience |
|------|----------|
| 🔄 | **SWE using Redis** — evaluating PeaDB as a replacement, wants practical migration & compatibility answers |
| 🔧 | **Senior SWE** — knows Redis internals, wants to understand PeaDB's architecture and implementation tradeoffs |

---

<!-- ═══════════════════════════════════════════ -->
## Compatibility & Migration 🔄
<!-- ═══════════════════════════════════════════ -->

Questions for engineers evaluating whether PeaDB can replace their existing Redis deployment.

---

## Is PeaDB a drop-in replacement for Redis?

**That is the explicit design goal — and it is close, with caveats.** PeaDB targets behavioral indistinguishability from Redis 7.2.5: same protocol, same commands, same error messages, same edge-case semantics. In practice, ~147 commands are implemented across core families, and standard client libraries work unmodified for supported features.

Known gaps that may affect drop-in replacement status:

| Area | Impact |
|------|--------|
| ACL engine | Only basic `AUTH` / default user; full ACL rules deferred |
| Pub/Sub | Stub — subscriptions are acknowledged but messages are not delivered |
| TLS | Not supported; plaintext only |
| Eviction policies | Hardcoded `noeviction`; no LRU/LFU/random eviction |
| RDB compatibility | Native RDB read/write is implemented, but not yet full Redis RDB coverage (edge cases/encodings still in progress) |
| Cluster slot migration | Stub-level; not production-ready |
| Modules ecosystem | Minimal `RedisModule_*` API; major modules not yet certified |

For read/write workloads using core data types (strings, hashes, lists, sets, sorted sets, streams) with transactions and Lua scripting, PeaDB is functionally compatible. Review [compat/delta.md](compat/delta.md) for the full list.

---

## What Redis version is PeaDB targeting?

**Redis 7.2.5** (commit `f60370ce28b946c1146dcea77c9c399d39601aaa`). All behavioral decisions, error messages, and edge-case semantics are validated against this exact version. The pin keeps the target deterministic and testable.

---

## Can I point my existing Redis client libraries at PeaDB without code changes?

**Yes.** PeaDB speaks RESP2 (with partial RESP3) on the same default port (`6379`). The following client libraries are explicitly tested:

- `redis-py` 5.0.1 (Python)
- `node-redis` 4.6.13 (Node.js)
- `go-redis/v9` 9.5.1 (Go)

`redis-cli`, `redis-benchmark`, `memtier_benchmark`, Jedis, Lettuce, and other RESP-speaking tools also work without modification. `COMMAND` and `INFO` output is shaped to satisfy client auto-discovery and handshake logic.

---

## Can PeaDB read my redis.conf?

**Yes, with a limited set of keys.** PeaDB's `load_config()` (in [src/config.cpp](src/config.cpp)) parses redis.conf-style files — `key value` lines, `#` comments. Pass it via `--config <path>`.

Recognized keys: `port`, `bind`, `loglevel`, `dir`, `dbfilename`, `appendonly`, `appendfilename`. Unknown keys are stored in a raw map but do not take effect. Most Redis config directives (e.g. `save 900 1`, `tcp-backlog`, `hz`, `slowlog-*`) are **silently ignored**.

Runtime changes via `CONFIG SET` modify in-memory state only — there is no `CONFIG REWRITE` to persist changes back to disk.

---

## Which Redis commands are unsupported or behave differently?

Most of the ~147 implemented commands are semantically identical to Redis. Notable differences:

- **Pub/Sub** (`PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`) — commands are accepted but message delivery is not implemented. `PUBLISH` always returns `0`.
- **`BGSAVE`** — uses `fork()` for background save, but child lifecycle/status tracking is minimal compared to Redis. `BGREWRITEAOF`, however, runs **synchronously** despite the name.
- **`AUTH` / `ACL`** — `AUTH` accepts credentials but does not enforce them (`OK`); `ACL SETUSER` is a stub and most ACL subcommands are not implemented.
- **`SLOWLOG`** — config keys exist (`slowlog-log-slower-than`, `slowlog-max-len`) but the slow log is never populated.
- **`OBJECT ENCODING`** — returns Redis-compatible encoding names but they do not reflect PeaDB's actual internal structures (e.g. PeaDB uses `std::deque` for lists but may report `listpack` or `quicklist`).
- **Cluster slot migration** (`CLUSTER SETSLOT MIGRATING/IMPORTING`) — routing enums are defined but the flow is not functional.
- **Sentinel introspection** — PeaDB does not respond to Sentinel-specific probing commands yet.

Commands **not** implemented include: full `ACL` subcommands, `LATENCY`, `MEMORY DOCTOR`, `MODULE` ecosystem beyond load/list/unload, and some less-common ops commands. See the [README supported commands list](README.md) for the full inventory.

---

## What are the known compatibility gaps?

Tracked in [compat/delta.md](compat/delta.md):

| Area | Severity | Current state |
|------|----------|---------------|
| ACL completeness | High | Basic `AUTH` / default user only; full multi-user ACL engine deferred |
| RDB parity gaps | Medium | Native RDB read/write exists; full opcode/encoding parity still in progress |
| AOF parity gaps | Medium | Core AOF works; edge-case parity in progress |
| Module ecosystem certification | High | Minimal `RedisModule_*` API; RedisJSON / RediSearch / etc. not yet certified |

Deviations from Redis behavior are treated as bugs unless explicitly documented as known deltas.

---

## Can I migrate live data from Redis to PeaDB with zero downtime?

PeaDB supports PSYNC2-based replication, which enables live migration:

1. **Start PeaDB** alongside the existing Redis master.
2. **`REPLICAOF <redis-host> <redis-port>`** — PeaDB connects as a replica and performs initial sync (currently via a background Python helper `scripts/redis/sync_from_redis.py`), then streams mutations in real time.
3. **Verify** replication lag is caught up (`INFO replication` on both sides).
4. **Promote PeaDB** — run `REPLICAOF NO ONE` on PeaDB, repoint application clients.

For RDB-based migration (offline or semi-online):

```bash
# Import an existing Redis RDB into PeaDB
python3 scripts/redis/import_rdb_via_redis.py <dump.rdb>

# Export PeaDB data back to an RDB
python3 scripts/redis/export_rdb_via_redis.py <output.rdb>
```

Native in-process full sync (no helper process) is planned but not yet implemented.

Native RDB parsing *is* implemented for common Redis RDB files; however, full parity is still in progress, so the bridge scripts remain the most robust option for edge cases.

---

## Can I roll back from PeaDB to Redis?

**Yes.** Two rollback paths:

1. **RDB export** — PeaDB writes Redis-readable RDB files for common cases via `SAVE` / `BGSAVE`. Copy the snapshot to a Redis instance and load it normally. You can also use `scripts/redis/export_rdb_via_redis.py`.
2. **Replication** — set up a Redis instance as a replica of PeaDB (PeaDB speaks PSYNC2). Once synced, promote Redis and repoint clients.

In practice, RDB export is the most reliable rollback path today. Replication interop aims to be Redis-compatible, but treat Redis-as-replica-of-PeaDB as "verify in your environment" until it is explicitly certified.

---

## Can PeaDB load my existing Redis RDB file?

**Yes, for common cases — but it is not yet full RDB parity.** PeaDB includes a native Redis RDB reader/writer (see `rdb_load()` / `rdb_save()`), and on startup it attempts to load the configured `dir/dbfilename` as an RDB file.

Known limitations (as of the current implementation): the RDB implementation focuses on the data types PeaDB supports and a subset of RDB encodings/opcodes. If you have exotic encodings, modules, or rarely-used RDB features, load may fail.

Bridge scripts are provided and remain the safest migration path for edge cases:

```bash
# Import a Redis RDB into PeaDB
python3 scripts/redis/import_rdb_via_redis.py <dump.rdb>

# Export PeaDB data as a Redis RDB
python3 scripts/redis/export_rdb_via_redis.py <output.rdb>
```

These scripts use a temporary Redis instance as an intermediary. Full Redis RDB parity (broader opcode/encoding coverage) is tracked for a future milestone.

---

## Does PeaDB support Redis Cluster protocol / Sentinel?

**Cluster: surface-level. Sentinel: not yet.**

Cluster support includes 16,384 hash-slot routing (`CLUSTER KEYSLOT`), `CLUSTER INFO/MYID/NODES/SLOTS`, `CLUSTER MEET` peer discovery, and a gossip bus on port N+10000. `MOVED` and `ASK` redirection enums are defined. However, full slot migration, config-epoch negotiation, and production cluster operation are **not yet functional**.

Sentinel compatibility is a requirement (PeaDB must be monitorable and electable by existing Redis Sentinel deployments), but the introspection commands Sentinel relies on are not yet implemented. PeaDB cannot participate in Sentinel-managed failover today.

---

<!-- ═══════════════════════════════════════════ -->
## Feature Compatibility 🔄
<!-- ═══════════════════════════════════════════ -->

Detailed answers on specific Redis features that matter for migration decisions.

---

## What Redis protocol coverage does PeaDB have?

**Broad RESP2 coverage with partial RESP3.** Implemented command groups:

| Category | Commands |
|----------|----------|
| **String** | GET, SET (NX/XX/EX/PX/KEEPTTL), GETDEL, GETEX, MGET, MSET, MSETNX, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, APPEND, STRLEN, SETRANGE, GETRANGE, SETBIT, GETBIT |
| **Hash** | HSET, HGET, HDEL, HLEN, HEXISTS, HGETALL, HKEYS, HVALS, HMSET, HMGET, HSETNX, HINCRBY, HINCRBYFLOAT, HSCAN |
| **List** | LPUSH, RPUSH, LPOP, RPOP (single-element form only; `LPOP/RPOP key count` not implemented), LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, BLPOP, BRPOP |
| **Set** | SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SSCAN |
| **Sorted Set** | ZADD (NX/XX/GT/LT/CH/INCR), ZRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANK, ZREVRANK, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZREM, ZPOPMIN, ZPOPMAX, ZSCAN |
| **Stream** | XADD, XLEN, XRANGE, XREVRANGE, XREAD, XREADGROUP, XGROUP CREATE/SETID, XDEL, XACK, XPENDING |
| **Key** | DEL, EXISTS, TYPE, RENAME, RENAMENX, COPY, SCAN, RANDOMKEY, MOVE, SWAPDB, OBJECT ENCODING, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME, PEXPIRETIME, UNLINK, KEYS |
| **Transactions** | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| **Scripting** | EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH |
| **Pub/Sub** | SUBSCRIBE, PUBLISH, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE |
| **Server** | PING, ECHO, INFO, CONFIG GET/SET, DBSIZE, FLUSHALL, FLUSHDB, SELECT, SAVE, BGSAVE, BGREWRITEAOF, CLIENT, COMMAND, WAIT, SHUTDOWN, DEBUG, TIME |
| **Replication** | REPLCONF, PSYNC (basic replica streaming) |
| **Cluster** | CLUSTER INFO/MYID/NODES/SLOTS/KEYSLOT (stub-level) |

### Not yet implemented

- Full RESP3 negotiation
- Sentinel introspection commands
- Full cluster slot migration (IMPORTING/MIGRATING)
- Full Redis Modules API (`RedisModule_*`) surface (currently a minimal subset)
- Many less-common commands

---

## Is RESP3 / HELLO supported?

**Basic RESP3 — yes.** The `HELLO` command is implemented: clients can negotiate `HELLO 2` or `HELLO 3` and PeaDB tracks the protocol version per session (`RespVersion::Resp2` / `RespVersion::Resp3`).

However, RESP3 support is **minimal in practice**. `HELLO 3` replies use RESP3 types (a map reply), but most command replies are still emitted in RESP2-style encodings. Full RESP3 type-distinct responses (maps, sets, doubles, booleans, big numbers as native types) are not yet wired through the entire command surface.

AUTH arguments passed via `HELLO 3 AUTH <user> <pass>` are accepted but not enforced (same stub behavior as `AUTH`).

---

## Do blocking commands (BLPOP, BRPOP, BLMOVE) work?

**Yes — all three are implemented.** `BLPOP`, `BRPOP`, and `BLMOVE` block the client when no data is immediately available. `BRPOPLPUSH` delegates to `BLMOVE RIGHT LEFT` internally.

If a key has data, the response is immediate. Otherwise the client enters a blocked state with a timeout. The server re-checks blocked clients after each `poll()` iteration (~100 ms). Timeout expiry returns a nil reply, matching Redis.

**Caveat**: wakeup is **polling-based**, not event-driven. When another client pushes to a key, blocked clients are not notified instantly — they discover the data at the next 100 ms poll cycle. Under Redis, `LPUSH` wakes blocked clients immediately. This may cause slightly higher latency for blocking pop patterns.

---

## Do Redis Streams and consumer groups fully work?

**Partially.** Core stream operations work; advanced features are incomplete:

| Feature | Status |
|---------|--------|
| XADD, XLEN, XRANGE, XREVRANGE | Fully implemented |
| XREAD (non-blocking) | Fully implemented |
| XREADGROUP (synchronous) | Works — reads from consumer group, acknowledges PEL entries |
| XGROUP CREATE / SETID | Implemented |
| XACK, XDEL | Implemented |
| XPENDING | **Summary only** — returns count + min/max IDs; per-consumer detail always returns empty |
| Blocking XREADGROUP | **Not implemented** — `BLOCK` option is parsed but silently ignored |
| PEL (Pending Entries List) | **Minimal** — `pending_to_consumer` map exists, but no idle time tracking, delivery count, or XCLAIM |
| XINFO | **Stub/diagnostic** — `XINFO STREAM <key>` returns a debug digest, not real Redis XINFO output |

If your workload depends on blocking consumer group reads (`XREADGROUP ... BLOCK`), stream monitoring (`XINFO`), or claim-based retry patterns (`XCLAIM`, `XAUTOCLAIM`), these are blocking gaps.

---

## Does Pub/Sub work?

**Stub only — messages are not delivered.** `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, and `PUNSUBSCRIBE` are accepted and acknowledged with proper RESP formatting, but no subscription tracking or message routing happens. `PUBLISH` always returns `0` (zero subscribers).

This means:

- Real-time messaging between clients does **not** work
- Keyspace notifications (`__keyevent@*__`, `__keyspace@*__`) do **not** fire
- Cache invalidation patterns built on Pub/Sub do **not** work

Full Pub/Sub with at-most-once delivery semantics (matching Redis) is planned.

---

## Does PeaDB support keyspace notifications?

**No.** The `notify_key_written()` function exists in the codebase but is a no-op stub. No `__keyspace@<db>__:*` or `__keyevent@<db>__:*` channel messages are published. The `notify-keyspace-events` config key is not implemented.

If your application relies on keyspace notifications for cache invalidation, TTL events, or audit trails, this is a blocking gap.

---

## Does PeaDB support client-side caching / CLIENT TRACKING?

**No.** `CLIENT TRACKING` is not implemented. The `INFO` output includes `tracking_clients:0` (hardcoded), but there is no tracking data structure, no invalidation message routing, and no `REDIRECT` support. Client-side caching protocols (RESP3 push invalidation or `BCAST` mode) do not work.

---

## Does PeaDB support MULTI/EXEC transactions?

**Yes — full optimistic-locking transactions.** `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` are all implemented.

- Commands between `MULTI` and `EXEC` are queued, not executed immediately.
- `WATCH` tracks a `mutation_epoch_` counter per key; if any watched key is modified before `EXEC`, the transaction is aborted (returns nil bulk).
- During `EXEC`, all queued commands run atomically (single-threaded — no interleaving possible).
- Replication events from a transaction are buffered separately and flushed as a unit.

---

## How does Lua scripting work?

PeaDB embeds the **real Lua 5.1 VM** (same version Redis uses) and supports:

| Feature | Status |
|---------|--------|
| `EVAL` / `EVALSHA` / `EVAL_RO` / `EVALSHA_RO` | Fully implemented |
| `SCRIPT LOAD / EXISTS / FLUSH / KILL` | Fully implemented |
| `FUNCTION LOAD / LIST / DELETE / DUMP / RESTORE / STATS / KILL` | Fully implemented |
| `FCALL` / `FCALL_RO` | Fully implemented |
| `redis.call()` / `redis.pcall()` | Fully implemented |
| Libraries: cjson, cmsgpack, struct, bit | All loaded |
| `redis.sha1hex()`, `redis.log()`, `redis.setresp()` | Available |
| Deterministic `math.random` seeding | Matches Redis |
| Script busy / kill semantics | Supported |

Scripts execute atomically (single-threaded — no interleaving). RESP3-aware type mapping is fully handled. For replication, non-deterministic commands invoked from scripts are rewritten to deterministic equivalents before propagation.

---

## Does PeaDB support Redis Modules?

**Minimally.** The following commands work: `MODULE LOAD`, `MODULE LIST`, `MODULE UNLOAD`. The initial ABI surface exposes:

- `RedisModule_OnLoad` / `RedisModule_OnUnload`
- `RedisModule_OpenKey`
- `RedisModule_StringSet` / `RedisModule_StringDMA`

This is enough to load simple test modules. Certification of popular third-party modules (RedisJSON, RediSearch, RedisTimeSeries, RedisBloom) is targeted for future milestones. Full binary compatibility with Redis's `RedisModule_*` C API is the end goal.

---

<!-- ═══════════════════════════════════════════ -->
## Operations & Deployment 🔄
<!-- ═══════════════════════════════════════════ -->

Practical answers for running PeaDB in development and production.

---

## How do I run PeaDB?

```bash
# Build from source
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
./build/peadb-server --port 6379 --bind 127.0.0.1

# Or via Docker Compose
docker compose up -d
```

Connect with any Redis client:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET hello world
OK
```

Configuration is via CLI flags or a config file (`--config <path>`), using the same syntax as Redis. The Docker image exposes port `6379` (configurable via `PEADB_PORT`), persists data to a named volume, and includes a health check.

---

## What OS and compiler does PeaDB require?

- **OS**: Linux (primary/production target). Minimum kernel 5.15+ for future `io_uring` support.
- **Compiler**: C++20 — GCC 11+ or Clang 14+.
- **Build system**: CMake ≥ 3.16.
- **Dependencies**: Lua 5.1 development libraries (`liblua5.1-0-dev` on Debian/Ubuntu).

Windows is explicitly a non-goal for production deployment.

---

## Does PeaDB support TLS?

**No.** There is no TLS support. All connections are plaintext. There are no `tls-port`, `tls-cert-file`, or `tls-key-file` configuration options.

For encrypted transport, use a TLS-terminating proxy (e.g. `stunnel`, Envoy, or a cloud load balancer) in front of PeaDB.

---

## What authentication / access control does PeaDB support?

**Minimal — `AUTH` is accepted but not enforced.** `AUTH` always returns `OK` regardless of the password provided. `ACL SETUSER` is similarly a no-op stub.

There is no `requirepass` enforcement, no multi-user ACL engine, no command-level permission filtering, and no audit logging. Any client that connects can execute any command.

The [project requirements](project-requirements.md) target full Redis ACL compatibility. Implementation is deferred to a later milestone (P4+). Until then, **rely on network-level isolation** (bind to localhost, firewall rules, VPC boundaries) to control access.

---

## What is the persistence model?

Two mechanisms:

| Mechanism | How it works |
|-----------|-------------|
| **RDB snapshots** | `rdb_save()` / `rdb_load()` — native RDB read/write for common Redis data types/encodings. `SAVE` blocks the event loop; `BGSAVE` forks a child process. |
| **AOF (Append-Only File)** | `append_aof()` opens the file, appends RESP-encoded commands, and closes it on every mutating command. `BGREWRITEAOF` rewrites synchronously (no background thread). |

PeaDB also has a legacy/native snapshot format (`save_snapshot_file()` / `load_snapshot_file()`), used as a fallback if RDB load fails.

**Important**: neither mechanism calls `fsync()`. See [durability guarantees](#what-are-peadbs-durability-guarantees-can-i-lose-writes-on-crash).

---

## How do I take snapshots / backups?

| Method | Command | Behavior |
|--------|---------|----------|
| **RDB snapshot** | `SAVE` | Blocks the event loop, writes an RDB-format file to disk |
| **Background RDB** | `BGSAVE` | Forks a child process; parent continues serving |
| **AOF rewrite** | `BGREWRITEAOF` | Rewrites the AOF file **synchronously** (blocks despite the name) |

Snapshots are written to the configured `--dir` directory with the filename from `--dbfilename` (default `dump.rdb`). The file is Redis-compatible and can be loaded by a real Redis instance.

For automated backups, schedule `BGSAVE` via cron and copy the resulting RDB file.

---

## How does AOF persistence work?

**Synchronous, foreground append.** Every mutating command is RESP-encoded and appended to the AOF file (default `appendonly.aof`). The file is opened and closed on each write — there is no persistent fd or batched flush.

- **Enable**: `--appendonly yes` or `CONFIG SET appendonly yes`.
- **Rewrite**: `BGREWRITEAOF` rewrites the file synchronously (no background thread or fork).
- **Replay**: On startup, if `appendonly` is enabled, the AOF file is replayed to reconstruct state.

Edge-case parity with Redis AOF is a known in-progress delta (see [compat/delta.md](compat/delta.md)).

---

## What happens on OOM? Does PeaDB evict, block, or crash?

**PeaDB rejects writes — it does not evict or crash.** When `maxmemory` is configured and the limit is exceeded, all write commands return:

```
-OOM command not allowed when used memory > 'maxmemory'.
```

Read commands continue to work. During `MULTI`/`EXEC`, if any queued write would violate the memory limit, the entire transaction is aborted.

There is **no key eviction**. The `maxmemory-policy` is hardcoded to `noeviction`.

---

## What are the supported eviction policies?

**Only `noeviction`** (hardcoded). `CONFIG GET maxmemory-policy` always returns `noeviction`. Attempting to `CONFIG SET maxmemory-policy` to another value is silently ignored.

Redis supports eight eviction policies (`noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`). PeaDB plans to implement these but has not done so yet. If your workload depends on automatic eviction, this is a blocking gap.

---

## How do I monitor PeaDB?

**No Prometheus `/metrics` endpoint.** PeaDB does not have an HTTP server or structured metrics export.

Monitoring is available via Redis-compatible commands:

| Command | What it provides |
|---------|-----------------|
| `INFO` | Server, clients, memory, persistence, stats, replication, CPU, keyspace, commandstats, errorstats sections |
| `INFO commandstats` | Per-command call counts and cumulative microseconds |
| `DBSIZE` | Key count per database |
| `CLIENT LIST` | Connected client details |
| `CONFIG GET` | Runtime configuration introspection |

For Prometheus integration, use community Redis exporters (e.g. `redis_exporter`) pointed at PeaDB — they scrape `INFO` output, which PeaDB formats in the expected Redis layout.

`SLOWLOG` config keys exist but the slow log is never populated — slow query tracking is not implemented.

---

## Does CONFIG SET persist changes to disk?

**No.** `CONFIG SET` modifies in-memory global variables (e.g. `g_config_maxmemory`, `g_config_min_replicas_to_write`) and returns `OK`, but there is no `CONFIG REWRITE` command and no mechanism to save runtime configuration changes to the config file. All `CONFIG SET` changes are lost on restart.

---

## How is PeaDB tested?

A **9-stage test pipeline** runs via `scripts/run_all_tests.sh`:

| Stage | What it does |
|-------|-------------|
| 1 | C++ unit tests via CTest |
| 2 | Python integration tests (~95 test files across all data types and features) |
| 3 | Command test coverage check |
| 4 | Compatibility delta validation |
| 5 | Fuzz sanity (RESP parser + command dispatcher fuzzing) |
| 6 | ASAN/UBSAN stability checks |
| 7 | Performance baseline regression |
| 8 | Sentinel / replication validation |
| 9 | Upstream Redis TCL test suite |

Differential tests (`tests/diff/`) compare PeaDB output against a real Redis instance command-by-command.

---

## Is there Kubernetes / Helm support?

**No.** There are no Kubernetes manifests, Helm charts, or operators provided. For Kubernetes deployment, containerize using the provided Dockerfile and manage with standard Kubernetes primitives (Deployment, Service, PVC for persistence).

---

<!-- ═══════════════════════════════════════════ -->
## Correctness, Durability & Performance 🔄🔧
<!-- ═══════════════════════════════════════════ -->

Questions relevant to both audiences — architects evaluating guarantees and SWEs benchmarking alternatives.

---

## What are PeaDB's durability guarantees? Can I lose writes on crash?

**Yes — writes can be lost on crash.** PeaDB does not call `fsync()` or `fdatasync()` anywhere in the codebase.

- **AOF mode**: Commands are appended via `std::ofstream`. The OS may buffer writes for seconds before flushing to disk. On a crash or power loss, all buffered writes are lost.
- **RDB mode**: Snapshots are point-in-time. Any mutations since the last `SAVE` / `BGSAVE` are lost on crash.
- **No persistence**: Without AOF or RDB enabled, all data is in-memory only and lost on any restart.

Redis offers `appendfsync always` (fsync every write — strongest durability) and `appendfsync everysec` (default — lose ~1 second). PeaDB currently provides durability weaker than Redis's default. This is a known gap targeted for improvement.

---

## Is PeaDB's replication synchronous or asynchronous?

**Asynchronous by default, with semi-synchronous semantics via `WAIT`.**

Mutating commands are appended to an in-memory replication buffer (`g_replication_events`) and flushed to connected replicas in the event loop. Replicas acknowledge receipt by reporting their replication offset.

The `WAIT` command provides semi-synchronous behavior: it blocks the client until `N` replicas have acknowledged a target offset, with a configurable timeout (polled at 10 ms granularity). The `min-replicas-to-write` config option can reject writes if insufficient replicas are connected.

There is no fully synchronous replication mode (every write fsynced on replica before ack).

---

## How does PeaDB handle split-brain in a cluster?

**It does not.** PeaDB has no split-brain detection, no quorum logic, and no network partition handling. There is no consensus protocol (Raft, Paxos) and no Sentinel integration.

In a simple master-replica topology, if both nodes believe they are master (e.g. after a network partition heals), there is no automatic conflict resolution. This must be handled externally — by Sentinel, an operator, or an orchestration layer.

---

## How does PeaDB perform compared to Redis?

Baseline numbers from `redis-benchmark` (50 concurrent connections, single PeaDB instance):

| Command | ops/sec | p50 | p99 |
|---------|---------|-----|-----|
| SET | 75,312 | 0.70 ms | 15.30 ms |
| GET | 76,481 | 0.36 ms | 5.98 ms |
| INCR | 75,392 | 0.63 ms | 12.42 ms |
| LPUSH | 75,585 | 0.66 ms | 9.35 ms |
| LPOP | 73,529 | 0.70 ms | 15.87 ms |
| SADD | 74,738 | 0.53 ms | 8.40 ms |
| ZADD | 75,041 | 0.46 ms | 5.10 ms |

Under heavier load (`memtier_benchmark`, 200 connections × 8 threads):

| Metric | Sets | Gets |
|--------|------|------|
| ops/sec | 17,999 | 17,973 |
| p50 | 37.1 ms | 37.1 ms |
| p99 | 155.6 ms | 154.6 ms |

Read and write throughput are roughly symmetric at low concurrency. Under high concurrency, the single-threaded architecture becomes the bottleneck — tail latencies increase significantly.

---

## What's the latency at p99 / p99.9?

From `memtier_benchmark` (200 connections × 8 threads, mixed workload):

| Percentile | Sets | Gets |
|------------|------|------|
| p50 | 37.1 ms | 37.1 ms |
| p99 | 155.6 ms | 154.6 ms |
| p99.9 | 248.8 ms | 252.9 ms |

From `redis-benchmark` (50 connections, moderate load):

| Percentile | SET | GET |
|------------|-----|-----|
| p50 | 0.70 ms | 0.36 ms |
| p99 | 15.30 ms | 5.98 ms |

Tail latency under heavy load is elevated because the single-threaded event loop serializes all work. Synchronous AOF writes and background expiry cycles can inject latency spikes.

---

<!-- ═══════════════════════════════════════════ -->
## Architecture & Internals 🔧
<!-- ═══════════════════════════════════════════ -->

Deep implementation details for senior SWEs who understand Redis internals and want to evaluate PeaDB's engineering tradeoffs.

---

## Is PeaDB multi-threaded?

**Mostly single-threaded on the command path.** The main server loop (`run_server()`) handles accept, read, parse, execute, and write sequentially in one thread. There is a detached helper thread used for Redis sync bootstrapping during `REPLICAOF`, but normal command execution remains single-threaded. The `DataStore` class uses no locks or synchronization whatsoever.

The [project requirements](project-requirements.md) call for a **sharded, single-thread-per-shard** model with lock-free inter-shard communication, but that architecture is not yet implemented.

---

## Is the data path lock-free?

**Trivially, yes — because there is only one thread.** There are no mutexes on the data path (the only mutex is `g_log_mutex` for serializing log output). The `DataStore` is a plain `std::unordered_map` with no concurrent-access protection. The design is "lock-free" by default, but only because concurrency does not exist yet. There are no lock-free data structures such as CAS-based queues or atomics-based inter-shard coordination.

---

## How does PeaDB implement its event loop / I/O model?

**Single `poll()` reactor.** One `poll()` call with a 100 ms timeout multiplexes all I/O in `run_server()`:

1. Accept new connections on the listener fd.
2. Read from client fds — parse RESP frames, execute commands, queue responses.
3. Write pending responses back to clients.
4. Accept/read/write on the cluster bus fd (port N+10000).
5. Flush the replication stream to connected replicas.
6. Run `active_expire_cycle()` to proactively expire keys.

There is no `epoll`, no `io_uring`, no per-core distribution. The fd set is a flat `std::vector<pollfd>` that is maintained persistently across iterations — entries are pushed on accept and erased on disconnect. This is the simplest possible reactor — functional but not optimized for high connection counts.

The requirements target `io_uring` with per-shard event loops, but the current implementation is intentionally simple.

---

## What data structures back each type?

PeaDB uses **16 databases** (index 0–15, matching Redis default), each backed by a plain `std::unordered_map<std::string, Entry>`.

Each `Entry` holds one of six value types, using straightforward STL containers instead of Redis's compact encodings:

| Redis type | PeaDB backing structure | Redis equivalent |
|------------|------------------------|------------------|
| String | `std::string` | SDS (simple dynamic string) |
| Hash | `std::unordered_map<string, string>` | ziplist / listpack → hashtable |
| List | `std::deque<string>` | listpack → quicklist |
| Set | `std::unordered_set<string>` | listpack → hashtable |
| Sorted Set | `std::unordered_map<string, double>` | listpack → skiplist + hashtable |
| Stream | `std::vector<pair<StreamID, fields>>` + consumer groups | rax tree + listpacks |

`OBJECT ENCODING` returns Redis-compatible encoding names (e.g. `listpack`, `quicklist`, `hashtable`, `skiplist`) regardless of actual internals — this is intentional to avoid breaking clients that branch on encoding names.

The requirements call for cache-friendly, open-addressing hash tables and NUMA-aware allocation, but these are future work.

---

## How does PeaDB handle key expiry — lazy, active, or hybrid?

**Hybrid, matching Redis's model:**

1. **Lazy expiry** — `expire_if_needed()` is called on every key access. If the key's TTL has passed, it is deleted before the command sees it.
2. **Active expiry** — `active_expire_cycle()` runs periodically in the event loop (each `poll()` iteration), iterating up to 64 keys per database (starting from `begin()` each time) and removing expired ones proactively. Unlike Redis, which genuinely random-samples the key space, PeaDB always iterates from the front of the `unordered_map`, so keys near the end of the iteration order are only reclaimed lazily.

TTL is stored as an absolute millisecond timestamp (`expire_at_ms`) in each entry's metadata. This matches Redis's approach of combining lazy deletion (correctness) with periodic active sampling (memory reclamation).

---

## What's the memory allocator and how does fragmentation get handled?

**Standard system `malloc` (glibc).** PeaDB does not link jemalloc, tcmalloc, or any custom allocator.

- `INFO MEMORY` reports `mem_fragmentation_ratio:1.00` — this value is **hardcoded**, not measured.
- There is no fragmentation monitoring, defragmentation pass, or active memory management beyond what glibc's allocator provides.
- The Lua VM uses a custom allocator callback that wraps `std::malloc` / `std::realloc` / `std::free`.

The requirements call for custom slab allocators, per-shard memory arenas, NUMA-aware allocation, and inline small-string optimization. None of these are implemented. For production workloads with high key churn, glibc malloc fragmentation may become an issue — linking jemalloc (via `LD_PRELOAD`) is a reasonable workaround.

---

## How does replication work?

PeaDB implements **PSYNC2-based replication**:

- **Role switching** via `REPLICAOF` / `SLAVEOF` commands.
- **REPLCONF** handshake including `capa eof`, `capa psync2`, `listening-port`, `GETACK`, and `ACK`.
- **Streaming replication**: mutating commands are appended to in-memory `g_replication_events` and flushed to connected replicas in the event loop.
- **WAIT**: blocks until `N` replicas acknowledge a target replication offset (polled with 10 ms granularity).
- **Deterministic rewriting**: commands like `INCRBYFLOAT` are rewritten to `SET key value KEEPTTL` before propagation to guarantee state convergence.
- **Transaction replication**: `MULTI`/`EXEC` blocks are captured in a separate buffer and flushed atomically.

Initial full-sync from a Redis master is performed via a background thread running `scripts/redis/sync_from_redis.py`. Native in-process full-sync is planned but not yet implemented.

---

## How is RESP parsed? Is it zero-copy?

**Not zero-copy.** The parser lives in [src/protocol.cpp](src/protocol.cpp) (`parse_one_command()`), which dispatches to `parse_resp_array()` for standard RESP2 `*N\r\n$N\r\n…` frames and `parse_inline()` for plain-text commands.

The input buffer is a `std::string` per client. Parsing uses `buffer.substr()` extensively — each argument is copied into a new `std::string`. There is no shared ownership, no `string_view`, and no arena-based allocation.

**Partial reads** are handled correctly: if the buffer doesn't contain a complete frame (e.g. `find("\r\n") == npos` or insufficient bytes for a bulk string), the parser returns `std::nullopt` and unconsumed data remains in the buffer for the next `read()`.

**Pipelining** is supported: the server loop calls `parse_one_command()` in a `while(true)` loop, processing multiple commands from a single `read()` and erasing consumed bytes with `input.erase(0, parsed->consumed)`. However, **replies are not batched** — each command's response is written via a separate `write()` syscall, so pipelining saves on read syscalls but not write syscalls.

---

## How does command dispatch work?

PeaDB uses a **static `std::unordered_map<std::string, CommandSpec>`** returned by `command_table()` in [src/command.cpp](src/command.cpp). Each `CommandSpec` contains the command name, arity, flags (e.g. `"write"`, `"readonly"`, `"fast"`), first/last/step key indices, and a `std::function` handler lambda.

Dispatch in `handle_command()`:

1. Uppercase the command name.
2. Look it up in the hash map (`table.find(cmd)`).
3. If not found, check `g_module_commands` (dynamically registered module commands).
4. Validate arity, check OOM for write commands, handle MULTI queueing.
5. Invoke `spec.handler(args, session, should_close)`.

New commands are added by inserting `t.emplace("CMD_NAME", CommandSpec{...})` entries in the static initializer. This is simple but means adding a command requires modifying a single ~4,500-line file. There is no external command registration DSL or code generation.

---

## Does PeaDB do incremental rehash like Redis?

**No.** PeaDB uses `std::unordered_map` for both the main key space and all collection types. Rehashing is delegated entirely to the C++ standard library, which performs **all-at-once rehashing** when the load factor exceeds the threshold.

Redis uses a custom `dict` with two hash tables and incrementally moves buckets from `ht[0]` to `ht[1]` across multiple operations to avoid latency spikes. PeaDB has no equivalent — a large `unordered_map` rehash will cause a latency spike proportional to the number of keys. For datasets with millions of keys, this can be significant.

There are no custom hash tables, no incremental migration, and no rehashing callbacks.

---

## How does the sorted set handle range queries without a skiplist?

**O(N log N) per range query.** PeaDB's sorted set is backed by `std::unordered_map<std::string, double>` — a hash map from member to score. There is no secondary index by score.

When `ZRANGE` (or `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`) is called, the implementation copies the entire hash map into a `std::vector<pair<string, double>>`, sorts it by score (then lexicographically by member for ties), and slices the requested range.

| Operation | PeaDB complexity | Redis complexity |
|-----------|-----------------|------------------|
| ZADD | O(1) avg | O(log N) |
| ZRANGE by index | O(N log N) | O(log N + M) |
| ZRANGEBYSCORE | O(N log N) | O(log N + M) |
| ZSCORE | O(1) | O(1) |
| ZRANK | O(N log N) | O(log N) |

Redis uses a skiplist + hashtable dual structure that maintains score order at insertion time. PeaDB defers ordering to query time, which is asymptotically worse for range queries on large sorted sets. This is a known area for optimization.

---

## How does SCAN cursor work on `std::unordered_map`?

**Simple ordinal index — not rehash-safe.** The SCAN implementation in [src/datastore.cpp](src/datastore.cpp) iterates the `unordered_map` from `begin()`, skipping `cursor` entries, then emitting up to `COUNT` matches:

```
for (auto it = db.begin(); it != db.end(); ++it) {
    if (expired(it)) continue;          // expired keys do not advance idx
    if (idx++ < cursor) continue;
    if (!matches_pattern_and_type(it)) continue;
    out.push_back(it->first);
    if (++emitted >= count) break;
}
return next_cursor = (idx >= db.size()) ? 0 : idx;
```

The cursor is just a sequential counter. (The snippet above captures the structure; the actual implementation also skips expired keys before incrementing `idx`, meaning the cursor counts only live keys.) **This is not safe across rehashing** — if `std::unordered_map` rehashes between SCAN calls (due to insertions triggering bucket expansion), the iteration order can change unpredictably. Keys may be missed or returned twice.

Redis uses reverse-binary-increment cursors that are stable across incremental rehashing. PeaDB does not implement this. For small datasets or one-shot `SCAN 0 COUNT <large>` calls, this is fine. For incremental scanning of a large, actively-mutated dataset, results may be inconsistent.

---

## How do blocking commands (BLPOP/BRPOP) wake up?

**Polling, not event-driven.** When `BLPOP` / `BRPOP` / `BLMOVE` finds no data, the client enters a `blocked` state with a deadline. The server's main `poll()` loop (100 ms timeout) calls `try_unblock_client()` on every blocked session each iteration.

`try_unblock_client()` checks:
1. Has the timeout expired? → return nil reply.
2. Can we `lpop` / `rpop` / `lmove` from the watched key? → return the data.

There is **no event-driven wakeup**. The `notify_key_written()` function exists but is a no-op. When another client pushes to a key, blocked clients discover the data at the next poll cycle (~100 ms worst-case), not immediately.

Redis wakes blocked clients synchronously during the `LPUSH` / `RPUSH` command execution. PeaDB's approach adds up to 100 ms of latency to blocking pop patterns.

---

## How does WRONGTYPE error checking work?

PeaDB uses **template helper functions** for type-safe command dispatch. Six helpers are defined in [src/command.cpp](src/command.cpp):

- `with_string_key(key, fn)` — checks `is_wrongtype_for_string(key)` before invoking `fn`
- `with_hash_key(key, fn)`, `with_list_key(...)`, `with_set_key(...)`, `with_zset_key(...)`, `with_stream_key(...)`

Each underlying `is_wrongtype_for_*()` method in [src/datastore.cpp](src/datastore.cpp) does: look up the key → call `expire_if_needed()` → if the key exists and its type doesn't match, return true.

The error reply is the standard Redis message: `WRONGTYPE Operation against a key holding the wrong kind of value`. This ensures commands operating on the wrong type fail early with the correct error, matching Redis behavior.

---

## How does BGSAVE fork() work?

**Direct `fork()` + `_exit()`.** The `BGSAVE` handler in [src/command.cpp](src/command.cpp):

1. Calls `fork()`.
2. **Child process**: calls `rdb_save(g_snapshot_path, err)` to write the RDB file, then `_exit(0)` or `_exit(1)`.
3. **Parent process**: records the save time, returns `"Background saving started"`.

**No `waitpid()`** — the parent does not track the child PID, check completion status, or reap the zombie process. The child relies on the OS (or `SIGCHLD` default handling) to clean up.

Redis's `BGSAVE` is more sophisticated: it tracks `server.child_pid`, calls `waitpid()` in the `serverCron()` timer, reports completion via `INFO persistence` (`rdb_bgsave_in_progress`, `rdb_last_bgsave_status`), and prevents concurrent background saves. PeaDB has none of this — multiple `BGSAVE` calls can fork concurrent children writing to the same file.

---

## How is the replication offset computed?

**Per-byte RESP encoding of the original command.** After every successful write command, the master replication offset is incremented:

```cpp
g_master_repl_offset += encode_command_resp(args).size();
```

This matches Redis's approach — the offset represents the byte position in the replication stream. The same `encode_command_resp(args)` output is pushed to replicas as the replication event.

Note: `encode_command_resp()` is called twice for every write command — once to compute the offset increment and once to append to `g_replication_events`. This is redundant serialization that could be optimized.

Replicas report their offset back via `REPLCONF ACK <offset>`, which is used by the `WAIT` command to determine if replicas have caught up.

---

## How does PeaDB handle pipeline batching?

**Reads are batched, writes are not.** When a client pipelines multiple commands, the server reads them in a single `read()` call (or multiple, depending on TCP framing). The server loop then parses and executes commands in a `while(true)` loop:

```
read(fd, buf) → append to client buffer
while (parse_one_command(buffer)):
    execute command
    write(fd, response)   // one syscall per command
    erase consumed bytes
```

Each command's response is written immediately via a separate `write()` syscall. There is no output buffering that coalesces pipeline responses into a single `write()`. For a 10-command pipeline, PeaDB makes 10 `write()` syscalls where an optimized implementation would make 1.

Redis buffers responses in a per-client output buffer and flushes once per event loop iteration, achieving syscall coalescing naturally. PeaDB's write-per-command approach adds syscall overhead under pipelining.

---

## What's the codebase structure and size?

~10,600 lines of C/C++ across `src/` and `include/`:

| File | Lines | Role |
|------|-------|------|
| [src/command.cpp](src/command.cpp) | ~4,564 | Command table, handlers, dispatch, replication logic |
| [src/datastore.cpp](src/datastore.cpp) | ~1,762 | Key-value storage, type operations, expiry, SCAN |
| [src/rdb.cpp](src/rdb.cpp) | ~1,269 | RDB save/load, snapshot format |
| [src/lua_engine.cpp](src/lua_engine.cpp) | ~996 | Lua VM embedding, script/function execution |
| [src/server.cpp](src/server.cpp) | ~256 | Event loop, connection management, replication flush |
| [src/protocol.cpp](src/protocol.cpp) | ~84 | RESP parsing |
| [src/config.cpp](src/config.cpp) | ~64 | Config file loading |
| [src/main.cpp](src/main.cpp) | ~99 | Entry point, CLI arg parsing |
| Lua stubs (4 files) | ~1,100 | cjson, cmsgpack, struct, bit C stubs |
| Headers (6 files) | ~400 | Interfaces and type definitions |

The core server logic — everything needed to accept connections, parse RESP, execute commands, persist data, and replicate — fits in ~9,500 lines of C++ (excluding Lua C stubs). This is roughly 1/30th the size of Redis's `src/` directory (~300K lines), reflecting the simpler architecture and use of STL containers instead of custom data structures.

---

## Summary

PeaDB today is a **functionally rich but architecturally simple** single-threaded `poll()`-based server. For SWEs evaluating it as a Redis replacement: core data type operations, transactions, Lua scripting, and basic replication work — but Pub/Sub, eviction, TLS, ACLs, and keyspace notifications are missing. For senior SWEs: the implementation is intentionally straightforward (~10K LoC using STL containers), with known algorithmic gaps (sorted set range queries, SCAN cursors, blocking command wakeup) that trade correctness-at-scale for implementation speed. The jump to the target architecture — sharded, `io_uring`, lock-free inter-shard, custom data structures — is the major engineering milestone ahead.
