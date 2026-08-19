# PeaDB Compatibility & Migration

PeaDB targets **Redis 7.2.5** (commit `f60370ce28b946c1146dcea77c9c399d39601aaa`); behavior, error messages, and edge cases are validated against that exact version. [`../compat/delta.md`](../compat/delta.md) is the **canonical, always-current registry** of known gaps — this guide is a high-level orientation, not a substitute for it.

## Is it a drop-in replacement?

For core workloads — strings, hashes, lists, sets, sorted sets, streams, transactions, Lua scripting, and pub/sub — yes. PeaDB speaks RESP2 (and basic RESP3) on port `6379`, and standard clients work unmodified. Explicitly tested clients: `redis-py` 5.0.1, `node-redis` 4.6.13, `go-redis/v9` 9.5.1. `redis-cli`, `redis-benchmark`, `memtier_benchmark`, Jedis, and Lettuce also work.

## Configuration

PeaDB parses redis.conf-style files (`key value` lines, `#` comments) — either redis-server style (first positional argument) or `--config <path>`; CLI flags override file values. Recognized keys are a subset of Redis's (the full table lives in the [README Configuration section](../README.md#configuration)); unknown directives are accepted but ignored. `CONFIG SET` changes are in-memory only; there is no `CONFIG REWRITE`, so runtime changes do not survive a restart.

## Live migration & rollback

1. Start PeaDB beside the existing Redis master.
2. Run `REPLICAOF <redis-host> <redis-port>` — PeaDB performs a native in-process PSYNC2 full sync (RDB load, then a continuous command stream).
3. Wait until `INFO replication` shows the replica caught up.
4. Run `REPLICAOF NO ONE` on PeaDB and repoint application clients.

To **roll back**, export a Redis-readable RDB with `SAVE`/`BGSAVE` (or `scripts/redis/export_rdb_via_redis.py`) and load it into Redis. For offline import, use the bridge scripts:

```bash
python3 scripts/redis/import_rdb_via_redis.py <dump.rdb>     # Redis RDB -> PeaDB
python3 scripts/redis/export_rdb_via_redis.py <output.rdb>   # PeaDB -> Redis RDB
```

The bridge scripts use a temporary Redis instance and remain the most robust path for exotic encodings while native RDB parity is hardened. **Streams** now persist and replicate within PeaDB (PSYNC full-sync handles stream datasets), but PeaDB's stream RDB encoding is PeaDB-specific — a real `redis-server` cannot load a PeaDB RDB that contains streams, so cross-engine stream migration still goes through AOF or the bridge scripts (see delta.md item 5).

## Feature status at a glance

See [`../compat/delta.md`](../compat/delta.md) for authoritative, per-feature detail.

| Works | Partial / surface | Not yet |
|-------|-------------------|---------|
| Strings, hashes, lists, sets, sorted sets, streams (RDB + replication) | Streams — full PEL, blocking XREAD/XREADGROUP, XCLAIM/XAUTOCLAIM, `XINFO STREAM/GROUPS/CONSUMERS`; consumer seen/active times don't survive an RDB round-trip | TLS (use a TLS-terminating proxy) |
| Transactions — `MULTI`/`EXEC`/`WATCH` (optimistic locking) | RESP3 — `HELLO` negotiates; `HGETALL` returns maps and the map/set/double/bool encoders exist, but many replies are still RESP2-shaped | Multi-user ACL rules |
| Pub/Sub — messages delivered; `PUBLISH` returns the receiver count | Cluster — routing, `KEYSLOT`, `MEET`; gossip + full slot migration in progress | Compact listpack/intset encodings (`OBJECT ENCODING` cosmetic); jemalloc/`activedefrag` |
| Key eviction — all 8 `maxmemory-policy` modes (approximate LRU/LFU/random/TTL sampling) | | |
| Lua — `EVAL`/`EVALSHA`, full `FUNCTION` subsystem (LOAD/LIST/DUMP/RESTORE/STATS), `FCALL`/`FCALL_RO` | Replication — PSYNC2 (randomized replid + replid2 on promotion) + fast `WAIT` (GETACK) + auto-reconnect; no automatic failover or diskless sync | Client-side caching (`CLIENT TRACKING`) |
| RDB + AOF persistence | Modules — `MODULE LOAD`/`LIST`/`UNLOAD` + a minimal `RedisModule_*` subset | Sentinel |

## Persistence & durability

PeaDB's durability matches Redis's options.

- **RDB snapshots** — `SAVE` (blocking) and `BGSAVE` (forked) write a native Redis-compatible RDB to `--dir`/`--dbfilename`. The writer fsyncs the file and its parent directory and fails the save on any fsync error rather than leaving a corrupt snapshot. On startup the trailing **CRC64 is verified**: a corrupt native RDB (`REDIS` magic, bad checksum) is fatal — the server refuses to start rather than silently falling back to an empty/legacy load. (Replication-stream RDBs skip CRC, since a foreign master's CRC variant differs; that path is integrity-checked by TCP + length framing.)
- **AOF** — enable with `--appendonly yes`; commands append to a persistent fd. The `appendfsync` policy governs durability:

  | `appendfsync` | Behavior | Worst-case loss on crash |
  |---------------|----------|--------------------------|
  | `always` | fsync after every write | none (matches Redis) |
  | `everysec` *(default)* | fsync ~once per second | ~1 second of writes |
  | `no` | flushing left to the OS | OS-buffer dependent |

  `BGREWRITEAOF` forks a child to rewrite, then the parent atomically renames the result over the live file. `appendfsync always`/`everysec` now check the `fsync()` return and flag a write error (which `stop-writes-on-bgsave-error` acts on) rather than silently swallowing a failed sync. On startup a **truncated AOF tail** is tolerated (logged, `aof-load-truncated` default yes) but **mid-file corruption is fatal** — the server refuses to start rather than serve partial data.
- **Save points** — `save <seconds> <changes>` (config or `CONFIG SET save`, repeatable; `save ""` disables) trigger a background `BGSAVE` when enough keys changed within the window, and `SHUTDOWN [SAVE|NOSAVE]` saves on a clean shutdown when save points are configured. The default ships **no** save points (set them, or enable AOF, to persist) — so an out-of-the-box instance with neither is non-durable, exactly like Redis started with `save ""` and `appendonly no`.
- **Streams** — persist via both **RDB** (entries + consumer groups + full PEL, PeaDB-internal encoding) and AOF, and replicate through PSYNC. A PeaDB RDB containing streams is not loadable by real Redis (PeaDB-specific stream type); use AOF/bridge for cross-engine stream migration.

## Operational notes

- **Security** — default-user authentication is enforced: set `requirepass` (config/CLI/`CONFIG SET`) and unauthenticated clients get `-NOAUTH` for every non-`no_auth` command; `AUTH` / `HELLO … AUTH` validate with a constant-time compare. **protected-mode** is on by default, so a server with no password refuses non-loopback clients. `MODULE LOAD` (which runs arbitrary native code) is gated behind `enable-module-command` (default off), `rename-command` can neutralise dangerous verbs, and `proto-max-bulk-len`/multibulk + query-buffer caps bound a malicious client's memory. `ACL WHOAMI`/`USERS`/`LIST`/`CAT`/`GETUSER`/`GENPASS`/`HELP` truthfully report the single `default` user (password shown as `#<sha256-hex>` of `requirepass`, or `nopass`); `ACL SETUSER`/`DELUSER`/`LOAD`/`SAVE`/`LOG`/`DRYRUN` refuse with an error naming the gap rather than applying, discarding, or faking a rule. **Still missing:** a multi-user **ACL** rule engine and **TLS** — terminate TLS at a proxy and keep multi-tenant isolation at the network layer until both land.
- **Memory pressure & eviction** — `maxmemory` enforces a real byte limit (with `100mb`/`2gb` unit parsing in both config and `CONFIG SET`). When used memory exceeds it, PeaDB **evicts** keys per `maxmemory-policy` — all eight Redis modes: `noeviction` (default), `allkeys-lru`/`-lfu`/`-random`, and `volatile-lru`/`-lfu`/`-random`/`-ttl`. Eviction uses approximate sampling (`maxmemory-samples`, default 5) over a random bucket walk across all DBs (or only TTL-bearing keys for `volatile-*`); the chosen victim is evicted, counted in `evicted_keys`, propagated as a `DEL` to replicas and the AOF, and emits an `evicted` keyspace notification. With `noeviction` (or when a `volatile-*` policy has no eligible keys) denyoom writes are rejected with `-OOM …` instead. Used memory is a bounded-staleness estimate (recomputed at most ~every 100 ms; the eviction loop keeps it live as it frees, so it bounds memory under sustained load rather than over-evicting). LRU/LFU metadata is approximate (a coarse access clock / log-increment counter touched on the command's primary key). `stop-writes-on-bgsave-error` (default on) additionally rejects writes with `-MISCONF` when a background save or AOF write is failing.
- **Monitoring** — there is no HTTP `/metrics` endpoint, but the standard tooling works: `INFO` (community exporters such as `redis_exporter` scrape it) now reports real `blocked_clients`, `used_memory_rss`/`mem_fragmentation_ratio`, `used_memory_peak`, `used_cpu_*`, `latest_fork_usec`/`total_forks`, `instantaneous_ops_per_sec`, and `pubsub_channels`. `SLOWLOG` is **populated** (real per-command timing via `slowlog-log-slower-than`/`slowlog-max-len`), `INFO commandstats` reports real `usec`/`usec_per_call`, `LATENCY LATEST`/`HISTORY`/`RESET`/`DOCTOR` work (gated by `latency-monitor-threshold`), `MONITOR` streams live commands, `CLIENT LIST`/`CLIENT KILL` (by `ID`/`ADDR`) and runtime `CONFIG SET loglevel` work. **Keyspace notifications** (`notify-keyspace-events`, `__keyspace@`/`__keyevent@`, incl. `expired`) are implemented.
- **Connections** — accepted client sockets get `TCP_NODELAY` + `SO_KEEPALIVE` (so half-open peers are detected and reaped). An idle-client `timeout` (seconds; config/CLI/`CONFIG SET`, default 0 = off) closes clients idle past the window; replicas, blocked/`WAIT`ing/`MULTI` clients and pub/sub subscribers are exempt. `client-output-buffer-limit` is enforced per class (`normal`/`slave`/`pubsub`) with Redis-default hard/soft/soft-seconds (normal unlimited; replica 256mb/64mb/60; pubsub 32mb/8mb/60): a slow consumer or lagging replica whose buffer exceeds its limit is disconnected instead of OOMing the single process.
- **TLS** — not supported; terminate TLS at a proxy (stunnel, Envoy, a cloud load balancer).
- **OS** — Linux is the primary target (kernel 5.15+ enables the optional io_uring backend); GCC 11+/Clang 14+ and CMake ≥ 3.16. Windows is a non-goal for production.
