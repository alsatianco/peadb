# Changelog

All notable changes to PeaDB are documented here. PeaDB targets drop-in compatibility with
Redis 7.2.5; see [`compat/delta.md`](compat/delta.md) for the authoritative per-feature gap
registry and [`docs/compatibility.md`](docs/compatibility.md) for the drop-in posture.

The format follows [Keep a Changelog](https://keepachangelog.com/); versioning is
[SemVer](https://semver.org/). Until 1.0.0 the API/on-disk formats may still change.

## [Unreleased]

### Added — production-readiness pass (from `AI-temp/analysis-opus.md`)
- **Key eviction (`maxmemory-policy`).** All eight Redis modes — `noeviction`,
  `allkeys-lru`/`-lfu`/`-random`, `volatile-lru`/`-lfu`/`-random`/`-ttl` — via approximate
  sampling (`maxmemory-samples`, default 5). Over `maxmemory`, keys are evicted (counted in
  `evicted_keys`, propagated as `DEL` to replicas/AOF, `evicted` keyspace event) instead of
  rejecting writes. Config via file / `CONFIG SET` / `--maxmemory-policy` / `--maxmemory-samples`.
  PeaDB can now serve as a cache.
- **Replication safety.** Randomized per-process replication id; `replid2`/`second_repl_offset`
  rotation on `REPLICAOF NO ONE` promotion (PSYNC2 partial-resync against history);
  `sync_full`/`sync_partial_ok`/`sync_partial_err` in `INFO`; `WAIT` now injects
  `REPLCONF GETACK *` so it resolves in ~1 RTT instead of up to a second.
- **RDB integrity.** The trailing CRC64 is verified on startup disk loads; a corrupt native
  (`REDIS`-magic) dump is fatal rather than a silent fallback. (Replication-stream RDBs skip
  CRC — a foreign master's CRC variant may differ.)
- **Protocol/commands.** Quote/escape-aware inline command parsing (`sdssplitargs` semantics);
  `ZRANGESTORE` `BYSCORE`/`BYLEX` (with `REV` + `LIMIT`).
- **Connection hygiene.** `SO_KEEPALIVE` on client sockets; idle-client `timeout` reaping
  (replicas, blocked/`WAIT`/`MULTI` clients and pub/sub subscribers exempt).
- **Per-class `client-output-buffer-limit`** (`normal`/`slave`/`pubsub`, hard/soft/soft-seconds)
  — a slow consumer or lagging replica is disconnected instead of OOMing the process.
- **Lazyfree.** `UNLINK` of a large value and `FLUSHALL`/`FLUSHDB ASYNC` detach the data and
  destruct it on a background thread, so huge-key deletes don't stall the event loop.

### Changed
- **Blocking-command latency floor removed.** `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`XREAD` wake
  in ~0.5 ms (was ~100 ms) — the blocked-client recheck now also runs after processing
  readable fds in the same loop iteration.
- **Memory model.** `Entry` is a tagged union (`std::variant`); only the active type allocates.
  `sizeof(Entry)` ~328 B → ~56 B; accounted bytes/key ~3.9×, real RSS ~32% lower for the
  small-key workload.

### Earlier (6-batch hardening)
Real `AUTH`/`requirepass` + `protected-mode`, Lua `os`/`io` removed, `MODULE LOAD` gating,
`rename-command`, protocol/query-buffer caps; exception boundary + crash report; real
`maxmemory` enforcement with unit parsing; save points + `SHUTDOWN SAVE` +
`stop-writes-on-bgsave-error`; AOF truncated-tail tolerance + `appendfsync` fsync-error
checks; replication auto-reconnect + `masterauth` + replica-expiry propagation +
`min-replicas-to-write` correctness + configurable backlog; SLOWLOG/LATENCY/cmdstats,
keyspace notifications, accurate `INFO`, `MONITOR`/`CLIENT KILL`/runtime `loglevel`;
HyperLogLog, `SMOVE`, `*_RO` variants, RESP3 encoders; stream RDB persistence + replication,
full PEL, `XCLAIM`/`XAUTOCLAIM`/`XSETID`, blocking `XREAD`.

### Deferred (documented in `compat/delta.md`)
TLS, multi-user ACL, automatic failover (Sentinel/Raft), multi-core (I/O threads + sharding),
`ServerContext` global-elimination refactor, byte-exact Redis RDB v11 (incl. stream listpack
layout), compact listpack/intset encodings, jemalloc/`activedefrag`, multi-part `appendonlydir`
AOF, diskless replication sync, skiplist zset (O(log N) rank), `CLIENT TRACKING`, sharded pub/sub.
