# Compatibility Delta

Date: 2026-05-10
Redis pin: 7.2.5 (`f60370ce28b946c1146dcea77c9c399d39601aaa`)

This file is the **canonical status registry** for PeaDB compatibility gaps.
README links should reference this document rather than repeating status.

Feature status labels:
- `full` — passes integration + differential tests including error shape
- `partial` — integration tests pass; differential or edge cases missing
- `surface` — command is registered and returns plausible output; no test coverage
- `stub` — command is registered but not implemented or returns hardcoded response
- `experimental` — implemented but not validated against Redis test suite

## Known Deltas

1. ACL completeness
- Status: intentional temporary delta for early milestones M1-M3.
- Owner: security-compat
- Severity: high
- Target milestone: P4
- Implemented: basic AUTH/default user flow.
- Missing: full ACL user/rules engine and ACL command family behavior.
- Tracking task: `M8` and later security-hardening milestones.

2. Redis RDB binary compatibility
- Status: temporary delta.
- Owner: persistence-compat
- Severity: high
- Target milestone: P3
- Implemented:
  - Native in-process RDB reader/writer for common Redis data types (see `rdb_load()` / `rdb_save()` usage at startup and in `SAVE`/`BGSAVE`).
  - RDB import/export interoperability via Redis bridge scripts
    (`scripts/redis/import_rdb_via_redis.py`, `scripts/redis/export_rdb_via_redis.py`).
  - Legacy PeaDB snapshot format as a fallback for local persistence.
  - Save points: `save <sec> <changes>` (config/`CONFIG SET`, repeatable; `save ""` disables)
    trigger a background BGSAVE via the event-loop cron; `SHUTDOWN [SAVE|NOSAVE]` saves on a
    clean shutdown. `stop-writes-on-bgsave-error` (default on) rejects writes with `-MISCONF`
    after a failed background save when save points are configured.
- Missing: full Redis RDB parity (broader opcode/encoding coverage, edge-case behavior, and rarely-used features). Default ships no save points (set them or enable AOF for durability).
- Tracking: ongoing RDB parity hardening.

3. AOF binary/semantic parity gaps
- Status: temporary delta.
- Owner: persistence-compat
- Severity: medium
- Target milestone: P3
- Implemented: AOF append + startup replay + rewrite for supported commands.
  BGREWRITEAOF uses a parent-owns-rename design: the child writes a temp AOF and exits
  without renaming; the parent appends the diff buffer and only then atomically renames
  over the live AOF. Any failure (256 MiB diff cap, write/rename error, child crash)
  unlinks the temp file and leaves the old AOF intact on disk. Failure is surfaced via
  `INFO persistence aof_last_bgrewrite_status:err`; operators should rerun BGREWRITEAOF
  after addressing the cause (typically by reducing write pressure during the rewrite).
  Durability hardening: `appendfsync always`/`everysec` check the `fsync()` return and
  set the AOF write-error flag instead of silently swallowing a failed sync; startup
  tolerates a truncated AOF tail (`aof-load-truncated`, default yes, logged) but treats
  mid-file corruption as fatal (refuses to start); AOF rewrite serialises zset scores
  with `%.17g` (round-trip) instead of `std::to_string` (which lost precision each rewrite).
- Missing: full Redis-equivalent behavior/edge-case parity across entire command space.
- Tracking tasks: broader compatibility hardening beyond current milestone.

4. Modules ecosystem certification
- Status: temporary delta.
- Owner: modules-compat
- Severity: high
- Target milestone: P6
- Implemented: minimal `MODULE LOAD/LIST/UNLOAD`, `RedisModule_OnLoad`
  invocation, and key/command API subset validated by integration tests.
- Missing: certification of popular external modules (RedisJSON, RediSearch,
  RedisTimeSeries, RedisBloom) without modification.
- Tracking: post-M8 expansion of `RedisModule_*` API surface.

5. Stream RDB encoding
- Status: `partial` (PeaDB-internal RDB works; not Redis byte-compatible)
- Owner: persistence-compat
- Severity: medium
- Target milestone: P2
- Implemented: streams now serialize to/from RDB (entries + last-id + consumer groups +
  full PEL with delivery time/count) under a PeaDB-specific type byte (200). SAVE/BGSAVE
  succeed with stream keys, and PSYNC full-resync replicates stream-containing datasets,
  so RDB persistence and replication bootstrap work end-to-end within PeaDB. Full PEL
  model drives XPENDING/XACK/XCLAIM/XAUTOCLAIM; XSETID/XTRIM (MAXLEN) and blocking
  XREAD/XREADGROUP are implemented. AOF persistence of streams remains available too.
- Missing: Redis's exact RDB_TYPE_STREAM_LISTPACKS_3 byte layout — a real redis-server
  cannot load a PeaDB RDB that contains streams (cross-engine stream migration still uses
  the AOF/bridge path). Ties to the broader Redis-RDB-v11 parity track (item 2).
- Tracking: P2-1 done (PeaDB-internal); byte-compat folded into RDB parity hardening.

6. Cluster bus gossip protocol
- Status: `stub`
- Owner: cluster-compat
- Severity: medium
- Target milestone: P4
- Implemented: CLUSTER MEET/INFO/NODES/SLOTS/KEYSLOT, MOVED/ASK redirection,
  slot migration basics, PONG gossip reply on cluster bus port.
- Missing: full binary gossip protocol (MEET/PING/PONG/FAIL messages); cluster bus
  communication is PONG-only — nodes do not broadcast topology updates to each other.
- Tracking: long-term cluster milestone.

7. AUTH / ACL enforcement
- Status: `partial` (default-user AUTH enforced; multi-user ACL engine still missing)
- Owner: security-compat
- Severity: high
- Target milestone: P3
- Implemented: real default-user authentication. `requirepass`/`masterauth` via config file,
  CLI flags, and `CONFIG GET/SET`; a dispatch-time gate returns `-NOAUTH Authentication required.`
  for unauthenticated clients (commands flagged `no_auth` — AUTH/HELLO/QUIT/RESET — are exempt);
  `AUTH` and `HELLO <proto> AUTH <user> <pass>` perform a constant-time compare and return
  `-WRONGPASS` on mismatch and the "no password is set" error when `requirepass` is empty.
  protected-mode (default on) refuses non-loopback clients when no password is set. Hardening:
  `MODULE LOAD` is gated behind `enable-module-command` (default off, blocks the dlopen RCE
  vector); `rename-command FROM TO` (TO empty disables) lets operators neutralise dangerous
  verbs; `proto-max-bulk-len` (512 MB) + 1M-element multibulk caps and a 1 GB per-client
  query-buffer cap close the unbounded-allocation / OOM DoS; the Lua sandbox now removes
  `os`/`io`. ACL SETUSER/GETUSER/LIST/LOG/CAT remain registered stubs.
- Missing: full multi-user ACL rule evaluation (users, key patterns, command categories,
  pub/sub channels) and TLS — both deferred as dedicated XL tracks.
- Tracking: security hardening milestone.

8. REPLICAOF in-process streaming
- Status: `partial` (functional + auto-reconnect; no automatic failover)
- Owner: replication-compat
- Severity: medium
- Target milestone: P3-reconnect
- Implemented: In-process TCP client (`replica_do_fullsync`). Does PING → (AUTH masterauth
  if set) → REPLCONF → PSYNC ? -1 → RDB load → continuous command stream. **Automatic
  reconnect**: a generation-tagged supervisor reconnects with exponential backoff (0.5→30 s)
  on link loss, sets `master_link_status:down` while disconnected, and stops promptly on a
  newer REPLICAOF (incl. NO ONE). **Replica expiry correctness**: replicas no longer
  actively self-expire — the master propagates a DEL for each actively-expired key, so
  clocks can't diverge. **min-replicas-to-write** now compares against the actual connected
  replica count (was: rejected whenever the knob was > 0). Backlog size is configurable via
  `repl-backlog-size` (default 1 MiB). Master supports +CONTINUE partial resync; replica
  emits REPLCONF ACK every ~1 s.
  Note: full-resync fails if master dataset contains stream keys (RDB stream encoding
  not yet implemented; PSYNC returns -ERR and replica does not sync).
  Operator-visible behaviour during full-sync RDB load: client writes on the replica
  return -READONLY and reads return MASTERDOWN if `replica-serve-stale-data no` is set.
  `INFO persistence loading:1` indicates that the event-loop is currently applying the
  full-sync RDB snapshot; clients should retry after `loading:0`.
  RDB load itself runs on the event-loop thread (handed off from the replication TCP
  thread), so DataStore mutations are never concurrent with client command processing.
- Missing: real full-state plain `SYNC` (still sends an empty RDB; PSYNC serializes the
  full dataset incl. streams, so PeaDB's own replicas are unaffected — plain SYNC for
  external clients is deferred as low-value); per-replica `slaveN:` INFO lines and
  RTT-granularity WAIT via master-initiated `REPLCONF GETACK` (deferred refinements);
  automatic failover/Sentinel (deferred XL track). (Stream replication bootstrap now works —
  PSYNC resyncs stream datasets via the stream RDB encoding, item 5.)
- Tracking: P2-2 done; P3-1 done (backlog, now configurable); auto-reconnect + masterauth +
  replica-expiry propagation + min-replicas-correctness done; failover deferred.

9. Sharding (shard_loop.cpp)
- Status: `stub` (scaffolding only, not wired)
- Owner: future-work
- Severity: n/a (future work)
- Target milestone: P5
- Implemented: `crc16`, `key_hash_slot`, `ShardLoop`, `shard_init` defined.
  PEADB_NUM_SHARDS CMake option defaults to 1 (single-threaded, unchanged behaviour).
- Missing: Actual multi-shard event loops, listener distribution, cross-shard routing.
  Prerequisites: full P3-3 ServerContext migration. (P4-1 `Entry` tagged-union variant is
  **done** — `Entry` now allocates only the active payload; see `docs/architecture.md`.)
- Tracking: P4-7 in improvement plan.

10. HyperLogLog byte-format compatibility
- Status: `partial` (functional; not byte-compatible with Redis HYLL)
- Owner: datatypes-compat
- Severity: low
- Target milestone: P5
- Implemented: `PFADD`/`PFCOUNT`/`PFMERGE` over a self-consistent dense representation
  (8-byte magic + 16384 one-byte registers). Estimates are standard-accuracy (~1% at
  16384 registers, with linear-counting small-range correction). `PFCOUNT` over multiple
  keys merges first. WRONGTYPE is returned for non-HLL string values.
- Missing: Redis's exact `HYLL` sparse/dense byte layout and hash function, so HLL values
  do not cross-migrate (DUMP/RESTORE/RDB) to/from real Redis, and `PFCOUNT` results are not
  bit-identical to Redis for the same inputs.
- Tracking: ties to RDB parity (item 2).

11. RESP3 reply-shape coverage
- Status: `partial`
- Owner: protocol-compat
- Severity: medium
- Target milestone: P4
- Implemented: `HELLO 2`/`3` negotiation; RESP3 type encoders (`encode_map`/`encode_set`/
  `encode_double`/`encode_bool`, plus RESP3 null `_`); `HGETALL` returns a RESP3 map.
- Missing: routing the remaining map/double/set/push replies through the RESP3 encoders
  (`CONFIG GET`, `XPENDING`, `XINFO`, `CLIENT INFO`, `ZSCORE`/`ZADD INCR`, etc.) and
  RESP3 push frames for client-side caching invalidation.
- Tracking: incremental protocol hardening.
