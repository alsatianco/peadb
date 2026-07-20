# Compatibility Delta

Redis pin: 7.2.5 (`f60370ce28b946c1146dcea77c9c399d39601aaa`)

This file is the **canonical status registry** for PeaDB compatibility gaps.
README links should reference this document rather than repeating status.

Feature status labels:
- `full` — passes integration + differential tests including error shape
- `partial` — integration tests pass; differential or edge cases missing
- `surface` — command is registered and returns plausible output; no test coverage
- `stub` — command is registered but not implemented or returns hardcoded response
- `experimental` — implemented but not validated against Redis test suite

## Verification (the parity ratchet)

Parity is measured by two CI-gated, monotonically-improving numbers:

- **Upstream TCL pass-rate** — the official Redis 7.2.5 test suite runs in *native
  mode* (the harness spawns `peadb-server` as a redis-server stand-in via
  `SUITE_MODE=native scripts/redis/run_redis_tests.sh`). Per-suite results are parsed
  by `scripts/redis/parse_tcl_results.py` into `artifacts/redis-parity/pass_rate.json`;
  the rate is `Σok / Σ(ok + err + skipfile-lines)`, so skipping a test never improves
  the number. Adopted suites and their baselines live in
  `compat/redis_suite_ledger.json`.
- **Scoreboard `full` count** — `compat/commands.generated.md` statuses are
  proof-based (`full` = exercised by a green upstream suite AND differentially
  covered; see the generator header).

Ratchet rules (`scripts/ci/check_suite_ratchet.py`): any `[err]` in an adopted suite
fails; lowering a ledger count or growing a skipfile fails unless the same change
touches this file. Every skipfile line under `scripts/redis/skipfiles/` is a tracked
debt with a comment naming its delta item — the skipfiles themselves are the
authoritative debt list; each item below cites its skipfile where one exists.

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
  XREAD/XREADGROUP are implemented. AOF rewrite round-trips streams command-for-command
  (Redis `rewriteStreamObject` shape): empty streams are recreated with the `XADD MAXLEN 0`
  trick, `XSETID … ENTRIESADDED … MAXDELETEDID` restores last-id/entries-added/max-deleted
  even after XDEL or trim, and consumer groups + PELs are rebuilt via `XGROUP CREATE` +
  `XCLAIM … JUSTID FORCE`.
- Missing: Redis's exact RDB_TYPE_STREAM_LISTPACKS_3 byte layout — a real redis-server
  cannot load a PeaDB RDB that contains streams (cross-engine stream migration still uses
  the AOF/bridge path). Ties to the broader Redis-RDB-v11 parity track (item 2). AOF-replayed
  PEL delivery time/count are approximate (the `XCLAIM TIME`/`RETRYCOUNT` options are
  accepted but not yet applied on replay); the RDB path preserves them exactly.
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
  protected-mode (default on) refuses non-loopback clients when no password is set. Auth state
  follows Redis's `clientSetDefaultAuth`: a client is authenticated at connect time iff no
  password is set, so a later `CONFIG SET requirepass` never retroactively de-authenticates
  open connections, and `RESET` re-derives the flag from the current password. Hardening:
  `MODULE LOAD` is gated behind `enable-module-command` (default off, blocks the dlopen RCE
  vector); `rename-command FROM TO` (TO empty disables) lets operators neutralise dangerous
  verbs; the RESP parser matches Redis framing byte-for-byte (`proto-max-bulk-len` 512 MB bulk
  cap, `INT_MAX` multibulk cap with incremental arg reads, a 64 KB single-line desync limit, and
  a 1 GB per-client query-buffer cap close the unbounded-allocation / OOM DoS); unauthenticated
  clients get Redis's tighter pre-auth caps (>10 multibulk elements or >16 KB bulk are refused
  before buffering); the Lua sandbox now removes `os`/`io`. ACL SETUSER/GETUSER/LIST/LOG/CAT
  remain registered stubs.
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
  emits REPLCONF ACK every ~1 s. **Randomized replid** per process (`init_replication_id`)
  so unrelated instances never share an id and can't be wrongly +CONTINUE'd; on promotion
  (`REPLICAOF NO ONE`) the old id rotates into `replid2`/`second_repl_offset` and PSYNC
  accepts a partial resync against that history. `INFO` reports `sync_full`/
  `sync_partial_ok`/`sync_partial_err`. **WAIT** injects `REPLCONF GETACK *` so replicas
  ACK within ~1 RTT instead of on the ~1 s heartbeat.
  Operator-visible behaviour during full-sync RDB load: client writes on the replica
  return -READONLY and reads return MASTERDOWN if `replica-serve-stale-data no` is set.
  `INFO persistence loading:1` indicates that the event-loop is currently applying the
  full-sync RDB snapshot; clients should retry after `loading:0`.
  RDB load itself runs on the event-loop thread (handed off from the replication TCP
  thread), so DataStore mutations are never concurrent with client command processing.
- Missing: real full-state plain `SYNC` (still sends an empty RDB; PSYNC serializes the
  full dataset incl. streams, so PeaDB's own replicas are unaffected — plain SYNC for
  external clients is deferred as low-value); per-replica `slaveN:` INFO lines; diskless
  fork-based RDB streaming for full resync (today the snapshot serializes on the event-loop
  thread); automatic failover/Sentinel (deferred XL track). (Stream replication bootstrap
  works — PSYNC resyncs stream datasets via the stream RDB encoding, item 5.)
- Tracking: P2-2 done; P3-1 done (backlog, now configurable); auto-reconnect + masterauth +
  replica-expiry propagation + min-replicas-correctness + randomized replid/replid2 +
  WAIT GETACK done; failover + diskless sync deferred.

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
- Implemented: `HELLO 2`/`3` negotiation with a dynamic reply (real proto/id/role);
  RESP3 type encoders (`encode_map`/`encode_set`/`encode_double`/`encode_bool`, plus
  RESP3 null `_`); `HGETALL` returns a RESP3 map. The RESP2 subscribe-mode command
  gate is now enforced (only (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT/RESET while
  subscribed; PING replies as a 2-element array), closing the old unenforced-gate gap.
- Missing: routing the remaining map/double/set/push replies through the RESP3 encoders
  (`CONFIG GET`, `XPENDING`, `XINFO`, `CLIENT INFO`, `ZSCORE`/`ZADD INCR`, etc.) and
  RESP3 push frames for client-side caching invalidation.
- Tracking: incremental protocol hardening.

12. Key eviction (maxmemory-policy)
- Status: `full` (approximate, like Redis — sampling-based, not exact LRU/LFU)
- Owner: memory-eviction
- Severity: n/a (implemented)
- Target milestone: P4
- Implemented: all eight `maxmemory-policy` modes — `noeviction`, `allkeys-lru`/`-lfu`/
  `-random`, `volatile-lru`/`-lfu`/`-random`/`-ttl` — via `DataStore::evict_one()` +
  `evict_to_fit()` at the OOM write gate. Approximate sampling (`maxmemory-samples`,
  default 5) over the `randomkey()` bucket walk across all DBs (or TTL-only for
  `volatile-*`). Eviction runs both at the OOM write gate and from the event-loop cron
  (`run_eviction_cycle`, mirroring Redis's `startEvictionTimeProc`), so lowering `maxmemory`
  below usage via `CONFIG SET` evicts in the background without waiting for the next write.
  Per-key LRU/LFU metadata in the inline `Entry::lru` word, touched on
  `get()` + the dispatch chokepoint (first key). Each eviction propagates `DEL` to the
  backlog + AOF, emits an `evicted` keyspace notification, and increments `evicted_keys`.
  Eviction runs on the master only, never during AOF replay; the loop keeps the cached
  `used_memory` live so it bounds memory rather than over-evicting. Config via
  `maxmemory-policy`/`maxmemory-samples` (config file, `CONFIG SET`/`GET`, CLI), reported
  in `INFO memory`.
- Missing: exact (non-sampled) LRU/LFU, LFU decay-over-time (`lfu-decay-time`), and
  per-read touch for every read command (currently the command's first key only — a
  documented approximation). Compact listpack/intset encodings and a jemalloc/activedefrag
  allocator (which would shrink RSS further after churn) remain deferred — see item 13.
- Tracking: keystone "Memory + eviction" track; builds on the `Entry` tagged-union (item 9).

13. Memory model — compact encodings & allocator (deferred)
- Status: `partial` (tagged-union Entry done; compact backends + allocator pending)
- Owner: memory-eviction
- Severity: medium
- Target milestone: P5
- Implemented: the `Entry` tagged union (only the active payload allocates; `sizeof(Entry)`
  ~328 B → ~56 B, accounted bytes/key ~3.9×, RSS ~32% on the small-key workload — item 9).
- Missing: real listpack/intset/ziplist backends (storage is hashtable-grade today;
  `OBJECT ENCODING` reports Redis-style names cosmetically), an open-addressing /
  incremental-rehash keyspace map (avoids the stop-the-world rehash on multi-million-key
  maps), a skiplist zset with O(log N) rank, and a jemalloc + `activedefrag` allocator.
  These are L/XL and architecturally invasive; deferred to protect the green baseline.
- Tracking: see the "Deferred" section of `CHANGELOG.md`.

14. Scripting + functions semantics beyond the green baseline
- Status: `partial` (unit/scripting passes natively at 301 ok / 0 err with 12 tracked skips;
  unit/functions passes natively at 109 ok / 0 err with 1 tracked skip)
- Owner: scripting-compat
- Severity: medium
- Target milestone: P1 suite burn-down
- Implemented: bundled Lua 5.1.5 on every platform (float tostring, table hash order,
  cjson/cmsgpack byte-parity with Redis — the interpreter Redis embeds);
  `redis.replicate_commands()` (no-op true), `redis.set_repl()` + `REPL_*` constants
  honored by both the replica and AOF propagation paths; `SCRIPT DEBUG yes|sync|no`
  accepted (LDB itself not implemented); HELLO/AUTH/RESET/QUIT allowed while a script
  is BUSY. Full Lua function libraries: `FUNCTION LOAD [REPLACE]/DELETE/FLUSH/LIST
  [LIBRARYNAME][WITHCODE]/STATS/DUMP/RESTORE/KILL/HELP`, `FCALL`/`FCALL_RO`, the shebang
  metadata parser, `redis.register_function` (positional + named args) with the
  no-writes/allow-oom/allow-stale/no-cluster/allow-cross-slot-keys flag matrix and C-level
  global-protection on the load environment; libraries persist through RDB (opcode 245 /
  FUNCTION2) and replicate to replicas (LOAD/DELETE/FLUSH/RESTORE propagated; function
  effects replicated); FCALL of a no-writes function runs on a read-only replica while a
  write function is rejected; OOM/allow-oom gating; `COMMAND GETKEYS` on FCALL/FCALL_RO.
- Missing: each of these is a skipfile line in `scripts/redis/skipfiles/unit_scripting.skip`
  or `unit_functions.skip` — the LDB debugger session protocol; SCRIPT KILL's UNKILLABLE
  distinction for scripts that already wrote; timed-out pure-Lua scripts staying BUSY
  instead of aborting; CLIENT LIST from inside a running script observing concurrent
  sessions; replica handshakes completing while a script blocks the loop; MASTERDOWN
  gating for EVAL and the no-writes/allow-stale shebang flags on replicas (the same gap
  that skips `FUNCTION - allow stale` — per-inner-command stale-flag enforcement inside a
  running function); a macOS `leaks` report on the Lua path (Linux CI unaffected; needs a
  leak hunt).
- Note: a trivially-infinite script/function (`while true do`) is advertised BUSY
  cooperatively (both EVAL and FCALL) rather than actually looping, since the
  single-threaded event loop cannot otherwise be interrupted — this satisfies the
  SCRIPT/FUNCTION KILL tests without a preemption thread.
- Tracking: burn down the skipfile; each removed line is a ratchet win.

15. Blocked-client serving order + expiry/SELECT propagation edges
- Status: known gaps (surfaced by the native list/zset/scripting suites)
- Owner: replication-compat
- Severity: medium
- Target milestone: P1/P4
- Implemented: BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP block and
  wake with correct data, deterministic effect propagation (LPOP/RPOP/LMOVE/ZPOPMIN/
  ZPOPMAX with counts), millisecond-precise timeouts, FIFO-ordered wakeups, and
  CLIENT UNBLOCK [TIMEOUT|ERROR].
- Missing: (each skipfile-tracked) a "ready keys" serving queue so an already-blocked
  client is served before a later same-batch pop takes the value (`Unblock fairness
  ...`, `... blocked twice`); nested-unblock propagation ordering; DEL propagation when a
  blocked key with an elapsed TTL is moved into place via SWAPDB / created inside MULTI
  (`SWAPDB wants to wake ...`, `MULTI + LPUSH + EXPIRE ...`); blocked-BLPOP-pop and
  in-script SELECT effects replication (`Replication of script multiple pushes to list
  with BLPOP`, `Lua scripts using SELECT are replicated correctly`); deterministic
  lazy-expire-DEL propagation on RANDOMKEY (`RANDOMKEY: Lazy-expire should not be wrapped
  in MULTI/EXEC` — RNG-dependent DEL count today).
- Tracking: implement the ready-keys queue + expiry propagation in the Phase-1
  scripting/replication pass, then drop the skips.

16. Compact encodings for list/hash/set/zset (OBJECT ENCODING label precision)
- Status: `partial` (cosmetic labels over hashtable/deque storage — see item 13)
- Owner: memory-eviction
- Severity: low
- Target milestone: P6
- Implemented: `OBJECT ENCODING` synthesizes listpack/intset/quicklist/skiplist/hashtable
  from element counts, value sizes, and the `*-max-listpack-*` / `set-max-intset-entries`
  / `list-max-listpack-size` thresholds (incl. negative list size-classes and 0→1
  clamping), with sticky one-way conversion for hash/set/zset and bidirectional for lists.
- Missing: byte-exact listpack layout, so a handful of encoding-transition/insertion-order
  tests are skipfile-tracked (`Hash ziplist of various encodings`, `SRANDMEMBER with a
  dict containing long chain`, `List listpack -> quicklist encoding conversion`, `List
  encoding conversion when RDB loading`). The same missing macro-node model means stream
  approximate trimming (`XADD`/`XTRIM` with `MAXLEN ~` / `MINID ~` / `LIMIT`, governed by
  `stream-node-max-entries`) removes whole 100-entry nodes in Redis but trims exactly in
  PeaDB — a spec-valid "keep at least N" result with a different retained count, so 11
  `unit/type/stream` tests are skipfile-tracked (`scripts/redis/skipfiles/unit_type_stream.skip`).
  Real backends land in Phase 6 (item 13).
- Tracking: folded into the compact-encodings track (item 13).

17. Stream consumer-group introspection depth (seen/active-time persistence, blocking-key counters)
- Status: `partial` (consumer groups are functionally complete; two introspection details lag)
- Owner: streams
- Severity: low
- Target milestone: P4
- Implemented: full consumer-group model — per-consumer objects with seen-time/active-time,
  `XINFO STREAM … FULL`, `XINFO GROUPS`, `XINFO CONSUMERS`, `XGROUP CREATE/SETID/DESTROY/
  CREATECONSUMER/DELCONSUMER`, `entries-read`/`lag` with Redis's nil-when-indeterminable
  semantics, and AOF round-trip of groups + consumers + PELs (`XGROUP CREATE … ENTRIESREAD`
  + `CREATECONSUMER` + `XCLAIM`) plus XREADGROUP effect propagation (CREATECONSUMER / XCLAIM
  / XGROUP SETID) to AOF and the replication backlog.
- Missing: (each skipfile-tracked in `scripts/redis/skipfiles/unit_type_stream-cgroups.skip`)
  per-consumer seen-time/active-time do not survive a DUMP/RESTORE or RDB round-trip (the
  PeaDB stream RDB encoding, item 5, omits them), and INFO Clients does not expose
  `total_blocking_keys` / `total_blocking_keys_on_nokey`.
- Tracking: seen/active-time persistence rides with the stream RDB byte-parity work (item 5);
  blocking-key counters land with the replication/introspection hardening in Phase 4.

18. Dirty-counter granularity (rdb_changes_since_last_save per command, not per change)
- Status: `partial` (counter exists and drives save points; granularity is coarser than Redis)
- Owner: persistence-compat
- Severity: low
- Target milestone: P4
- Implemented: `rdb_changes_since_last_save` (INFO Persistence) is the mutation epoch, bumped
  once per successful write command; save points and `BGSAVE`-since-last-change logic use it.
- Missing: Redis increments `server.dirty` by the number of *actual* changes, so a no-op write
  (e.g. `SETBIT`/`BITFIELD SET` writing the same value, `SADD` of an existing member) adds 0.
  PeaDB's per-command counter over-counts such no-ops — one `unit/bitops` test is skipfile-tracked
  (`scripts/redis/skipfiles/unit_bitops.skip`).
- Tracking: needs per-command dirty deltas (handlers report how much they changed); folded into
  the persistence-hardening work in Phase 4.

19. MIGRATE advanced options (multi-key KEYS, AUTH, target-connection caching)
- Status: `partial` (single-key MIGRATE works; advanced forms are deferred)
- Owner: replication-compat
- Severity: low
- Target milestone: P4
- Implemented: single-key `MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH pass] [AUTH2
  user pass]` — DUMPs the key, opens a socket to the target, authenticates (AUTH/AUTH2) when
  requested, SELECT + RESTORE, and (unless COPY) deletes the source; TTL, large values, hashes,
  and timeouts are handled. AUTH/AUTH2 credentials are redacted in the slow log / MONITOR.
- Missing: the multi-key `KEYS k1 k2 …` form and Redis's target-connection caching/idle-release.
  The `unit/dump` multi-key + connection-caching MIGRATE tests remain skipfile-tracked
  (`scripts/redis/skipfiles/unit_dump.skip`).
- Tracking: cluster/migration hardening in Phase 4.

20. Unsorted GEORADIUS/GEOSEARCH result ordering (geohash-score order, not cell-scan order)
- Status: `partial` (result set + all sorted/ANY/STORE forms are exact; only the unsorted tie order differs)
- Owner: geo-compat
- Severity: low
- Target milestone: P6
- Implemented: GEO search brute-forces the zset and filters every candidate by true haversine
  distance, so the returned *set* is byte-identical to Redis for `GEORADIUS`, `GEORADIUSBYMEMBER`,
  `GEOSEARCH`, and `GEOSEARCHSTORE` across `BYRADIUS`/`BYBOX`, `ASC`/`DESC`, `COUNT`/`ANY`,
  `WITHCOORD`/`WITHDIST`/`WITHHASH`, and `STORE`/`STOREDIST`. Sorted queries match Redis exactly.
- Missing: when no `ASC`/`DESC` is given, PeaDB emits results in global geohash-score order, while
  Redis emits them in geohash-neighbor cell-scan order (center box, then N/S/E/W, then diagonals;
  each box in score order). Redis documents unsorted GEORADIUS order as unspecified; one
  `unit/geo` test pins the cell-scan order and is skipfile-tracked
  (`scripts/redis/skipfiles/unit_geo.skip`).
- Tracking: exact cell-scan ordering rides with the geohash-neighbor scan optimization in the
  Phase 6 performance work (the same code that replaces the brute-force scan).

21. Server introspection/ops verbs used by unit/other (DEBUG HTSTATS, proc-title-template)
- Status: `partial` (the suite is adopted; a few ops-only introspection verbs are stubbed/absent)
- Owner: ops-compat
- Severity: low
- Target milestone: P4
- Implemented: `unit/other` is adopted green — `OBJECT`/`MEMORY`/`PUBSUB`/`SLOWLOG`/`CLIENT`/
  `COMMAND`/`CONFIG`/`FUNCTION`/`MODULE` all answer `HELP`; `RESET` clears pub/sub, MONITOR, and
  MULTI state and runs immediately inside a transaction; `CONFIG <unknown>` returns the Redis
  `unknown subcommand …` error; `INFO rdb_changes_since_last_save` reports the dirty delta.
- Missing: `DEBUG HTSTATS` and Redis's incremental-dict rehash suppression during a save child
  (PeaDB uses `std::unordered_map`, which has no table-size introspection or rehash knob); and
  `proc-title-template` / `setproctitle` (process-title rewriting). All skipfile-tracked in
  `scripts/redis/skipfiles/unit_other.skip`; the two RESET tests needing CLIENT TRACKING and
  multi-user ACL are tracked under item 7; the FLUSHALL dirty-counter reset under item 18.
- Tracking: folds into the introspection/ops hardening in Phase 4 (DEBUG verbs, process title).
