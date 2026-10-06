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

Parity is measured by two monotonically-improving numbers. They are not CI-gated yet: `parse_tcl_results.py` and `check_suite_ratchet.py` are run by hand, and neither `scripts/run_all_tests.sh` nor any CI workflow runs them.

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

Limits of this signal (found by the 2026-10 review): none of the 26 upstream `integration/*` suites (replication, psync2, aof, rdb, shutdown, logging, corrupt-dump and others) is adopted, which is why the persistence and replication defects in items 27–47 pass every gate; and some upstream passes come from code shaped to match the test rather than real behavior (items 42 and 54), so the pass rate overstates parity there.

## Known Deltas

1. ACL completeness
- Status: intentional temporary delta for early milestones M1-M3.
- Owner: security-compat
- Severity: high
- Target milestone: P4
- Implemented: basic AUTH/default user flow; a truthful ACL introspection surface
  (`WHOAMI`/`USERS`/`LIST`/`CAT`/`GETUSER`/`GENPASS`/`HELP`) against the single `default`
  user — see item 7 for the full description.
- Missing: full ACL user/rules engine. `SETUSER`/`DELUSER`/`LOAD`/`SAVE`/`LOG`/`DRYRUN` refuse
  with an error naming this gap rather than applying, discarding, or faking a rule (item 7).
- Tracking task: `M8` and later security-hardening milestones.

2. Redis RDB binary compatibility
- Status: temporary delta.
- Owner: persistence-compat
- Severity: high
- Target milestone: P3
- Implemented:
  - Native in-process RDB reader/writer for common Redis data types (see `rdb_load()` / `rdb_save()` usage at startup and in `SAVE`/`BGSAVE`). Its files do not load in real Redis and real Redis files do not load in PeaDB today, because the CRC64 checksum is wrong (item 27); lists of 65,536 or more items are cut (item 29).
  - RDB import/export via Redis bridge scripts (`scripts/redis/import_rdb_via_redis.py`, `scripts/redis/export_rdb_via_redis.py`). They have no password option and do not check errors (item 52).
  - Legacy PeaDB snapshot format as a fallback for local persistence.
  - Save points: `save <sec> <changes>` (config/`CONFIG SET`, repeatable; `save ""` disables)
    trigger a background BGSAVE via the event-loop cron; `SHUTDOWN [SAVE|NOSAVE]` saves on a
    clean shutdown. `stop-writes-on-bgsave-error` (default on) rejects writes with `-MISCONF`
    after a failed background save when save points are configured.
- Missing: full Redis RDB parity (broader opcode/encoding coverage, edge-case behavior, and rarely-used features). Default ships no save points (set them or enable AOF for durability). Open defects from the 2026-10 review: items 27, 28, 29, 34, 51 and 53; item 49 lists every broken migration path.
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
- Missing: full Redis-equivalent behavior/edge-case parity across entire command space. Open defects from the 2026-10 review: a tolerated torn tail is never cut off, so the next restart fails (item 30); the RDB and the AOF are both loaded on restart (item 28); the write-error flag is never cleared and `appendfsync always` acknowledges failed fsyncs (item 35); no MULTI/EXEC framing, and eviction DELs without SELECT (item 36); the write cost on the event loop (item 58); no automatic rewrite (item 59).
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
  the AOF/bridge path). Ties to the broader Redis-RDB-v11 parity track (item 2). PEL delivery time/count can differ after AOF replay and on replicas: XCLAIM and XAUTOCLAIM skip their `IDLE`/`TIME`/`RETRYCOUNT` options on every call and are propagated as sent (item 48); the RDB path preserves them exactly. DUMP of a stream returns nil (item 51).
- Tracking: P2-1 done (PeaDB-internal); byte-compat folded into RDB parity hardening.

6. Redis Cluster protocol surface
- Status: `experimental` — **not production-safe.** The 2026-10 review found that cluster mode can write keys on the wrong node, apply part of a transaction or script, drop replicated writes, and accept writes for slots other nodes own after a restart (items 43, 64, 65, 66, 67). What exists: real per-node ownership, addressing, and Redis 7.2 reply shapes over an operator-configured topology; still no gossip, no failure detection, no automatic failover.
- Owner: cluster-compat
- Severity: critical
- Target milestone: P5
- Implemented: a stable node table (`g_cluster_nodes`) keyed by a 40-hex id derived deterministically from each node's announced address (`sha1_hex("peadb-cluster-node:" + host + ":" + port)`), stable across restarts without a `nodes.conf` file; real slot ownership (`g_slot_routes`/`g_slot_owner`) drives `ADDSLOTS`/`DELSLOTS`/`ADDSLOTSRANGE`/`DELSLOTSRANGE`/`SETSLOT NODE`, and `MOVED` redirects name the real owning node's announced `host:port`, not a hardcoded address. `SETSLOT MIGRATING`/`IMPORTING` do **not** write `g_slot_owner`/`g_slot_routes` — a slot in flight stays reported as owned by whoever owned it before the migration started, exactly like Redis keeping the slot in the owner's bitmap throughout migration; the redirect target instead lives in two side-channel arrays, `g_slot_migrating_to`/`g_slot_importing_from` (mirroring Redis's own `server.cluster->migrating_slots_to`/`importing_slots_from`), consulted only by the dispatch chokepoint's `ASK` check and by `CLUSTER NODES`' `[slot->-<id>]`/`[slot-<-<id>]` markers on this node's own line (`clusterGenNodeDescription`, `cluster.c:5329-5344`) — never by `cluster_owned_ranges()`, so a migration in progress cannot leak into `CLUSTER SLOTS`/`SHARDS`/`NODES`'s reported ownership or into `cluster_size`. A definitive `SETSLOT NODE` clears both side-channel arrays for that slot, so a concluded migration cannot leave a stale marker naming a migration that already finished. (An earlier version of this feature conflated the two facts — `SETSLOT MIGRATING` wrote the same `g_slot_owner` array `cluster_owned_ranges()` reads, so a client refreshing its slot map would see the slot move to the migration target before any key had actually moved, and get misses instead of `ASK`; caught in final review and fixed in the same milestone — see `test_p5_cluster_ownership.py`'s migration assertions.) `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` walk the real keyspace via `DataStore::for_each_key` (no cached slot-to-keys index, so results are exactly correct, and `for_each_key` hands out each live key by reference instead of building a `std::vector` copy of the keyspace first — but see Missing below for the O(N) walk this still carries and an ordering divergence). `cluster-enabled` is a real startup-only (Redis's own `IMMUTABLE_CONFIG` treatment) config field, refused by `CONFIG SET`, and gates the entire `CLUSTER` command before any subcommand dispatch — including read-only ones like `KEYSLOT` and `HELP` — exactly where Redis's `clusterCommand()` does it; a node refuses to start with `cluster-enabled yes` if its resolved announced address is a wildcard (`0.0.0.0`, `::`, `*`, or empty), whether that resolution comes from the `bind` fallback or an explicit `cluster-announce-ip` naming a wildcard itself; `CONFIG SET cluster-announce-ip` carries the same refusal at runtime (the two port parameters beside it were already range-checked). `CLUSTER SLOTS` groups contiguous slots by their real owner and omits unassigned slots entirely, matching `clusterReplyMultiBulkSlots`; each node entry is Redis 7.2's 4-element shape (ip, port, 40-hex id, and an additional-endpoints map that is always empty because PeaDB has no `cluster-preferred-endpoint-type` config to make it anything other than the default "ip"), and that map is RESP2/RESP3-correct (`*0` vs `%0`). `CLUSTER SHARDS` returns one shard per real owner: a 2-field map (`slots`, a flat start/end array; `nodes`, an array of 7-field maps — `id`, `port`, `ip`, `endpoint`, `role`, `replication-offset`, `health`); every node reports `role:master`/`health:online` because PeaDB models no replicas inside the cluster (a deployment wanting both sharding and replica failover runs `REPLICAOF` on top of the cluster model, separately), and `replication-offset` is the same counter `INFO replication`'s `master_repl_offset` reports. `CLUSTER NODES` joins lines with `\n` (not `\r\n`, matching `clusterGenNodesDescription`), reports every node's own announced address — including self, which used to hardcode `127.0.0.1` regardless of `cluster-announce-ip` — and each node's real owned slot ranges. `CLUSTER INFO` derives `cluster_slots_assigned`/`cluster_slots_ok`/`cluster_state` from real slot coverage (`ok` only once all 16384 slots have an owner, matching Redis's `cluster-require-full-coverage yes` default — PeaDB does not expose that config, so full coverage is always required) and `cluster_size` from the count of distinct owners that hold at least one slot, not the whole known-node count. `CLUSTER MEET`'s port-parse error now uses Redis's exact wording, `"Invalid base port specified: <arg>"` (`cluster.c:5958`), instead of a made-up message; `MEET` also refuses a target that resolves to this node's own announced address, and the peer-reachability round trip (the synchronous `MEETBACK` callback, `try_send_cluster_meetback`) is now bounded by a 1-second connect/read timeout instead of the OS default — see Missing below for what this does and does not fix. `CONFIG SET cluster-announce-ip`/`-port`/`-bus-port` re-derive `g_cluster_nodes[0]` (id included, since the id is derived from the address) immediately, so `CONFIG GET`, `CLUSTER NODES`, and `CLUSTER SLOTS` can never disagree about this node's own advertised address regardless of whether the `CONFIG SET` lands before or after the first `CLUSTER` call has lazily snapshotted the node table. Full design rationale: `docs/decisions/p5-cluster-addressing-and-version-provenance.md`.
- Open defects (2026-10 review): routing looks only at the first key and there is no CROSSSLOT (item 64); no CLUSTERDOWN, no saved topology and no health reporting (item 65); standard tooling, resharding, SETSLOT checks and MEET (item 66); multi-database commands, ignored `cluster-*` settings and the bus listener (item 67); a cluster-enabled replica drops replicated writes (item 43); `COUNTKEYSINSLOT`/`GETKEYSINSLOT` erase expired keys without DEL (item 37) and cost about 90–100 ms per call at 1M keys, against about 0.1 ms in Redis (measured on macOS).
- Missing: no cluster bus gossip protocol at all — the bus port opened at `announced_port + 10000` is a placeholder listener that answers a bare `PONG`, not a MEET/PING/PONG/FAIL message exchange, so topology never propagates between nodes on its own; no failure detection (no `PFAIL`/`FAIL` state machine, because there is no gossip channel for nodes to disagree about liveness over); no automatic failover (there are no replicas in the cluster model at all, so there is nothing to promote); every node's config-epoch reads as a hardcoded `1` (`cluster_current_epoch`/`cluster_my_epoch` in `CLUSTER INFO`, and the config-epoch column of every node's `CLUSTER NODES` line) — Redis assigns and bumps a real, growing epoch per node so a node or client can tell whose slot claim is newer after a partition or a manual race; PeaDB has no gossip protocol for two nodes to ever disagree about slot ownership in the first place, so there is no conflict left for an epoch to resolve, and a constant says that plainly — inventing varying epoch numbers with no real conflict behind them would claim a resolution process that does not exist, which is worse than the constant; no `nodes.conf` persistence (topology is entered per node via `CLUSTER MEET`/`SETSLOT`/`ADDSLOTS(RANGE)` and is lost on restart — an operator or their orchestration tooling must re-issue it, or script it into node startup, every time a node restarts — and the natural shape of that script, a `MEET` loop from every node to every known address including its own, used to hang the process outright on the self-address case and could block it for minutes on a genuinely unreachable one, since `try_send_cluster_meetback` ran an unbounded, synchronous `connect()`/`read()` on the single event-loop thread; `MEET` now refuses a self-address target and bounds the round trip to any other target with a 1-second connect/read timeout, but the round trip is still synchronous, so a bad address still stalls every other client on this node for up to that timeout — a full fix makes the handshake asynchronous, unchanged XL-scope future work); `CLUSTER MEET`'s peer-reachability round trip (the synchronous `MEETBACK` callback) accepts numeric IPv4 only, via `inet_pton(AF_INET, ...)` — no hostname resolution and no IPv6; `CLUSTER SETSLOT MIGRATING`/`IMPORTING` do not enforce Redis's ownership preconditions (`"I'm not the owner of hash slot %u"` when `MIGRATING` a slot this node does not own, `"I'm already the owner of hash slot %u"` when `IMPORTING` a slot it already owns — `cluster.c:6091-6110`), there is no `SETSLOT STABLE` to clear a migrating/importing state other than a definitive `SETSLOT NODE`, and PeaDB does not yet special-case `ASKING` traffic for an importing slot (a slot this node is importing is simply not locally owned until the final `SETSLOT NODE`, so it still `MOVED`s rather than accepting `ASKING`-flagged commands the way Redis does). `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` are accurate but not O(1), and this is a cost claim, not just a completeness one: Redis's `countKeysInSlot` (`cluster.c`) is O(1) — a single `by_slot[hashslot].count` read — while PeaDB has no slot-to-keys index and instead walks the entire keyspace on every call via `DataStore::for_each_key`, which visits each live key by reference (no `std::vector<std::string>` copy of the keyspace, no per-key string allocation); `COUNTKEYSINSLOT` only increments a counter per key it visits, and `GETKEYSINSLOT` collects and sorts only the keys that actually match the target slot, not the whole keyspace as it used to (a whole-keyspace `store().keys("*")` copy followed by a whole-keyspace `std::sort` before either subcommand had even looked at a single key's slot). Both are still O(N) in the database's key count — an admin tool that loops `COUNTKEYSINSLOT` over all 16384 slots (a plausible rebalancing-audit pattern) still triggers 16384 full keyspace walks, not 16384 O(1) reads, and on PeaDB's single-threaded event loop each walk still stalls every other client for its duration — but the constant factor per walk is now a single allocation-free pass instead of "copy every key into a vector, then (for `GETKEYSINSLOT`) sort all of them regardless of how few land in the requested slot." `GETKEYSINSLOT` also returns keys in lexicographic order (an artifact of the sort used to make results deterministic across calls), not Redis's slot-index (bucket-scan) order — a caller that relies on result order for anything beyond "some keys in this slot" will see a different first key than against real Redis. Redis 7.2.5 has 27 canonical `CLUSTER` subcommands (28 `cluster-*.json` command specs in the pinned source, minus `SLAVES`, which is a deprecated-since-5.0.0 alias for `REPLICAS`, not a distinct subcommand); PeaDB implements 15 of them, and 12 remain unimplemented: `BUMPEPOCH`, `COUNT-FAILURE-REPORTS`, `FAILOVER`, `FLUSHSLOTS`, `FORGET`, `LINKS`, `MYSHARDID`, `REPLICAS`, `REPLICATE`, `RESET`, `SAVECONFIG`, `SET-CONFIG-EPOCH`. `MEETBACK` is a PeaDB-only extension, not a real Redis subcommand at all — it lets a `MEET`ed peer learn of the meeting node immediately, synchronously, since there is no gossip round for that news to travel on otherwise — so it is not counted among the 15.
- Tracking: long-term cluster milestone (gossip protocol, failure detection, automatic failover, and `nodes.conf` persistence are all separate, XL-scope future work). The operator-configured topology model this item now closes against is a deliberate, documented design choice — see the decision record above — not a stopgap awaiting a rewrite; large regulated customers already run an external orchestrator that owns membership and placement, so a smaller, auditable, explicitly-administered topology is the intended shape here, not a shortcut. Cluster mode stays `experimental` until items 43 and 64–67 are closed or the scope decision changes.

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
  before buffering); the Lua sandbox now removes `os`/`io`. `ACL WHOAMI`/`USERS`/`LIST`/`CAT`/
  `GETUSER`/`GENPASS`/`HELP` truthfully introspect the single `default` user: `ACL LIST` and
  `ACL GETUSER` print its password the way Redis does, `#<sha256-hex>` of `requirepass` or
  `nopass` when none is set (`ACLDescribeUser`/`ACLUpdateDefaultUserPassword`); `ACL CAT
  <category>` filters the real generated per-command category metadata rather than a
  hand-written list; `ACL GENPASS` draws from the same entropy source as the replication run
  id. `ACL SETUSER`/`DELUSER`/`LOAD`/`SAVE`/`LOG`/`DRYRUN` refuse with an error naming this gap
  instead of the previous behavior — `SETUSER` used to answer `+OK` and silently discard its
  rule, and the others were unregistered — because there is no rule engine to create, delete,
  persist, or execute a rule against, and no log of auth events to show or reset.
- Missing: full multi-user ACL rule evaluation (users, key patterns, command categories,
  pub/sub channels), the rule-authoring subcommands themselves (`SETUSER`/`DELUSER`/`LOAD`/
  `SAVE`/`DRYRUN`), `ACL LOG`, and TLS — all deferred as dedicated XL tracks. One upstream
  `unit/slowlog` test needs a working `SETUSER` to redact its arguments and is skipfile-tracked
  (`scripts/redis/skipfiles/unit_slowlog.skip`).
- Open defects (2026-10 review): audit counters and logging (item 60); weaker defaults than Redis for DEBUG, CLIENT SETINFO and the password compare (item 61). Since the item 24 fix, a config with `user` or `aclfile` lines, or one that turns TLS on, stops startup, so a redis.conf that sets its password that way no longer starts PeaDB with no password.
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
  `repl-backlog-size` (default 1 MiB). Master supports +CONTINUE partial resync (but PeaDB replicas never ask for it, and for a real Redis replica the data may start one byte late — item 47); replica
  emits REPLCONF ACK every ~1 s (for data received, not applied — item 44). **Randomized replid** per process (`init_replication_id`)
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
  external clients is still open and blocks tools that use SYNC, see item 49); per-replica `slaveN:` INFO lines; diskless
  fork-based RDB streaming for full resync (today the snapshot serializes on the event-loop
  thread); automatic failover/Sentinel (deferred XL track). (Stream replication bootstrap
  works — PSYNC resyncs stream datasets via the stream RDB encoding, item 5.)
- Open defects (2026-10 review): replicas silently differ from the master in several ways — writes over the backlog size in one loop pass are lost (item 40), a failed SET NX with a TTL and a RESTORE with a TTL replicate wrongly (items 41 and 42), client-side checks drop replicated writes (item 43), replicas ACK before they apply (item 44) and do not persist replicated data (item 45), full sync freezes the master and fails above about 2.3M keys (item 46), and there is no replica-side partial resync or link timeout (item 47). PeaDB as a replica of a real Redis master: item 50.
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
- Open defect (2026-10 review, seen in code): pub/sub messages are sent as RESP2 `*3` arrays even to clients that chose RESP3 (`encode_pubsub_message` has no `>` push form), so strict RESP3 client parsers can lose their place in the reply stream (old plan Task 1.10).
- Tracking: incremental protocol hardening.

12. Key eviction (maxmemory-policy)
- Status: `partial` — the eviction policies themselves work (approximate, like Redis — sampling-based, not exact LRU/LFU), but the memory estimate they act on is slow and too low, so `maxmemory` does not protect the host today, and the OOM check also refuses writes that free memory (items 38 and 39, 2026-10 review).
- Owner: memory-eviction
- Severity: critical (through items 38 and 39)
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
- Tracking: keystone "Memory + eviction" track; builds on the `Entry` tagged-union (item 13).

13. Memory model — compact encodings & allocator (deferred)
- Status: `partial` (tagged-union Entry done; compact backends + allocator pending)
- Owner: memory-eviction
- Severity: medium
- Target milestone: P5
- Implemented: the `Entry` tagged union (only the active payload allocates; `sizeof(Entry)`
  ~328 B → ~56 B, accounted bytes/key ~3.9×, RSS ~32% on the small-key workload — see `docs/architecture.md`).
- Missing: real listpack/intset/ziplist backends (storage is hashtable-grade today;
  `OBJECT ENCODING` reports Redis-style names cosmetically), an open-addressing /
  incremental-rehash keyspace map (avoids the stop-the-world rehash on multi-million-key
  maps), a skiplist zset with O(log N) rank, and a jemalloc + `activedefrag` allocator.
  These are L/XL and architecturally invasive; deferred to protect the green baseline.
- Measured cost (2026-10 review, macOS with libc++ and libc malloc; measure again on Linux): because the keyspace map rehashes all at once, PING stalled 229–238 ms while the keyspace grew to 4M keys, against 9.5–13.9 ms in Redis; RSS was about 3× Redis on a mixed dataset (829 MB against 278 MB at 1.4M keys; small lists 26×, small zsets 4.9×, much of it from libc++'s 4 KB `std::deque` blocks, which libstdc++ on Linux does not use).
- Tracking: see the "Deferred" section of `CHANGELOG.md`.

14. Scripting + functions semantics beyond the green baseline
- Status: `partial` (unit/scripting passes natively at 301 ok / 0 err with 12 tracked skips;
  unit/functions passes natively at 109 ok / 0 err with 1 tracked skip)
- Owner: scripting-compat
- Severity: medium
- Target milestone: P1 suite burn-down
- Implemented: bundled Lua 5.1.5 on every platform. It is stock 5.1.5, not the patched copy Redis embeds, so table order for long string keys differs (item 62), and `cjson`/`cmsgpack`/`bit`/`struct` are PeaDB's own code whose output differs in places (item 63);
  `redis.replicate_commands()` (no-op true), `redis.set_repl()` + `REPL_*` constants
  honored by both the replica and AOF propagation paths; `SCRIPT DEBUG NO`, with
  `YES` and `SYNC` refused because there is no debugger (item 26); HELLO/AUTH/RESET/QUIT allowed while a script
  is BUSY. Full Lua function libraries: `FUNCTION LOAD [REPLACE]/DELETE/FLUSH/LIST
  [LIBRARYNAME][WITHCODE]/STATS/DUMP/RESTORE/KILL/HELP`, `FCALL`/`FCALL_RO`, the shebang
  metadata parser, `redis.register_function` (positional + named args) with the
  no-writes/allow-oom/allow-stale/no-cluster/allow-cross-slot-keys flag matrix and C-level
  global-protection on the load environment; read-only scripts (`no-writes`, `EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`) refuse every command Redis flags write or may-replicate (since 2026-10-06; before, only a list of 37 commands, so `HINCRBY`, `LMOVE`, `ZINCRBY`, `PUBLISH` and others ran, and `EVALSHA_RO` was not read-only at all); the `EVAL`/`EVALSHA`/`SCRIPT LOAD` shebang is parsed like Redis's `evalExtractShebangFlags()` (all five flags, Redis's error texts, the shebang line's newline kept so Lua's line numbers match), `SCRIPT LOAD` accepts a script with a shebang (it used to fail to compile one) and `EVALSHA` honors the cached script's flags, and `EVAL_RO` of a script that declares it may write is refused; libraries persist through RDB (opcode 245 /
  FUNCTION2) and replicate to replicas (LOAD/DELETE/FLUSH/RESTORE propagated; function
  effects replicated); FCALL of a no-writes function runs on a read-only replica while a
  write function is rejected; OOM/allow-oom gating; `COMMAND GETKEYS` on FCALL/FCALL_RO.
- Missing: each of these is a skipfile line in `scripts/redis/skipfiles/unit_scripting.skip`
  or `unit_functions.skip` — the LDB debugger session protocol (`SCRIPT DEBUG YES` and `SYNC` are refused, and `scripts/redis/suite-native.patch` wraps the one upstream `SCRIPT DEBUG SYNC` call that sits outside any test, so the refusal does not abort `unit/scripting`); SCRIPT KILL's UNKILLABLE
  distinction for scripts that already wrote; timed-out pure-Lua scripts staying BUSY
  instead of aborting; CLIENT LIST from inside a running script observing concurrent
  sessions; replica handshakes completing while a script blocks the loop; MASTERDOWN
  gating for EVAL and the no-writes/allow-stale shebang flags on replicas (the same gap
  that skips `FUNCTION - allow stale` — per-inner-command stale-flag enforcement inside a
  running function); a macOS `leaks` report on the Lua path (Linux CI unaffected; needs a
  leak hunt). Not a skipfile line yet (seen 2026-10-06 against the pinned Redis): errors raised in a script lack the ` script: <sha or function name>, on @user_script:<line>.` that Redis appends (the upstream `unit/pause` test "Test may-replicate commands are rejected in RO scripts" is skipped for it); `EVAL` compiles a script under a chunk name that is the script text, so a runtime error names a line of PeaDB's sandbox helper (`[string "..."]:97: Script attempted to access nonexistent global variable 'foo'`, Redis: `user_script:1: ...`); `redis.pcall` of an unknown command returns `ERR unknown command` (Redis: `Unknown Redis command called from script`); the `no-cluster` and `allow-stale` script flags are accepted but not enforced, and an `EVAL` script with a shebang under OOM gets `command not allowed when used memory > 'maxmemory'` where Redis says `allow-oom flag is not set on the script, can not run it when used memory > 'maxmemory'` (the rest of Redis's `scriptPrepareForRun()` checks: `READONLY` replica and `MISCONF` texts); `SCRIPT LOAD` does not compile the script, so a syntax error shows only when the script runs.
- Note: any script, or any function library, whose text contains `while true do` or `while 1 do` is not run at all; the server is set BUSY instead, which is how the SCRIPT/FUNCTION KILL tests pass. This is code shaped to match the tests, not real behavior: a normal loop written that way never runs, and a real long-running script cannot be stopped (item 54).
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
- Open defect (2026-10 review): the single-key form works only between PeaDB servers, because the checksum and the value encodings do not match across engines (items 27 and 51). Since 2026-10-06 PeaDB no longer sends ASKING to a target when it does not run in cluster mode (item 26).
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

22. INFO section and field coverage (Latencystats absent, standard fields missing)
- Status: `partial` (every section Redis's default/all INFO emits is present except Latencystats, but about 16 standard fields inside those sections are missing and some fields are fixed values — see "Missing fields" below)
- Owner: observability
- Severity: medium
- Target milestone: P5
- Implemented: `INFO cluster` now emits a real `# Cluster` section (`cluster_enabled`), matching
  Redis's `genRedisInfoString`, and is included in the default and `all`/`everything` output like
  real Redis. An unrecognised section name now returns a genuinely empty bulk string instead of
  falling back to a hardcoded `# Server` stub, matching Redis (which appends nothing for a section
  it does not recognise).
- Missing: `# Latencystats` (the `LATENCY HISTORY`/`LATENCY LATEST`-derived percentile section
  Redis emits under `all`/`everything` or when explicitly requested) has no `want("LATENCYSTATS")`
  block yet, so `INFO latencystats` returns empty and `INFO all` omits the section.
- Missing fields (2026-10 review, seen in code): `run_id`, `process_id`, `rejected_connections`, `total_net_input_bytes`, `total_net_output_bytes`, `instantaneous_input_kbps`, `aof_last_write_status`, `total_system_memory`, `used_memory_dataset`, `rdb_last_bgsave_time_sec`, `client_recent_max_output_buffer`, `master_last_io_seconds_ago`, `master_link_down_since_seconds`, `master_sync_in_progress`, `slave_repl_offset` (and `slave_read_repl_offset`; two upstream `unit/pause` tests are skipped for them) and `slave_priority`, plus the per-replica `slaveN:` lines (item 8) and the ACL counters (item 60). Fixed or misleading values: `repl_backlog_active:0`, `os:Linux` and `arch_bits:64` on every platform, an always-empty `config_file:`, and an `instantaneous_ops_per_sec` measured since the previous INFO call, so two scrapers distort each other; MEMORY STATS returns zeros and MEMORY DOCTOR always reports no problems. Exporter alerts built on these fields never fire, and Redis Sentinel, which needs `run_id`, `slaveN:`, `slave_priority` and `slave_repl_offset`, cannot manage PeaDB. Every INFO call also walks the keyspace (item 55).
- Tracking: add the Latencystats section alongside the existing per-command latency histogram
  (`g_cmd_latency_hist`) as part of the observability hardening track.

23. `CONFIG SET` accepts unrecognized parameter names
- Status: `full` (fixed 2026-10-06). Evidence: checked against the pinned Redis 7.2.5 by `tests/integration/test_config_set_get_parity.py`.
- Owner: config-compat
- Severity: medium (before the fix)
- Target milestone: R0
- Implemented: `CONFIG SET` knows every Redis 7.2.5 setting and alias (`include/redis_config_table.generated.hpp`, generated from the pinned `config.c` with each setting's flags and default) and answers like Redis's `configSetCommand()`: a name Redis does not have gets `ERR Unknown option or number of arguments for CONFIG SET - '<name>'`, an odd number of arguments gets `syntax error`, an `IMMUTABLE_CONFIG` setting `can't set immutable config`, a `PROTECTED_CONFIG` setting (`dir`, `dbfilename`) `can't set protected config` unless `enable-protected-configs` allows it, a repeated name `duplicate parameter`, and a bad value Redis's own check and text, which the config loader shares (`check_redis_config_value()` in `src/config.cpp`). Several settings in one command are all or nothing: nothing changes until every name and value has passed, and if applying one fails the earlier ones are put back. What `CONFIG SET` does with a known setting is item 25, and with a Redis setting PeaDB does not apply item 24.
- Missing: nothing for unknown names.
### Findings of the 2026-10 review (items 24–71)

A verified review of `cb7194a` (2026-09-30 to 2026-10-03) compared PeaDB with the pinned Redis 7.2.5 in seven areas: registry, security, cluster, infra, commands, scale and durability. The commands area did not finish, so per-command behavior outside the adopted upstream suites is not reviewed yet. Two independent verifiers re-checked the 54 most serious items: 44 held exactly, 10 were real but narrower (the narrower claim is used here), and none was disproved. Items 24–71 register what it found, and the older items above that it proved wrong are corrected in place. **Item 26 is fixed (2026-10-06), and items 24 and 25 are fixed in part (2026-10-05 and 2026-10-06); none of the other items 24–71 is fixed yet.** File:line references are as of `cb7194a`.

Evidence words used below: **reproduced** means run on live servers and re-checked by an independent verifier; **seen in code** means read in the code and re-checked, but not run; **code reading** means one reviewer's reading that nobody has re-checked yet. Speed and memory numbers were measured on macOS (libc++, libc malloc) and must be measured again on Linux.

Target milestone for items 24–71 names the planned fix order: R0 stop false claims, R1 single-server data loss, R2 replication, R3 migration and rollback, R4 tests, R5 scale, R6 security and audit, R7 cluster, R8 command and protocol compatibility.

24. Redis settings that PeaDB accepts but does not apply
- Status: `partial` (fixed in part 2026-10-05 and 2026-10-06: loading and `CONFIG SET` now follow Redis; some Redis settings are still accepted with a warning and have no effect). Evidence: reproduced before the fix; the fix is checked against the pinned Redis 7.2.5 by `tests/integration/test_config_redis_parity.py` and `tests/integration/test_config_set_get_parity.py`.
- Owner: config-compat
- Severity: medium
- Target milestone: R0
- Implemented: the config file and the command line are loaded like Redis 7.2.5's `loadServerConfig()` (`src/config.cpp`): Redis's line splitting (quotes and escapes; `#` starts a comment only at the start of a line), its 183 settings and their aliases (generated from the pinned `config.c` into `include/redis_config_table.generated.hpp` by `scripts/ci/generate_redis_config_table.py`), its value checks and error text, `include` with wildcards, `rename-command` with Redis's rules (`rename-command X ""` removes the command), and `--name value` options parsed as config lines after the file. A `dir` line changes the working directory at once, as Redis's `chdir(2)` does, so later relative paths (an `include`, `logfile`, `unixsocket`, the data files) are inside it. Where Redis's loader refuses to start, PeaDB refuses too, with the same reason and line: a config file that cannot be opened, an unknown name, the wrong number of arguments, unbalanced quotes, a bad value (including `activedefrag yes`, refused like a Redis built without defragmentation, as the pinned build is), a `logfile` that cannot be opened, an invalid `locale-collate`, and `rename-command` with no target, of a command that does not exist (checked against Redis's command table as earlier renames change it), or onto a name that is taken. NUL bytes are dropped the way Redis's `fgets()` loop drops them, `port 0` opens no TCP listener (with no listener at all PeaDB exits, as Redis does), and `save <sec> 0` saves every `<sec>` seconds. PeaDB also refuses the Redis settings it cannot honour safely: `user`, `aclfile`, `loadmodule`, module settings, `replicaof`/`slaveof` other than `no one` (item 45), `masteruser` (item 50), `tls-port` other than 0, `tls-replication yes` and `tls-cluster yes` (item 7), `enable-debug-command no` or `local` (item 61), `bind` with more than one address (item 69), and `--sentinel` (item 8). Like every setting these are judged on the final value, so a later line or option can undo an earlier one. Settings that `CONFIG SET` already changed but the config file used to ignore are applied now (`min-replicas-to-write`, `replica-serve-stale-data`, `busy-reply-threshold`, `lazyfree-lazy-server-del`, `stop-writes-on-bgsave-error`, `aof-load-truncated`, `slowlog-log-slower-than`, `slowlog-max-len`, `latency-monitor-threshold`), as are `unixsocketperm`, `--maxclients` and, since 2026-10-06, `proto-max-bulk-len`, `client-query-buffer-limit`, `tcp-keepalive`, `lfu-log-factor` and `enable-protected-configs`. `CONFIG SET` treats every Redis setting PeaDB does not apply the same way as the config file (item 25 covers the settings PeaDB applies): a refused one is refused with the same reason; one with a fixed value (below) refuses any other value, and `CONFIG GET` reports the fixed value; any other one keeps the value it is given, which `CONFIG GET` then reports, and logs a warning.
- Missing: every other Redis setting is accepted and has no effect. The log gets one warning for each such setting, naming its last line (never its value), and a value that is what PeaDB always does is accepted silently. These fixed values are `daemonize no`, `databases 16`, `logfile ""`, `pidfile ""`, `syslog-enabled no`, `supervised no`, `io-threads 1`, `activedefrag no`, `aof-use-rdb-preamble no`, `aof-timestamp-enabled no`, `auto-aof-rewrite-percentage 0` (no automatic rewrite, item 59), `rdbcompression no`, `rdbchecksum yes`, `lfu-decay-time 0` (LFU counters never decay), `maxmemory-clients 0`, `replica-read-only yes`, `replica-ignore-maxmemory yes`, `tls-port 0`, `tls-replication no`, `tls-cluster no`, `aclfile ""`, `masteruser ""`, `enable-debug-command yes` and `replicaof no one`; where one differs from Redis's default, `CONFIG GET` reports PeaDB's value. The ignored settings include `logfile`, `syslog-*`, `pidfile`, `daemonize yes`, `supervised`, `databases` other than 16, `min-replicas-max-lag`, `hz`, `appenddirname`, `aof-use-rdb-preamble yes`, `rdbcompression yes`, `shutdown-on-sigterm`/`-sigint`, `oom-score-adj*`, `maxmemory-clients` other than 0, `repl-timeout`, `repl-ping-replica-period`, `acl-pubsub-default`, the `tls-*` file and cipher settings, and 17 `cluster-*` settings (item 67). Redis's `isValidProcTitleTemplate` check is not run (PeaDB sets no process title). Renaming a Redis command that PeaDB does not implement (item 68) to a new name gives a warning, because the new name does not exist either. With no `save` line PeaDB keeps no save points, where Redis uses `3600 1 300 100 60 10000` (item 2).
- Tracking: apply or refuse each warned setting, most-used first (`logfile`, `pidfile`, `min-replicas-max-lag`, `daemonize yes`).

25. `CONFIG SET` and `CONFIG GET` answers for known parameters that are not true
- Status: `partial` (fixed in part 2026-10-06). Evidence: reproduced before the fix; the fix is checked against the pinned Redis 7.2.5 by `tests/integration/test_config_set_get_parity.py`.
- Owner: config-compat
- Severity: medium (high before the fix)
- Target milestone: R0
- Implemented: `CONFIG GET` takes several patterns, reports every Redis 7.2.5 setting and alias (195 names; a `HIDDEN_CONFIG` setting only when asked for by its exact name), shows a name the way the client spelled it, ignores case, and answers with a map under RESP3, as Redis does. Each value is the running one: `dir` is the working directory, which PeaDB now changes to the configured `dir` the way Redis does (so the data files are relative to it), `cluster-announce-port` and `-bus-port` print the configured value (`0` when unset), `notify-keyspace-events` prints its classes in Redis's order, and the defaults are Redis's (`hash-max-listpack-entries` was 128 and `list-max-listpack-size` 128, Redis's are 512 and -2). Every setting PeaDB applies takes effect at once when `CONFIG SET` changes it: `appendonly yes` starts the AOF with a rewrite of the data and keeps every client write made meanwhile, each with its own `SELECT` (Redis's `AOF_WAIT_REWRITE`; a failed rewrite is retried, after 3 failures in a row only after 1, 2, 4 … 60 minutes like Redis's `aofRewriteLimited()`, and `INFO` now has `aof_rewrite_scheduled` and `aof_rewrites_consecutive_failures`), `appendonly no` closes it and stops a running rewrite, `maxclients` is read by the accept loop on every connection and raises the open-files limit or fails with Redis's text, `proto-max-bulk-len` and `client-query-buffer-limit` are enforced (they were fixed at 512 MB and 1 GB), and, as in Redis, not on commands replayed from the AOF or applied from the master, `repl-backlog-size` resizes the backlog and keeps the newest bytes and the offsets, `tcp-keepalive` sets the keepalive time of new connections (`0` turns it off), `lfu-log-factor` sets the LFU counter growth, `dir` and `dbfilename` work when `enable-protected-configs` is `yes` (or `local`, for a loopback or unix-socket client), and the aliases (`slave-serve-stale-data`, `min-slaves-to-write`, `lua-time-limit`, the `*-ziplist-*` names) change the same setting as their main name. A value that cannot be applied (`maxclients` above what the open-files limit allows, a `dir` that cannot be entered, a wildcard `cluster-announce-ip`) is found before anything changes. As in Redis, scripts may not call `CONFIG` except `CONFIG HELP`. `CONFIG HELP` is Redis's text, a subcommand with the wrong number of arguments gets `wrong number of arguments for 'config|<sub>' command`, an unknown one `unknown subcommand '<sub>'. Try CONFIG HELP.`, and `CONFIG REWRITE` without a config file gets Redis's `The server is running without a config file`.
- Missing: `CONFIG SET port` and `CONFIG SET bind` are refused unless the value stays the same, because PeaDB cannot move its listener while it runs (Redis can). `CONFIG REWRITE` with a config file is refused (`CONFIG REWRITE is not supported by PeaDB yet`), so `CONFIG SET` changes do not survive a restart. The AOF that `appendonly yes` turns on is PeaDB's single file, not Redis's `appendonlydir` with a manifest (item 3). On a replica it does not hold the replicated writes (item 45). The writes made during a rewrite are kept in memory up to 256 MiB; past that the rewrite is dropped and, while `appendonly yes` waits for its first rewrite, tried again, so under a very high write rate the AOF may never come on although `INFO` shows `aof_enabled:1` (Redis 7 writes them to a new file instead; seen in code). `BGREWRITEAOF` while the AOF is off throws the rewrite away and reports `aof_last_bgrewrite_status:err`, where Redis writes a new AOF (seen in code). As in Redis, an AOF that is on keeps writing to its old file after `CONFIG SET dir` until the next rewrite, and a rewrite running during that `CONFIG SET dir` fails because its temporary file is in the old directory.
- Tracking: move the listener at runtime for `port`/`bind`; write `CONFIG REWRITE`.
26. Commands that reply `OK` but do nothing
- Status: `full` (fixed 2026-10-06). Evidence: reproduced before the fix; the fix is checked against the pinned Redis 7.2.5 by `tests/integration/test_noop_replies_parity.py` and `tests/integration/test_client_pause_parity.py`, and by the upstream `unit/pause` suite (adopted, 15 ok, 3 tracked skips).
- Owner: ops-compat
- Severity: high (before the fix)
- Target milestone: R0
- Implemented: `CLIENT PAUSE <timeout> [WRITE|ALL]` and `CLIENT UNPAUSE` work like Redis's: Redis's argument checks and errors (the mode before the timeout, a strict integer timeout in milliseconds, `timeout is negative`, `timeout is out of range`); a later pause replaces the mode but keeps the later end time, and a write pause does not weaken a pause of everything. While a pause lasts, a client's command that it holds waits, with everything the client sent after it, and the held clients run in arrival order when the pause ends or becomes less strict; they count in `blocked_clients` and show flag `b` in `CLIENT LIST`, and a held client that disconnects is dropped. `WRITE` holds every command Redis flags write or may-replicate (so `PUBLISH`, `PFCOUNT` and `EVAL`/`EVALSHA`/`FCALL` without declared flags too), a script or function by the flags it declares (`no-writes` runs), and `EXEC` of a transaction that holds such a command; a write sent inside `MULTI` waits before it is queued; `ALL` holds every command. The checks that refuse a command (unknown command, wrong arguments, `-NOAUTH`, `-MISCONF`, `-READONLY` and so on) still answer at once. A pause sent inside a transaction starts after it, so the writes queued after it still run. Like Redis, a pause also stops key expiry (an expired key is hidden from commands but kept, counted by `DBSIZE`, `INFO keyspace` and not by `expired_keys`, and expired after the pause), eviction (and with it `-OOM` refusals) and replica traffic (the `REPLCONF GETACK` that `WAIT` sends waits for the end of the pause), and a paused replica applies nothing its master sends until the pause ends (what it holds is dropped if `REPLICAOF` changes the master or a full resync replaces the dataset meanwhile, so nothing from an old stream runs later). Without cluster mode, `ASKING`, `READONLY` and `READWRITE` are refused with Redis's "This instance has cluster support disabled", and `MIGRATE` sends `ASKING` before `RESTORE` only when PeaDB runs in cluster mode (Redis sends `RESTORE-ASKING` only then), so a `MIGRATE` to a server without cluster mode no longer fails on that step. `SCRIPT DEBUG` checks its arguments like Redis (`wrong number of arguments for 'script|debug' command`, `Use SCRIPT DEBUG YES/SYNC/NO`, and `SCRIPT DEBUG must be called outside a pipeline` inside a transaction or after another command in the same pipeline), `SCRIPT DEBUG NO` replies `OK`, and `SCRIPT DEBUG YES` and `SYNC` are refused with "SCRIPT DEBUG YES and SYNC are not supported by PeaDB yet (it has no Lua debugger)", so no script runs with kept effects when its writer expected a debug session (the debugger itself is item 14).
- Missing: nothing for these commands, with one limit that comes from item 44: a paused replica still acknowledges the bytes it has received, not the ones it has applied, so `WAIT` on its master can count it as caught up while the pause holds those writes (Redis acknowledges the applied offset). On purpose, `CLIENT UNBLOCK` of a client that `CLIENT PAUSE` holds returns 0 and leaves it held, where Redis 7.2.5 crashes (`replyToBlockedClientTimedOut()` has no case for a postponed client). A pause is set only by `CLIENT PAUSE`: PeaDB has no `FAILOVER` (item 68) and its `SHUTDOWN` does not wait for replicas (item 33), the two other ways Redis pauses clients. The upstream `unit/pause` tests skipped are the two that read `slave_repl_offset` and `slave_read_repl_offset` from `INFO` (item 22; a replica counts received bytes, not applied ones, item 44) and the one that matches the script name and line Redis appends to script errors (item 14).
- Tracking: none for these commands.

27. RDB CRC64 checksum uses the wrong table
- Status: open defect. Evidence: reproduced.
- Owner: persistence-compat
- Severity: critical
- Target milestone: R1
- Implemented: RDB files and DUMP payloads written by PeaDB load back into PeaDB, and the bytes before the checksum follow Redis's format (real Redis read every key of a PeaDB file before it stopped at the checksum).
- Missing: `crc64_init` (`src/rdb.cpp:104-124`) uses the polynomial `0xad93d23594c935a9` as is in a reflected (right-shift) loop, where Redis's reflected table needs `0x95ac9329ac4bc9b5`. PeaDB computes `crc64("123456789") = 0xcf228cf2176e85ed`; Redis's self-test expects `0xe9c6d914c4b8d9ca` (`crc64.c:135`). As a result PeaDB refuses to start on a real Redis `dump.rdb` ("RDB CRC64 checksum mismatch"), real Redis refuses a PeaDB `dump.rdb` ("Wrong RDB checksum"), a real Redis replica of a PeaDB master exits during full sync, and `RESTORE` fails across engines in both directions ("DUMP payload version or checksum are wrong"), so `MIGRATE` and `redis-cli --cluster add-node` fail too. No test caught it because every test loads PeaDB files into PeaDB. Only the Redis master to PeaDB replica path skips the check (`verify_crc=false`), and that path fails for other reasons (item 50).
- Tracking: fix the table, add a known-answer unit test, keep loading files and payloads written with the old checksum, and add cross-load tests against the pinned Redis 7.2.5 in both directions.

28. A restart with both `dump.rdb` and an AOF applies writes twice
- Status: open defect. Evidence: reproduced.
- Owner: persistence-compat
- Severity: critical
- Target milestone: R1
- Implemented: an RDB alone and an AOF alone each load correctly.
- Missing: with `appendonly yes` and a `dump.rdb` in `dir` (after any SAVE, BGSAVE, save point or SHUTDOWN SAVE), startup loads the RDB and then replays the whole AOF on top of it (`src/main.cpp:186-227`), so INCR, LPUSH, APPEND, XADD and every other non-idempotent write is applied twice on each restart. Redis loads only the AOF when AOF is on.
- Tracking: load only the AOF when `appendonly yes`, and add a restart test with both files present.

29. Lists with 65,536 or more items are cut in RDB files and full sync
- Status: open defect. Evidence: reproduced.
- Owner: persistence-compat
- Severity: critical
- Target milestone: R1
- Implemented: lists with up to 65,535 items save and load correctly.
- Missing: the writer puts a whole list into one listpack (`src/rdb.cpp:731-736`) whose entry count is a 16-bit field (`uint16_t count_`, `src/rdb.cpp:518`), so a list with N ≥ 65,536 items comes back with only N mod 65,536 items after SAVE or BGSAVE and on every PSYNC full sync to a replica. No error is shown.
- Tracking: split large lists into several quicklist nodes like Redis, and add a round-trip test with more than 65,536 items checked by real Redis's `redis-check-rdb`.

30. A torn AOF tail is kept, so the next restart fails
- Status: open defect. Evidence: reproduced.
- Owner: persistence-compat
- Severity: critical
- Target milestone: R1
- Implemented: startup accepts a torn last command (`aof-load-truncated yes`, logged) and refuses corruption in the middle of the file (item 3).
- Missing: after it accepts the torn tail, PeaDB does not cut the file (`src/command.cpp:11133-11139`) and opens it for append as it is (`O_APPEND`, `:1672-1674`), so new writes land after the torn bytes. The next restart then finds corruption in the middle of the file and refuses to start, and a repair by hand loses every write since the first restart. Redis truncates the file to the last good command.
- Tracking: truncate to the last good offset before appending, and add a test that crashes and restarts twice.

31. LTRIM, LSET, LREM, LINSERT, ZREM and ZREMRANGEBY* remove the key's TTL
- Status: open defect. Evidence: reproduced.
- Owner: datatypes-compat
- Severity: critical
- Target milestone: R1
- Implemented: the commands return the right replies and keep the right elements.
- Missing: each handler copies the collection, deletes the key and writes it back (`src/command.cpp:8489` LSET, `:8525` LREM, `:8632` LINSERT, `:8660` LTRIM, `:8882` ZREMRANGEBYSCORE/RANK/LEX, `:8958` ZREM), which drops `expire_at_ms`, so a key that should expire lives for ever. The copy also makes them O(N) per call (item 56).
- Tracking: change the collection in place inside `DataStore` (old plan Track 6 "in-place list ops"), and add a TTL test for each command.

32. SCAN misses keys when the keyspace grows during a scan
- Status: open defect. Evidence: reproduced.
- Owner: keyspace-compat
- Severity: critical
- Target milestone: R1
- Implemented: SCAN returns every key when the keyspace does not grow during the scan.
- Missing: the cursor is a raw bucket index of a `std::unordered_map` (`src/datastore.cpp:3129-3141`, `include/datastore.hpp:475`), whose bucket counts are not powers of two; when the table grows during a scan, keys move to buckets the cursor has already passed. In the test, about 12.5% of the keys that existed for the whole scan were never returned, which breaks Redis's SCAN guarantee. Migration and backup tools copy data with SCAN. `HSCAN`, `SSCAN` and `ZSCAN` use the same cursor over the collection's own table (`src/datastore.cpp:867`, `:1127`, `:1245`), so they can miss elements when the collection grows during a scan (seen in code 2026-10-05, not run).
- Tracking: a reverse-binary cursor over a power-of-two table like Redis's `dictScan`, or a table that rehashes in steps (item 13).

33. Shutdown and signal handling
- Status: open defect. Evidence: reproduced for SIGTERM and SIGINT; seen in code for SIGHUP and `SHUTDOWN ABORT` (checked again 2026-10-05); code reading for the wait for replicas.
- Owner: persistence-compat
- Severity: high
- Target milestone: R1
- Implemented: `SHUTDOWN [SAVE|NOSAVE]` saves on request and fsyncs the AOF before the process exits.
- Missing: SIGTERM and SIGINT only stop the event loop (`src/main.cpp:22-24`, `src/server.cpp:1013-1024`). There is no final RDB save when save points are set (Redis does one), so an RDB-only server loses every write since the last save point on each `systemctl`, Docker or Kubernetes stop (1 of 1001 keys survived, against 1001 on Redis); an AOF server keeps its data and only skips the final fsync. SIGHUP is not handled (`src/main.cpp:58-60`), so its default action kills the server without saving; Redis ignores it. `SHUTDOWN ABORT` (and `NOW`, `FORCE`) is accepted as a no-op and the server then shuts down (`src/command.cpp:3229`); Redis replies "No shutdown in progress." and keeps running. `SHUTDOWN SAVE NOSAVE` is accepted (the last one wins), where Redis returns a syntax error. A successful SHUTDOWN replies `+OK`, where Redis closes the connection with no reply, and a failed save replies "Check logs." but logs nothing. SHUTDOWN does not wait for replicas to catch up (there is no `shutdown-timeout`).
- Tracking: one clean-shutdown path for the command and the signals (old plan Task 1.1), then adopt the upstream `unit/shutdown` and `integration/shutdown` suites.

34. SAVE and BGSAVE safety and status
- Status: open defect. Evidence: reproduced for the shared temp file and the save time; seen in code for the fork failure (checked again 2026-10-05).
- Owner: persistence-compat
- Severity: high
- Target milestone: R1
- Implemented: BGSAVE forks a child that writes a temp file and renames it, and a second BGSAVE during a running one is refused.
- Missing: foreground SAVE and the BGSAVE child write the same temp file, `dump.rdb.tmp` (`src/rdb.cpp:818`); SAVE does not check for a running BGSAVE (`src/command.cpp:4096-4103`), and SHUTDOWN SAVE does not stop a running BGSAVE child, so two saves can corrupt each other's file or replace a newer dump with an older one. A failed `fork()` returns without a status change or log line (`src/command.cpp:519-520`): the BGSAVE command tells its caller "fork error", but for the save points nothing reports it — `rdb_last_bgsave_status` stays `ok`, no MISCONF is raised, nothing is logged, and the cron retries on every loop pass. The child drops its error text (`:522-524`). After a failed BGSAVE, `rdb_last_save_time` still moves forward (`:11872-11873`). `LASTSAVE` is missing (item 68).
- Tracking: per-process temp file names like Redis's `temp-<pid>.rdb`, an in-progress guard for SAVE, and status plus log lines for fork failures and child errors.

35. AOF write and fsync errors
- Status: open defect. Evidence: reproduced for the flag that is never cleared; seen in code for `appendfsync always` (checked again 2026-10-05).
- Owner: persistence-compat
- Severity: high
- Target milestone: R1
- Implemented: a failed AOF write or fsync sets `g_aof_write_error`, and later writes are refused with `-MISCONF`.
- Missing: nothing ever clears the flag (declared at `src/command.cpp:106`, set at `:1701`, `:1705`, `:1712` and `:11512`), so one short disk error keeps the server read-only until a restart, where Redis clears it after the next good write. With `appendfsync always`, a write whose `write(2)` or `fsync` failed is still answered `OK` (`src/command.cpp:1703-1713`), so the client is told that data which is not on disk is safe; Redis exits in this case (`aof.c:1192-1199`).
- Tracking: clear the flag after a good write and fsync, exit like Redis under `always`, and add fault-injection tests.

36. AOF content: no transaction framing, and eviction DELs in the wrong database
- Status: open defect. Evidence: seen in code (checked again 2026-10-05).
- Owner: persistence-compat
- Severity: high
- Target milestone: R1
- Implemented: every write reaches the AOF, with a `SELECT` whenever the client's database changes, and the replication backlog wraps EXEC in MULTI/EXEC.
- Missing: commands inside MULTI/EXEC and inside scripts are written to the AOF one by one with no MULTI/EXEC around them (`src/command.cpp:1684-1696`; only the backlog is wrapped, `:3459-3469`), and AOF load keeps every complete command before a torn tail (`:11125-11152`), so a crash can replay half a transaction, for example the debit without the credit. Script effects are not wrapped for replicas either. Redis wraps each unit and drops an incomplete MULTI at load (`aof.c:1586`). Eviction writes a bare `DEL` to the AOF without `SELECT` (`src/command.cpp:1749`), so with several databases a restart deletes a key of the same name in another database and brings the evicted key back; the backlog path just above it sends `SELECT` correctly.
- Tracking: MULTI/EXEC framing for transactions and scripts in the AOF and the backlog, dropping an incomplete MULTI at load, and a `SELECT` before eviction DELs.

37. Some paths delete expired keys without DEL or `expired` events
- Status: open defect. Evidence: reproduced for KEYS and the cluster slot commands; seen in code for SAVE and full sync (checked again 2026-10-05).
- Owner: replication-compat
- Severity: high
- Target milestone: R1
- Implemented: active expiry and the expired-key sweep that RANDOMKEY and SCAN run send `DEL` to replicas and the AOF and emit `expired` events.
- Missing: `DataStore::keys()` and `for_each_key()` erase expired entries with no propagation (`src/datastore.cpp:392-419`). They run under KEYS (`src/command.cpp:5819-5826`), `CLUSTER COUNTKEYSINSLOT` and `GETKEYSINSLOT`, and the RDB writer (`src/rdb.cpp:689`), which runs in the master process for foreground SAVE, SHUTDOWN SAVE and every PSYNC full sync. Replicas then keep keys the master has dropped, and the `expired` events that audit or cache flows rely on are lost.
- Tracking: send a DEL and an event wherever an expired key is erased, or let read-only walks skip expired keys without erasing them, as Redis does for KEYS and RDB save.

38. `used_memory` is slow to compute and too low
- Status: open defect. Evidence: reproduced (macOS; the size of the undercount on Linux is not measured).
- Owner: memory-eviction
- Severity: critical
- Target milestone: R1
- Implemented: `used_memory` is an estimate from a full walk of every key and element, cached for 100 ms (`src/datastore.cpp:2512-2526`); eviction and the OOM check use it.
- Missing: the cache time is taken before the walk, so once one walk takes 100 ms or more (about 2M keys), every write with `maxmemory` set walks again: at 2M keys writes fell from about 36,000/s to 3–9/s, and going over the limit froze the server for minutes. The estimate also leaves out hash-table nodes and buckets, allocator overhead, `std::deque` blocks and the second copy of each zset member (`src/datastore.cpp:2470-2489`): with `maxmemory 64mb`, PeaDB grew to 843 MB RSS with zero evictions. So `maxmemory` does not protect the host. INFO runs the same walk (item 55).
- Tracking: keep a running total updated where writes happen, keep the full walk only as a periodic self-check (old plan Track 6 "incremental used_memory"), and measure again on Linux.

39. The OOM check refuses DEL and other writes that free memory
- Status: open defect. Evidence: seen in code.
- Owner: memory-eviction
- Severity: high
- Target milestone: R1
- Implemented: over `maxmemory` with `noeviction`, writes are refused with `-OOM`; CONFIG, FLUSHALL, FLUSHDB and `allow-oom` scripts are exempt.
- Missing: there is no `denyoom` command flag (`CmdFlag`, `src/command.cpp:65-75`), and the check uses the `write` flag (`:10652-10658`), so DEL, UNLINK, EXPIRE, HDEL, LPOP and every other write is refused too. A full server with `noeviction` (the default policy) cannot be cleaned up except by FLUSHALL, FLUSHDB or a higher `maxmemory`. Redis refuses only `denyoom` commands (`server.c:3997-4014`).
- Tracking: carry Redis's `denyoom` flag from the generated command metadata into the check.

40. Writes larger than the replication backlog never reach replicas
- Status: open defect. Evidence: reproduced (a 2,000,000-byte SET was missing on the replica while the link stayed up).
- Owner: replication-compat
- Severity: critical
- Target milestone: R2
- Implemented: writes reach replicas through a ring buffer of `repl-backlog-size` bytes (default 1 MiB) that is flushed once per event-loop pass.
- Missing: when more than the backlog size is written in one loop pass (one big value, a large MSET or RPUSH batch, an EXEC or script with many writes, or many clients at once), the ring overwrites bytes no replica has read yet (`src/command.cpp:223-224`); since `CONFIG SET repl-backlog-size` works (item 25), a smaller backlog (down to Redis's 16 KB floor) makes this happen with less data, and shrinking it can leave an attached replica that is behind below the bytes still held. `get_slice` then fails (`:229-230`), `replication_event_at` returns an empty string (`:11260-11263`), and the replica's position jumps past the lost bytes (`src/server.cpp:646-649`). There is no error, no disconnect and no log line; `master_link_status` stays up, and the replica's ACK stays behind for ever, so WAIT only times out. `repl-backlog-size` cannot be changed with CONFIG SET. Redis keeps every block a replica still needs and drops a slow replica only through its output-buffer limit.
- Tracking: send each replica its pending bytes before the ring overwrites them (or disconnect it and force a full sync), and add a test with a write bigger than the backlog.

41. A failed `SET ... NX` with a TTL is replicated as a plain SET
- Status: open defect. Evidence: reproduced (the master kept `v1`, the replica got `v2`).
- Owner: replication-compat
- Severity: critical
- Target milestone: R2
- Implemented: SET with a TTL is rewritten to an absolute `PXAT` for replicas and the AOF, like Redis does.
- Missing: every write whose reply is not an error is propagated (`src/command.cpp:10831-10835`), and a SET that did nothing because of NX or XX replies nil, which is not an error. The rewrite (`:1540-1542`) also drops NX and XX and takes the TTL of the key that already exists. So each losing `SET lock token NX PX 30000` (the standard lock and idempotency-key pattern) overwrites the value on every replica and in the AOF, and after a failover or an AOF restart the lock belongs to the wrong client. Redis propagates nothing when the SET did not happen (`t_string.c:99-106`).
- Tracking: propagate only writes that changed data (the same question as the dirty counter, item 18), and add tests for failed SET NX and XX with and without a TTL.

42. RESTORE with a TTL is replicated with a broken payload
- Status: open defect. Evidence: reproduced (the key existed on the master and not on the replica).
- Owner: replication-compat
- Severity: critical
- Target milestone: R2
- Implemented: RESTORE works on the server that receives it.
- Missing: when the restored key has a TTL, the propagated command wraps the payload in literal braces (`src/command.cpp:1564`), so the payload checksum fails on every replica, where the key is lost and the error is dropped, and on AOF reload, which stops startup ("AOF replay command failed", `:11146-11148`). Migration tools and MIGRATE into PeaDB use RESTORE, and most cache and session keys have a TTL. The upstream `unit/expire` propagation test passes only because the braces match its TCL pattern `{restore foo6 * {*} ABSTTL}`.
- Tracking: propagate the payload unchanged, with the absolute TTL and `ABSTTL` (`cluster.c:6700-6702`), and check it with an AOF reload and a replica, not only the upstream test.

43. Replicated and AOF-replayed writes are blocked by client-side checks
- Status: open defect. Evidence: seen in code; reproduced for the cluster routing check.
- Owner: replication-compat
- Severity: critical
- Target milestone: R2
- Implemented: the MISCONF and READONLY checks skip writes that come from the master stream or AOF replay (`g_replaying_aof`, `src/command.cpp:10644`, `:10695-10696`).
- Missing: the OOM check (`:10652`), the NOREPLICAS check (`:10660`) and the cluster routing check (`:10724-10743`) do not skip them, and the replica apply loop throws the reply away (`:11553-11558`). So a replica over its own `maxmemory` silently drops every replicated write, including the master's DELs, and never recovers; a node made a replica after `min-replicas-to-write` was set drops every write; and a cluster-enabled replica that knows the slot map drops writes for slots it does not own. In each case the link stays up and WAIT still counts the replica (item 44). An AOF whose data once went over `maxmemory` cannot be loaded at startup. Redis uses one rule for all of these checks (`mustObeyClient`).
- Tracking: one exemption for the master stream and AOF replay in every client-side check.

44. Replicas acknowledge data before they apply it
- Status: open defect. Evidence: reproduced (WAIT returned 1 for writes the replica had dropped); seen in code.
- Owner: replication-compat
- Severity: high
- Target milestone: R2
- Implemented: replicas send `REPLCONF ACK` every second and at once on `GETACK`, and WAIT counts these ACKs.
- Missing: the replica ACKs the bytes it has received and parsed (`src/command.cpp:11847`), not the bytes it has applied, so WAIT reports writes as replicated even when the replica dropped them (items 40 and 43).
- Tracking: ACK the applied offset.

45. Replicas do not persist replicated data and forget their role
- Status: open defect. Evidence: seen in code (checked again 2026-10-05), not yet reproduced.
- Owner: replication-compat
- Severity: critical
- Target milestone: R2
- Implemented: a replica loads the full-sync RDB and applies the master stream in memory.
- Missing: replicated writes are applied with `g_replaying_aof` set, and the AOF writer returns early in that case (`src/command.cpp:1686`), so a replica's own AOF never holds the master's data; this is also true of an AOF turned on with `CONFIG SET appendonly yes` on a replica (item 25). The full-sync RDB is written to `/tmp/peadb_fullsync_<pid>.rdb.tmp` (`:11718`, outside `--dir`, opened without `O_EXCL`) and deleted after the load (`:11766`); it is never kept as `dbfilename`. There is no `replicaof` config directive (item 24) and no CONFIG REWRITE, so the role cannot survive a restart. As a result, after a manual failover (`REPLICAOF NO ONE`) the new master's first restart loads only the writes made after the promotion, and a replica that restarts comes back as a writable master with old or empty data. Redis keeps the received RDB as its dump file, rewrites its AOF after a sync and appends the master stream to its AOF.
- Tracking: keep the received RDB under `dir`, write replicated commands to the replica's AOF, and support `replicaof` in the config file.

46. Full sync freezes the master and fails for large datasets
- Status: open defect. Evidence: reproduced (macOS).
- Owner: replication-compat
- Severity: critical
- Target milestone: R2
- Implemented: PSYNC full sync sends a complete RDB, streams included, to PeaDB replicas (a real Redis replica rejects it today because of the checksum, item 27).
- Missing: the master builds the whole RDB in memory on the event loop and returns it as one reply (`src/command.cpp:3317-3328`), so every client waits 6.7 s at 1M keys and 17 s at 3M keys on each attempt. The reply goes through the replica's output buffer, whose hard limit closes the replica (`src/server.cpp:414-423`): above about 2.3M small keys the sync never finishes and retries for ever. Because PeaDB replicas always ask for a full sync (item 47), every reconnect does this. Redis forks a child and streams the RDB.
- Tracking: a fork-based or diskless full sync that streams in chunks (item 8; old plan Track 7).

47. Partial resync and link checks
- Status: open defect. Evidence: seen in code for `PSYNC ? -1`; code reading for the one-byte offset; reproduced for the missing timeouts.
- Owner: replication-compat
- Severity: high
- Target milestone: R2
- Implemented: the master answers `+CONTINUE` for a replica whose replid and offset are still in the backlog, and accepts the previous replid after a promotion (item 8).
- Missing: PeaDB replicas always send `PSYNC ? -1` (`src/command.cpp:11678`), so every reconnect is a full sync (item 46). For a real Redis replica, the `+CONTINUE` data looks one byte late: the replica's offset (the next byte it needs) is used as a 0-based index (`:3304-3307`). There is no `repl-timeout`, no master PING to replicas, no `master_last_io_seconds_ago` and no TCP keepalive on the replica's socket, so a replica of a hung or vanished master serves stale data for ever while it reports the link as up.
- Tracking: replica-side partial resync, an offset check against real Redis, and `repl-timeout` plus `repl-ping-replica-period` (old plan Tasks 1.5 and 1.6).

48. XCLAIM and XAUTOCLAIM are replicated as sent
- Status: open defect. Evidence: seen in code (checked again 2026-10-05); the divergence is not measured.
- Owner: streams
- Severity: medium
- Target milestone: R2
- Implemented: XCLAIM and XAUTOCLAIM change the pending-entries list correctly on the master.
- Missing: there is no propagation rewrite for them (`canonicalize_propagated_command`, `src/command.cpp:1435-1622`), so replicas and AOF replay run the claim again against their own clock and PEL times, and the `IDLE`, `TIME` and `RETRYCOUNT` options are skipped on every call (`:10215`), not only on replay as item 5 said before. Pending owners can therefore differ on replicas, so after a failover messages are processed twice or stay stuck. Redis propagates each claimed entry as `XCLAIM ... TIME <ms> RETRYCOUNT <n> FORCE JUSTID LASTID <id>`.
- Tracking: apply the options and propagate Redis's rewritten form.

49. No migration or rollback path works end to end
- Status: open defect. Evidence: each part is reproduced or seen in code (see the items named below).
- Owner: persistence-compat
- Severity: critical
- Target milestone: R3
- Implemented: replication, RDB files and DUMP/RESTORE work between PeaDB servers, apart from the defects in items 29 and 40–46. The AOF is plain RESP commands, so the checksum bug does not affect it (loading a PeaDB AOF into Redis was not tested).
- Missing: every documented path between Redis and PeaDB fails today. Redis to PeaDB by REPLICAOF: item 50. Native RDB files in both directions, and a Redis replica of a PeaDB master: item 27. DUMP, RESTORE and MIGRATE: items 27 and 51. The bridge scripts: item 52. Plain SYNC serves an empty dataset (item 8). RDB files from a Redis with more than 16 databases: item 53.
- Tracking: close the items above, then add one end-to-end acceptance test in both directions as a release gate, with `requirepass`, several databases, more than 2M keys, streams with consumer groups, and functions.

50. PeaDB as a replica of a real Redis master
- Status: open defect. Evidence: reproduced for the keep-alive newline; seen in code for `masteruser`.
- Owner: replication-compat
- Severity: critical
- Target milestone: R3
- Implemented: PeaDB replicates from Redis 7.2.5 when the master's RDB is ready within about 1 s, and it authenticates with `masterauth`.
- Missing: after `+FULLRESYNC`, the replica accepts only one empty keep-alive line before the `$<len>` header (`src/command.cpp:11700-11708`). Redis sends one newline per second while its BGSAVE runs, so the sync fails for any master whose RDB takes more than about 1 s to produce, which is any real production dataset. PeaDB also cannot send a user name (it sends `AUTH <masterauth>` only, `:11650-11651`; there is no `masteruser`), so it cannot sync from an ACL-hardened master whose default user is disabled.
- Tracking: skip any number of keep-alive lines, add `masteruser`, and test against the pinned Redis with a slow BGSAVE.

51. DUMP, RESTORE and MIGRATE with real Redis
- Status: open defect. Evidence: reproduced.
- Owner: persistence-compat
- Severity: high
- Target milestone: R3
- Implemented: DUMP, RESTORE and MIGRATE work between PeaDB servers for strings, lists, sets, hashes and zsets (item 19 covers the advanced MIGRATE options).
- Missing: `MIGRATE` to a real Redis fails because of the checksum and encoding gaps below; the `ASKING` it used to send first to a non-cluster target, which Redis refuses, is fixed (2026-10-06, item 26: PeaDB sends `ASKING` only in cluster mode, where Redis sends `RESTORE-ASKING`, which PeaDB does not have yet, item 68). `DUMP` of an existing stream returns nil and `MIGRATE` of one returns NOKEY (`:5913-5918`; `rdb_dump_key` has no stream case, `src/rdb.cpp:1251-1302`), so tools think the key does not exist. `RESTORE` accepts only the plain string, list, set, hash and zset types (`src/rdb.cpp:1317-1441`) and rejects the listpack, intset, quicklist and stream encodings that real Redis DUMP produces ("Bad data format"). The checksum bug (item 27) comes on top of all of this.
- Tracking: add stream DUMP, and decode Redis's compact encodings (together with RDB v11 load, item 2).

52. The RDB bridge scripts fail silently
- Status: open defect. Evidence: seen in code.
- Owner: persistence-compat
- Severity: critical
- Target milestone: R3
- Implemented: against a server with no password, `scripts/redis/import_rdb_via_redis.py` and `export_rdb_via_redis.py` copy database 0's strings, lists, sets, hashes, zsets and stream entries through a temporary Redis.
- Missing: neither script has a password option or sends AUTH, and neither checks replies, so against a server with `requirepass` the export writes an empty RDB and the import copies nothing, and both print "complete" and exit 0. They copy only database 0 (no SELECT), copy stream entries without consumer groups, skip functions, and a second import appends list items again (RPUSH).
- Tracking: add authentication, check every reply and exit non-zero on error, cover all databases, consumer groups and functions, and test with `requirepass`.

53. RDB load edge cases with real Redis files
- Status: open defect. Evidence: reproduced by one reviewer for the database index and subnormal scores; seen in code for type 14.
- Owner: persistence-compat
- Severity: medium
- Target milestone: R3
- Implemented: RDB load handles the types and encodings Redis 7.2.5 writes for 16 databases (once item 27 is fixed).
- Missing: a `SELECTDB` of 16 or more is ignored (`src/rdb.cpp:951-961` drops the result of `select_db`), so keys from a Redis with more than 16 databases go into the previous database and can overwrite keys there; Redis refuses such a file. Subnormal zset scores such as `5e-324` in a listpack zset load as 0.0 (`std::stod` errors are ignored, `src/rdb.cpp:1188-1191`). Lists in the pre-7.0 quicklist format (type 14) are not supported, and `RDB_TYPE_LIST_QUICKLIST` is defined as 15 (`src/rdb.cpp:80`), which is Redis's stream type number.
- Tracking: refuse out-of-range databases with a clear error, parse scores like Redis, and decide on pre-7.0 support.

54. Scripts that contain `while true do` never run
- Status: open defect. Evidence: seen in code.
- Owner: scripting-compat
- Severity: high
- Target milestone: R4
- Implemented: SCRIPT KILL and FUNCTION KILL clear a BUSY state, and the upstream kill tests pass.
- Missing: EVAL and EVALSHA search the script text for `while true do` or `while 1 do` and, when found, do not run the script but set the server BUSY (`src/command.cpp:2117-2123`); FCALL searches the whole library text (`:1248-1262`). So a normal loop such as `while true do ... if cursor == '0' then break end end` (common in cleanup and queue scripts) never runs, and every other client gets BUSY until someone sends SCRIPT KILL. A real long-running script is never interrupted: the Lua hook only sets a flag (`lua_timeout_hook`, `src/lua_engine.cpp:73`) and does not serve other clients, so SCRIPT KILL cannot arrive; Redis serves events after `busy-reply-threshold`. This code exists to pass upstream tests, so those passes overstate parity.
- Also shaped to the tests (seen in code 2026-10-06): for a script without a shebang, `EVAL` decides whether to refuse it with `-NOREPLICAS` by searching its text for ten `redis.call('set'`-style patterns (`script_has_write_intent()` in `src/command.cpp`), where Redis checks `min-replicas-to-write` on each write command the script calls, so a script that writes with any other command runs with too few replicas.
- Tracking: run every script, and serve SCRIPT KILL, FUNCTION KILL and SHUTDOWN NOSAVE from the Lua hook after the busy threshold, like Redis; check `min-replicas-to-write` per write command inside scripts.

55. INFO, SCAN and RANDOMKEY walk the whole keyspace
- Status: open defect. Evidence: reproduced (macOS).
- Owner: keyspace-compat
- Severity: high
- Target milestone: R5
- Implemented: the replies are correct.
- Missing: every INFO call, for any section, computes `used_memory` (a full walk when the 100 ms cache is stale, item 38) and walks each database for the keyspace stats (`src/command.cpp:3902`, `:4082`; `src/datastore.cpp:2671-2685`): 105–252 ms per call at 1.4M keys, against under 1.2 ms in Redis, so each monitoring scrape stalls every client. SCAN and RANDOMKEY first collect all expired keys with a full walk (`src/command.cpp:5830`, `:5856`, `:1650`; `src/datastore.cpp:3160-3172`): about 43 ms per call at 1M keys. SCAN with MATCH stops only after COUNT matches (`src/datastore.cpp:3139`), so one call can visit every key, where Redis visits about COUNT buckets. The cost of `CLUSTER COUNTKEYSINSLOT` and `GETKEYSINSLOT` is in item 6.
- Tracking: keep keyspace stats as counters, give the lazy-expire sweep a budget (old plan Track 6; keep its ordering guard), and count visited buckets in SCAN.

56. Large-collection commands copy the whole collection
- Status: open defect. Evidence: reproduced (macOS).
- Owner: datatypes-compat
- Severity: high
- Target milestone: R5
- Implemented: the replies are correct.
- Missing: commands that are O(1) or O(log N) in Redis copy the full collection first (`store().zrange/lrange/smembers/hgetall(..., 0, -1)`): ZCARD (`src/command.cpp:8714`), ZSCORE (`:8724`), ZRANK and ZREVRANK (`:8790`, `:8811`), ZINCRBY (`:8975`), ZREM (`:8947`, `:8958`, which also deletes and re-adds every remaining member), LINDEX (`:8465`), LSET (`:8482`), LTRIM (`:8660`), SRANDMEMBER (`:9376`) and HRANDFIELD (`:9589`). At 1M members, ZSCORE takes about 8 ms, ZREM 300–400 ms and HRANDFIELD about 46 ms, and each call blocks every client.
- Tracking: read and change collections in place (old plan Track 6 ZRANK item; the in-place changes also fix item 31).

57. Active expiry is too slow
- Status: open defect. Evidence: reproduced (macOS).
- Owner: memory-eviction
- Severity: high
- Target milestone: R5
- Implemented: active expiry runs every 100 ms on the master and propagates DEL.
- Missing: each run samples at most 64 entries per database, with or without a TTL (`include/datastore.hpp:395`, `src/datastore.cpp:2942-2973`, `src/server.cpp:690-693`), and never repeats when many keys are expired, so it clears about 640 keys per second: 1M expired keys take about 26 minutes, where Redis clears them in under 2 s. Until then the expired keys use memory and count toward `maxmemory`.
- Tracking: sample only keys with a TTL, repeat while the expired share is high, and add a time budget like Redis's `activeExpireCycle`.

58. Persistence write cost on the event loop
- Status: open defect. Evidence: reproduced (macOS).
- Owner: persistence-compat
- Severity: high
- Target milestone: R5
- Implemented: AOF and RDB data are correct and follow the chosen fsync policy.
- Missing: with AOF on, each write command does its own `write(2)` (`src/command.cpp:1703`) and the `everysec` fsync runs on the event-loop thread (`:11508-11516`, `src/server.cpp:695`): SET runs at 0.27× Redis, and a slow disk adds latency to every client. Every write is also encoded and copied byte by byte into the replication backlog, even with no replica attached (`src/command.cpp:223-224`): 10 KB SETs run at 0.34× Redis. RDB save is 7–30× slower than Redis and builds the whole file in one string plus a copy of every key name (`src/rdb.cpp:812-814`, `:689`), so the BGSAVE child needs extra memory of at least the RDB size; there is no LZF compression, so files are larger for compressible values.
- Tracking: batch AOF writes per loop pass and fsync on a background thread (old plan Task 1.7), skip the backlog copy when no replica needs it, and stream the RDB to the file.

59. No automatic AOF rewrite
- Status: missing feature. Evidence: seen in code.
- Owner: persistence-compat
- Severity: medium
- Target milestone: R5
- Implemented: `BGREWRITEAOF` rewrites the AOF safely on request (item 3).
- Missing: `auto-aof-rewrite-percentage` and `auto-aof-rewrite-min-size` do not exist, and nothing starts a rewrite on its own, so with AOF on the file grows until an operator runs BGREWRITEAOF or the disk is full. Redis rewrites at +100% growth over 64 MB by default.
- Tracking: old plan Task 1.4; it also unblocks the upstream `unit/aofrw` suite.

60. Audit trail and logging
- Status: open defect. Evidence: reproduced for the missing counters; code reading for the log events.
- Owner: observability
- Severity: medium
- Target milestone: R6
- Implemented: a failed AUTH is counted as `errorstat_WRONGPASS`, and warnings and errors go to stdout with a timestamp.
- Missing: INFO has no `acl_access_denied_auth`, `_cmd`, `_key` or `_channel` counters and no `rejected_connections`, so alerts on failed logins never fire, and there is no `ACL LOG` (item 7) to tie a failed login to an address and a time. Logs go only to stdout (`src/logger.cpp:48-57`): there is no `logfile`, syslog or pidfile (item 24), and the line format (no milliseconds, pid or role) differs from Redis's. No log line records a SHUTDOWN or SIGTERM, a BGSAVE start, success or failure, an RDB or AOF load with key counts, a REPLICAOF role change, a replica attaching, or a MODULE LOAD, although some error replies say "Check logs" (`src/command.cpp:3235`, `:10649`). Redis 7.2.5 does not log failed AUTH either, but it logs the other events.
- Tracking: add the counters, `logfile`, syslog and pidfile, and log lines for these events (old plan Task 1.12).

61. Security defaults weaker than Redis 7.2.5
- Status: open defect. Evidence: reproduced for DEBUG and CLIENT SETINFO; seen in code for the rest.
- Owner: security-compat
- Severity: medium
- Target milestone: R6
- Implemented: protected-mode is on by default, `MODULE LOAD` needs `enable-module-command`, and `rename-command` can disable any command (item 7).
- Missing: DEBUG works for every authenticated client (`src/command.cpp:5051`), while Redis 7.2.5 refuses it unless `enable-debug-command` is set; PeaDB's DEBUG includes SLEEP, POPULATE, RELOAD, LOADAOF, PAUSE-CRON and SET-SKIP-CHECKSUM-VALIDATION (the workaround today is the config-file line `rename-command DEBUG ""`, as in Redis; `enable-debug-command no` or `local` stops startup rather than being ignored, item 24). `CLIENT SETINFO lib-name` and `lib-ver` accept spaces and newlines (`src/command.cpp:8082-8092`), so a client can change how the `CLIENT LIST` output that monitoring and audit tools parse reads; Redis accepts only the characters `!` to `~`. The `requirepass` compare returns early when the lengths differ (`src/command.cpp:178-184`), so timing can reveal the password length; Redis compares hashes. The protected-mode `-DENIED` text differs from Redis's (`src/server.cpp:794-800`). `enable-module-command local` lets every client run `MODULE LOAD`, where Redis allows only a loopback or unix-socket client; PeaDB also gates only `MODULE LOAD` (Redis refuses every `MODULE` subcommand) and with its own error text (seen in code 2026-10-05, the `MODULE` handler in `src/command.cpp`; `SessionState::is_local`, added for `enable-protected-configs local`, is what the fix needs).
- Tracking: add `enable-debug-command` (default `no`) and Redis's attribute check, compare fixed-length hashes, use Redis's text, and gate `MODULE` like Redis's `allowProtectedAction()`.

62. The bundled Lua is stock 5.1.5, not Redis's patched copy
- Status: open defect. Evidence: seen in code (file comparison with Redis's `deps/lua`); reproduced for the table order.
- Owner: scripting-compat
- Severity: medium
- Target milestone: R6
- Implemented: Lua 5.1.5 is bundled on every platform (`third_party/lua-5.1.5`), and a Lua-level sandbox protects globals and removes `os` and `io` (`src/lua_engine.cpp:888-1008`).
- Missing: Redis 7.2.5 patches its Lua, and PeaDB's copy lacks those patches. It has no read-only-table patch (no `readonly` field in `Table`, no `lua_enablereadonlytable`), so global protection relies only on the Lua-level sandbox. It keeps the bytecode loader (`ldo.c:496-498`) that Redis removed, guarded only at the Lua level (`src/lua_engine.cpp:933-939`). It keeps stock `lstring.c` string hashing (`step = (l>>5)+1`, where Redis hashes every byte), so `pairs`/`next` order and `cjson.encode` key order differ from Redis for string keys of 32 bytes or more. There is no record of where the copy came from or which patches it has. Whether the sandbox can be bypassed was not tested.
- Tracking: re-vendor Redis's `deps/lua` or port its patches (an owner decision), record the provenance, and re-run `unit/scripting` and `unit/functions`.

63. Lua library output differs from Redis
- Status: open defect. Evidence: reproduced for cjson integers; seen in code for the rest.
- Owner: scripting-compat
- Severity: high
- Target milestone: R8
- Implemented: `cjson`, `cmsgpack`, `bit` and `struct` are PeaDB's own code (`src/lua_*_stub.c`), and their main functions exist.
- Missing: `cjson.encode` writes every integer-valued number with `.0` (`src/lua_cjson_stub.c:103-110`): PeaDB returns `{"a":1.0}` where Redis returns `{"a":1}`, so consumers that hash, sign, compare or type-check JSON from scripts see different data. `cjson` lacks `encode_sparse_array`, `encode_number_precision`, `decode_invalid_numbers`, `new`, `_NAME` and `_VERSION` (`src/lua_cjson_stub.c:371-377`). `struct.pack` with `s` writes a 4-byte length prefix, where Redis writes the bytes and a trailing NUL (`src/lua_struct_stub.c:121-131`), and the `c`, `x`, `L`, `T` and `!` formats are missing. `cmsgpack` and `bit` output were not compared byte by byte.
- Tracking: match Redis's `lua_cjson.c` and `lua_struct.c`, and add differential tests against the pinned Redis.

64. Cluster routing checks only the first key
- Status: open defect. Evidence: reproduced.
- Owner: cluster-compat
- Severity: critical
- Target milestone: R7
- Implemented: a command whose key spec names a first key gets MOVED or ASK from the real slot owner (item 6).
- Missing: routing hashes only `args[first_key]` (`src/command.cpp:10724-10731`), and there is no CROSSSLOT, so when the first key belongs to this node the other keys of a multi-key write are written here too, even when another node owns them. 23 key-bearing commands whose spec has first key 0 are never routed. Commands queued in MULTI are never routed (they are queued before the check, `:10582-10605`), and EVAL and FCALL keys are not routed, so a transaction or script with one key on another node applies part of its writes on the wrong node. PUBLISH is redirected by channel name and delivered only on the local node, and SSUBSCRIBE is never routed while SPUBLISH is.
- Tracking: route on every key with CROSSSLOT and TRYAGAIN like Redis's `getNodeByQuery`, including MULTI/EXEC and scripts; cluster-wide PUBLISH needs the cluster bus (item 6).

65. No CLUSTERDOWN, no saved topology and no health reporting
- Status: open defect. Evidence: reproduced.
- Owner: cluster-compat
- Severity: critical
- Target milestone: R7
- Implemented: CLUSTER INFO reports `cluster_state:ok` only when all 16384 slots have an owner.
- Missing: that state is only reported, never enforced: a command for an unassigned slot runs locally (`src/command.cpp:10727-10731`), and no CLUSTERDOWN reply exists. With no `nodes.conf`, a restarted node forgets the topology and accepts writes for slots other nodes own (split brain) until an operator enters the topology again. Every node is always shown as healthy, even when it is down: NODES lines say `connected` (`:4489-4490`), SHARDS says `online` (`:4563`), and INFO reports fixed `cluster_slots_pfail:0` and `cluster_slots_fail:0`.
- Tracking: return CLUSTERDOWN for unassigned slots, persist the topology or refuse to serve without it, and report the real link state.

66. Cluster tooling and membership
- Status: open defect. Evidence: reproduced.
- Owner: cluster-compat
- Severity: high
- Target milestone: R7
- Implemented: an operator can build a static topology node by node with MEET, ADDSLOTS(RANGE) and SETSLOT (item 6).
- Missing: `redis-cli --cluster create` never finishes, because nodes do not share membership or slot ownership. Resharding a slot that holds keys breaks it: `MIGRATE ... KEYS` is not supported (`src/command.cpp:6000`), the source sends ASK for every key of a migrating slot without checking whether the key is still local (`:10735-10741`), and `--cluster fix` cannot repair the result. SETSLOT has none of Redis's safety checks (`:4409-4444`): `SETSLOT NODE` on a slot with local keys strands them in a MOVED loop, and extra arguments are accepted. `CLUSTER MEET` returns OK even when the peer never registered this node (because of a password, a hostname, IPv6 or an unreachable address) and logs nothing (`:4277-4279`). A PeaDB node cannot join a running Redis 7.2.5 cluster (`--cluster add-node` fails at FUNCTION RESTORE on the checksum, item 27, and there is no gossip), so there is no node-by-node migration from Redis Cluster.
- Tracking: most of this depends on the cluster scope decision (topology propagation); SETSLOT checks and MEET errors can land first.

67. Cluster-mode leftovers
- Status: open defect. Evidence: code reading and experiment by one reviewer, not re-checked.
- Owner: cluster-compat
- Severity: medium
- Target milestone: R7
- Implemented: `cluster-enabled` and the three `cluster-announce-*` settings work (item 6).
- Missing: SELECT, SWAPDB, MOVE and `COPY ... DB` work in cluster mode, where Redis refuses them, so keys can sit in databases cluster tools never see. 17 of Redis's 21 `cluster-*` settings (`cluster-config-file`, `cluster-node-timeout`, `cluster-require-full-coverage`, `cluster-port` and others) have no effect; since the item 24 fix the log warns about each one. The bus port is always `port + 10000` (`src/server.cpp:272`), which wraps to an unrelated low port above 55535; a bus bind failure is only logged; and the bus accepts any connection, answers any bytes with `PONG` and never closes the connection.
- Tracking: refuse multi-database commands in cluster mode, fail loudly on unsupported cluster settings, and harden the bus listener.

68. Missing commands and an empty COMMAND DOCS
- Status: open defect. Evidence: reproduced for LASTSAVE; seen in code for the rest (checked again 2026-10-05).
- Owner: protocol-compat
- Severity: medium
- Target milestone: R8
- Implemented: 234 of Redis 7.2.5's 242 top-level commands are registered (`compat/commands.generated.md`).
- Missing: FAILOVER, LASTSAVE, LOLWUT, PFDEBUG, PFSELFTEST, RESTORE-ASKING, SENTINEL and WAITAOF are not registered, and the scoreboard counts only registered commands, so it cannot show them. WAITAOF is how a Redis 7.2 application waits until a write is fsynced; FAILOVER is the coordinated switchover without lost writes; backup scripts use LASTSAVE. `COMMAND DOCS` returns an empty map for every command (`src/command.cpp:4766`).
- Tracking: implement or refuse each command (SENTINEL stays out of scope), and fill COMMAND DOCS from the generated command metadata.

69. `bind` accepts one IPv4 address only
- Status: open defect. Evidence: reproduced.
- Owner: ops-compat
- Severity: medium
- Target milestone: R8
- Implemented: one IPv4 bind address, plus an optional Unix socket.
- Missing: the listener is `AF_INET` only and takes one address (`src/server.cpp:185`, `:197-200`), so an IPv6 address fails at startup with "invalid bind address", and a list of addresses stops startup with a config error that names this item (item 24). The stock Redis 7.2.5 `redis.conf` (`bind 127.0.0.1 -::1`) therefore does not start PeaDB, and networks that need IPv6 cannot use it. The cluster bus is IPv4 only too.
- Tracking: parse the bind list (including the `-` prefix for optional addresses) and open one socket per address and family.

70. No `-LOADING` reply during the startup load
- Status: open defect. Evidence: seen in code (checked again 2026-10-05); the load time at scale is not measured.
- Owner: ops-compat
- Severity: medium
- Target milestone: R8
- Implemented: INFO reports `loading:1` while a replica applies a full-sync RDB (item 8).
- Missing: at startup the RDB and AOF load before the listener opens (`src/main.cpp:186-229`), so clients get "connection refused" instead of `-LOADING`. Health checks and supervisors that accept LOADING from Redis may restart a PeaDB node in the middle of a long load, again and again. Redis opens its listeners first and answers `-LOADING` during the load.
- Tracking: open the listeners first and serve LOADING and INFO while loading.

71. Random commands repeat the same sequence
- Status: open defect. Evidence: reproduced by one reviewer; seen in code (checked again 2026-10-05).
- Owner: datatypes-compat
- Severity: medium
- Target milestone: R8
- Implemented: the replies have the right shape and size.
- Missing: SPOP always takes the first element in hash order (`src/datastore.cpp:1107`), and ZRANDMEMBER, SRANDMEMBER and HRANDFIELD use `std::rand()` with no seed (for example `src/command.cpp:9173`, `:9202`, `:9383`, `:9405`), so the same data gives the same "random" picks on every rebuild and restart. Applications that use these commands for sampling, load spreading or picking winners get predictable results.
- Tracking: use one seeded random generator for all of them.
