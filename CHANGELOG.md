# Changelog

All notable changes to PeaDB are documented here. PeaDB targets drop-in compatibility with
Redis 7.2.5; see [`compat/delta.md`](compat/delta.md) for the authoritative per-feature gap
registry and [`docs/compatibility.md`](docs/compatibility.md) for the drop-in posture.

The format follows [Keep a Changelog](https://keepachangelog.com/); versioning is
[SemVer](https://semver.org/). Until 1.0.0 the API/on-disk formats may still change.

## [Unreleased]

### Added — parity roadmap Phase 1 (wave 3): rehash-safe SCAN cursors

- **`unit/scan` adopted natively at 0 errors** (11 suites total, 98.0% pass-rate).
- **Rehash-safe `HSCAN`/`SSCAN`/`ZSCAN` cursors.** Replaced the ordinal (index) cursor —
  which silently skipped elements when the collection was mutated mid-scan (Redis
  issue #4906) — with the same reverse-binary / monotonic bucket cursor the main-keyspace
  `SCAN` uses, so every element present throughout a scan is guaranteed to be returned.
- **`HSCAN NOVALUES`** (fields-only) and **`MATCH`** on `HSCAN`/`SSCAN`/`ZSCAN`.
- **Subnormal float parsing.** `parse_f64` now uses `strtold` instead of `std::stold`,
  which threw `out_of_range` on subnormal underflow (e.g. `9.88e-323`); such values are
  valid in Redis (`ZADD`, `INCRBYFLOAT`, score bounds).

### Added — parity roadmap Phase 1 (wave 2): list + zset suites adopted

- **`unit/type/zset` (320 ok) and `unit/type/list` (264 ok) adopted natively at 0 errors**
  — all four data-type suites now green (10 suites total). Major command work:
  - **Unified `ZRANGE`** (REV/BYSCORE/BYLEX/LIMIT/WITHSCORES via a shared selector reused
    by `ZRANGESTORE`); strict option validation on `ZREVRANGE`/`ZRANGEBYSCORE`.
  - **New commands**: `BLMPOP`, `BZMPOP`, `ZLEXCOUNT`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`,
    `ZREMRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZREMRANGEBYLEX`, `ZINTERCARD`, `HSTRLEN`,
    `CLIENT UNBLOCK`.
  - **`ZUNION`/`ZINTER`/`ZDIFF`(+`STORE`)** rewritten as one engine with `WEIGHTS` +
    `AGGREGATE SUM|MIN|MAX`, plain-set sources (implicit score 1), and NaN→0 collapse.
  - **`ZADD`** flag-incompatibility errors (NX+XX, GT/LT+NX, GT+LT), NaN-score rejection,
    XX-never-creates, INCR-suppressed→nil; **`ZINCRBY`** NaN-result error without mutation.
  - **`ZRANK`/`ZREVRANK` `WITHSCORE`** + correct nil-array-for-missing; **`ZMPOP`/`ZPOPMIN`/
    `ZPOPMAX`** nested-pair replies with RESP3 doubles + negative-count errors.
  - **`ZRANDMEMBER`/`HRANDFIELD`/`SRANDMEMBER`**: distinct positive counts, RESP3 pair
    shape, `-INT64_MIN`/WITHSCORES-overflow guards.
  - **Lists**: `LPOP`/`RPOP` RESP3 nil shapes, `LPOS` RANK bounds/wording, `LMPOP`
    double-`COUNT` rejection, `BLMOVE`/`BRPOPLPUSH` block-regardless-of-dst-type +
    wrong-dst handling, negative `list-max-listpack-size` size-classes (0→1, clamp <-5).
  - Blocking infra: **millisecond-precise timeouts** (poll clamps to the nearest
    deadline; huge timeouts rejected instead of blocking forever), **FIFO wakeup order**,
    correct nil-array vs nil-bulk timeout replies, and deterministic effect propagation
    for all blocking pops.

### Added — parity roadmap Phase 1 (wave 1): encoding shim + hash/set suites adopted

- **Encoding-transition thresholds.** All eight `*-max-listpack-*` / `set-max-intset-entries`
  / `list-max-(ziplist|listpack)-size` params (config file, `CONFIG GET/SET`, ziplist
  aliases) drive `OBJECT ENCODING`, with Redis-style one-way sticky conversions (per-entry
  flags; intset disqualification detected at `SADD` time). `embstr` boundary corrected to 44.
- **`unit/type/hash` + `unit/type/set` adopted natively at 0 errors** (70 + 118 ok; 8 suites
  total now green — 786 ok, 97.5% pass-rate). Fixes pulled in by the suites:
  - **Plain `SET`/`SETEX`/`PSETEX` now overwrite keys of any type** (was WRONGTYPE — a
    fundamental drop-in bug); `SETNX` returns 0 without a type error; `SET ... GET` still
    type-checks.
  - `HRANDFIELD`/`SRANDMEMBER`: positive counts return **distinct** elements, RESP3
    `WITHVALUES` returns pairs, `-INT64_MIN` counts rejected (crashed before).
  - `HSTRLEN` implemented; `HINCRBY`/`HINCRBYFLOAT` strict value parsing + overflow/NaN
    errors with Redis wording; `INCRBYFLOAT`/`HINCRBYFLOAT` results use Redis's fixed
    `%.17Lf`-and-trim formatting (never scientific notation).
  - `SPOP <key> <count>` that empties the set propagates `DEL`/`UNLINK`
    (`lazyfree-lazy-server-del` knob added); single-member SPOP keeps SREM.
  - `SINTERCARD` error wording (`Number of keys can't be greater than number of args`,
    `LIMIT can't be negative`).

### Added — parity roadmap Phase 0: the verification ratchet + native upstream suite

- **Native-mode upstream TCL harness.** The official Redis 7.2.5 test harness now spawns
  `peadb-server` as a redis-server stand-in (`SUITE_MODE=native scripts/redis/run_redis_tests.sh`,
  one-line `harness-native.patch`): positional config-file argv, `unixsocket` listener,
  Redis startup-log contract (` PID:` / `Server initialized` / `Ready to accept`), logger on
  stdout. Six suites adopted at **0 errors** (keyspace, type/string, type/incr, expire, multi,
  scripting — 598 ok, 17 tracked skips, 97.2% pass-rate).
- **The parity ratchet.** `compat/redis_suite_ledger.json` (adopted suites + baselines),
  `scripts/redis/parse_tcl_results.py` (pass-rate report), `scripts/ci/check_suite_ratchet.py`
  (adopted suites must stay at 0 err; skipfile growth or count regressions require a
  `compat/delta.md` change). Skipfiles under `scripts/redis/skipfiles/` are tracked debts.
- **Proof-based scoreboard.** `compat/commands.generated.md` statuses now derive from real
  signals (green upstream-suite coverage via the `PEADB_COMMAND_COVERAGE_FILE` hook +
  differential corpus): `full`/`upstream`/`diff`/`tested`/`registered` replaces the old
  mention-counting `full`/`partial`/`stub`.
- **Bundled Lua 5.1.5** (`third_party/lua-5.1.5`) on every platform — the interpreter Redis
  7.2.5 embeds. Fixes cjson float formatting and cmsgpack table-order byte-parity with Redis;
  removes the system-Lua dependency and the macOS system-Lua miscompile caveat.
- **Scripting parity.** `redis.replicate_commands()`, `redis.set_repl()` + `REPL_*` constants
  (honored by both replica and AOF propagation), `SCRIPT DEBUG yes|sync|no` accepted,
  HELLO/AUTH/RESET/QUIT allowed during BUSY scripts.
- **Command/option gaps closed.** `LPOP`/`RPOP` `COUNT`; `READONLY`/`READWRITE`; RESP2
  subscribe-mode command gate enforced (incl. array-form `PING` while subscribed); keyspace
  `new` ('n') events; dynamic `HELLO` reply; `OBJECT IDLETIME`/`FREQ`/`HELP` + shared-integer
  `REFCOUNT`; strict `INCR`-family value parsing + overflow errors (`increment or decrement
  would overflow`, `decrement would overflow`); strict `INCRBYFLOAT` (`string2ld` rules, NaN/Inf
  rejection); `SETBIT`/`GETBIT`/`SETRANGE`/`APPEND` proto-max-bulk-len (512 MB) bounds;
  Redis-exact `numkeys` error wording; two-arg `AUTH` returns `WRONGPASS` for unknown users;
  `tcp-backlog` config knob.
- **DEBUG verbs for the harness.** `DIGEST` (order-independent dataset digest), `RELOAD`
  (RDB save+flush+load), real `LOADAOF` (fsync+flush+replay), `POPULATE`, real `OBJECT`
  fields (raw-dict peek: expired-but-unreaped keys still report), `SET-SKIP-CHECKSUM-VALIDATION`,
  `QUICKLIST-PACKED-THRESHOLD`, `PAUSE-CRON`, `JMAP`.
- **AOF/replication propagation correctness.** AOF appends are now canonicalized like the
  replication stream (shared rewriter): relative expirations → `PEXPIREAT`, `SPOP` → `SREM`,
  `XADD *` → the concrete generated id (fixes nondeterministic stream ids on replay/replicas),
  `INCRBYFLOAT` → `SET`; AOF now emits `SELECT` on db change (multi-db AOF replay was broken);
  `INCR` preserves TTL.

### Added — production-readiness pass
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
