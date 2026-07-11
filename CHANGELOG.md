# Changelog

All notable changes to PeaDB are documented here. PeaDB targets drop-in compatibility with
Redis 7.2.5; see [`compat/delta.md`](compat/delta.md) for the authoritative per-feature gap
registry and [`docs/compatibility.md`](docs/compatibility.md) for the drop-in posture.

The format follows [Keep a Changelog](https://keepachangelog.com/); versioning is
[SemVer](https://semver.org/). Until 1.0.0 the API/on-disk formats may still change.

## [Unreleased]

### Added — parity roadmap Phase 1 (wave 19): unit/info-command adopted — multi-section INFO

- **`unit/info-command` adopted natively at 0 errors** (33 suites total), which required bringing
  `INFO`'s section selection to Redis 7.0+ semantics:
  - **Multiple section arguments** — `INFO cpu sentinel`, `INFO cpu all`, `INFO cpu default`
    now union the requested sections (each emitted once), instead of honoring only the first arg.
  - **`default` vs `all`/`everything`** — the default set (no args or `default`) now **excludes**
    `commandstats` and `latencystats` (matching Redis); they appear only under `all`/`everything`
    or when named explicitly. Section names are matched case-insensitively (`INFO commandSTATS`).

### Added — parity roadmap Phase 1 (wave 18): FUNCTION / FCALL Lua function libraries + unit/functions adopted

- **`unit/functions` adopted natively at 0 errors** (32 suites total; 109 ok / 1 skip), which
  required building out the full Redis function-library engine on top of the bundled Lua 5.1.5:
  - **`FUNCTION LOAD [REPLACE]`** — shebang metadata parser (`#!lua name=<lib>`), exact Redis
    error strings for every malformed-metadata case, a restricted load environment whose
    `redis` table exposes `register_function`/`log`/`REDIS_VERSION[_NUM]`, C-level global
    protection (accessing/modifying anything else raises "attempted to access nonexistent
    global variable"/"Attempt to modify a readonly table"), and `register_function` in both
    positional and named-argument forms with the full flag matrix (`no-writes`, `allow-oom`,
    `allow-stale`, `no-cluster`, `allow-cross-slot-keys`).
  - **`FCALL` / `FCALL_RO`** — numkeys validation, no-writes/read-only enforcement, allow-oom
    gating, and `COMMAND GETKEYS`/`GETKEYSANDFLAGS` key extraction (shared with the EVAL family).
  - **`FUNCTION DELETE/FLUSH [SYNC|ASYNC]/LIST [LIBRARYNAME <pat>] [WITHCODE]/STATS/DUMP/
    RESTORE [FLUSH|APPEND|REPLACE]/KILL/HELP`** — with RESP3-aware map/set replies, a
    self-checksummed DUMP/RESTORE payload, and atomic RESTORE (a bad/colliding payload never
    partially applies).
  - **Persistence + replication** — libraries round-trip through RDB (real Redis opcode 245 /
    FUNCTION2, so `DEBUG RELOAD` and `SAVE`/full-sync preserve them), `DEBUG RELOAD
    NOSAVE|NOFLUSH|MERGE` flag semantics, and `FUNCTION LOAD/DELETE/FLUSH/RESTORE` + function
    effects replicate to replicas; a no-writes `FCALL` runs on a read-only replica while a
    write `FCALL`/`FUNCTION` mutation is rejected with `READONLY`.
  - **BUSY / kill** — `SCRIPT KILL` and `FUNCTION KILL` act only on their own kind (the
    mismatched killer is rejected with `BUSY`); `FUNCTION STATS` reports the running script;
    a trivially-infinite function is advertised BUSY cooperatively (mirroring EVAL) since the
    single-threaded loop cannot be preempted.
  - Deferred: `FUNCTION - allow stale` (per-inner-command stale-flag enforcement on a stale
    replica) is skipfiled — the same replica-mode-scripting gap tracked for EVAL in
    `compat/delta.md` item 14.
  - Hardening (adversarial-review findings, not suite-covered): a client that triggers the
    cooperative BUSY simulation (an infinite-loop script/function) and then disconnects without
    issuing SCRIPT/FUNCTION KILL no longer wedges the whole server BUSY — `on_client_disconnect`
    now releases the BUSY episode it owned (also nulling the otherwise-dangling busy-session
    pointer). A replica full-sync now flushes function libraries before loading the master's RDB,
    so a node that had its own libraries before `REPLICAOF` no longer diverges from the master.

### Added — parity roadmap Phase 1 (wave 17): limits + violations suites adopted

- **`unit/limits` and `unit/violations` adopted natively at 0 errors** (31 suites total) — both were
  already conformant (maxclients connection-limit behavior and large-value/edge boundary handling);
  now ratcheted.

### Added — parity roadmap Phase 1 (wave 16): latency-monitor + type/list-2 suites adopted

- **`unit/latency-monitor` adopted natively at 0 errors** (29 suites total), which required
  building out `LATENCY`:
  - **`LATENCY HISTOGRAM [command ...]`** — per-command latency histograms (log2 `histogram_usec`
    buckets + call counts), keyed like Redis's commandstats including the `parent|sub` form
    (`config|resetstat`, `client|id`); filtering by a plain name expands to all its subcommands,
    by a full `parent|sub` returns just one, unknown/never-called names are skipped, and it resets
    with `CONFIG RESETSTAT`.
  - **`LATENCY HISTORY <event>`** now returns the full sample series (previously only the latest),
    **`LATENCY GRAPH <event>`** outputs the `high/low ms` summary, `expire-cycle` latency events
    are recorded from the active-expiry cron, and `LATENCY HELP` rejects extra arguments.
- **`unit/type/list-2` adopted** (large-list conversion coverage; already conformant, now ratcheted).

### Added — parity roadmap Phase 1 (wave 15): slowlog suite adopted (entry parity + redaction)

- **`unit/slowlog` adopted natively at 0 errors, no skips** (27 suites total). The slow log was
  brought to Redis parity:
  - **Entry fields**: each `SLOWLOG GET` entry now carries the real client address and client
    name (fields 4 and 5), captured from the session.
  - **Argument trimming**: argv is capped at 31 elements + a `... (N more arguments)` sentinel
    (was 32+sentinel), and long arguments are truncated to 128 bytes + `... (N more bytes)`
    (previously reported the full length with the wrong wording).
  - **Sensitive-argument redaction**: `ACL SETUSER`/`GETUSER` args, `CONFIG SET` values for
    `requirepass`/`masterauth`/`masteruser`, `MIGRATE … AUTH`/`AUTH2` credentials, and `AUTH`/
    `HELLO … AUTH` passwords are logged as `(redacted)`.
  - **Blocking commands** are logged only when they unblock (served or timed out), with the full
    block span and their original argv — not at block time and not as the rewritten pop form.
  - **`EXEC` is no longer logged** (its queued commands are logged individually); `SLOWLOG GET`
    rejects a count `< -1` with `ERR count should be greater than or equal to -1`.
- **`MIGRATE … AUTH <pass>` / `AUTH2 <user> <pass>`** are now parsed and sent to the target
  (previously rejected with `Unsupported MIGRATE option`); single-key MIGRATE auth works (delta 19).

### Fixed — RDB/DUMP listpack encoding corrupted collection elements ≥ 64 bytes (data loss)

- **Any list/hash/set/zset element of 64 bytes or more was silently corrupted across an RDB
  save/load (`SAVE`/`BGSAVE`/`DEBUG RELOAD`), `DUMP`/`RESTORE`, and replication full-sync.** The
  listpack writer used a home-grown string header whose `0x40–0x7F` prefix (used for lengths
  64–16383) collided with the reader's 6-bit-length branch (`enc & 0x80 == 0`), so a ≥64-byte
  string was decoded as a zero-length string and the rest of the container desynced (elements
  truncated/scrambled). Rewrote the listpack writer and reader to the **real Redis listpack
  encoding** (6-bit / 12-bit / 32-bit string headers, 7-bit / 13-bit / 16/24/32/64-bit int
  headers, size-derived backlen skip), so all collection types now round-trip losslessly at
  every element size — and PeaDB's listpack-encoded RDB payloads are genuinely Redis-loadable.

### Added — parity roadmap Phase 1 (wave 14): other suite adopted (HELP/RESET/CONFIG parity)

- **`unit/other` adopted natively at 0 errors** (26 suites total). Fixes:
  - **`HELP` subcommands** added for `PUBSUB`, `COMMAND`, `CONFIG`, `MODULE`, `CLIENT`, and
    `FUNCTION` (each returns the Redis `<CMD> <subcommand> …. Subcommands are:` array).
  - **`RESET`** now exits pub/sub and MONITOR mode (previously left the client subscribed /
    monitoring), and is executed immediately inside `MULTI` instead of being queued (so a
    subsequent `EXEC` correctly reports `EXEC without MULTI`).
  - **`CONFIG <unknown>`** returns Redis's lowercase `unknown subcommand or wrong number of
    arguments for '…'. Try CONFIG HELP.` instead of a generic arity error.
  - **`INFO` `rdb_changes_since_last_save`** now reports the delta since the last save (Redis's
    `server.dirty`), dropping to 0 after a save, rather than the absolute mutation count.
- 5 tests are skipfile-tracked: `RESET clears client state` (needs CLIENT TRACKING, Phase 5),
  `RESET clears authenticated state` (needs multi-user ACL, Phase 5), `Don't rehash if redis has
  child process` (needs Redis dict internals + `DEBUG HTSTATS`), `Process title set as expected`
  (needs `setproctitle`), and `FLUSHALL should reset the dirty counter to 0 …` (dirty-counter
  granularity, delta 18).

### Added — parity roadmap Phase 1 (wave 13): printver + type/list-3 suites adopted

- **`unit/type/list-3` adopted natively at 0 errors, no skips** (11 tests — large-list / quicklist
  fuzz and conversion coverage; already conformant, now ratcheted).
- **`unit/printver` adopted** — INFO now reports the Redis-standard `redis_git_sha1:00000000` and
  `redis_git_dirty:0` fields (previously PeaDB emitted `peadb_git_sha1`/`peadb_git_dirty`, which
  broke drop-in tooling such as redis_exporter and the version-probe harness).

### Added — parity roadmap Phase 1 (wave 12): protocol suite adopted (RESP parser alignment)

- **`unit/protocol` adopted natively at 0 errors, no skips** (23 suites total). The RESP request
  parser and `DEBUG PROTOCOL` were aligned to Redis 7.2.5's exact wire semantics:
  - **Framing errors now match Redis verbatim and close the connection**: `expected '$', got 'X'`
    for a mis-framed bulk header, `invalid multibulk length` (non-numeric or `> INT_MAX` count),
    `invalid bulk length` (non-numeric / negative / over `proto-max-bulk-len`), and
    `unbalanced quotes in request` (a closing quote must be followed by whitespace, so
    `"""x"""` is rejected). Previously these produced a generic `Protocol error` or, worse,
    silently stalled the connection.
  - **No-ops match Redis**: an empty inline line and a non-positive multibulk count (`*-10`,
    `*0`) are consumed silently with no reply; any non-`*` first byte is treated as an inline
    command (a bare `\r\n`/`\n` line no longer hangs the connection).
  - **Desync protection**: a single unterminated line (inline, multibulk count, or bulk length)
    exceeding 64 KB (`PROTO_INLINE_MAX_SIZE`) is rejected with `too big inline/mbulk count/bulk
    count string` and the connection closed, so a garbage flood can't grow the buffer unbounded.
  - **`DEBUG PROTOCOL` is now RESP2/RESP3 aware** (mirroring Redis's `addReply*` downgrades):
    `bignum`/`verbatim`/`double`/`null` fall back to bulk strings in RESP2, `true`/`false` to
    `:1`/`:0`, `map`/`set` to flat arrays, and the `attrib` block is emitted only in RESP3 with
    the correct `key-popularity → [key:123, 90]` shape; added `string`/`integer`/`array`/`push`
    types and the exact unknown-type error.

### Fixed — unit/auth and unit/pubsub now complete without hangs (full adoption)

Both suites were previously recorded green only up to the first test that hung the harness; the
hangs masked real gaps. All are now fixed and the suites run to completion (auth 6→10 ok,
pubsub 28→34 ok):

- **Unauthenticated protocol limits**: when a password is required and the client has not
  authenticated, a multibulk header of >10 elements or a bulk length of >16 KB is rejected at
  the protocol-parse layer with `Protocol error: unauthenticated multibulk/bulk length` and the
  connection is closed — before any buffering — matching Redis's pre-auth memory-safety valve
  (`processMultibulkBuffer`). Previously PeaDB waited for the promised elements, hanging the client.
- **Connect-time authentication state** (`clientSetDefaultAuth`): a client is now marked
  authenticated at connect time iff no password is set, captured once rather than recomputed per
  command. A later `CONFIG SET requirepass` no longer retroactively de-authenticates connections
  that joined while the server was open; `RESET` likewise re-derives auth state from the current
  password instead of always clearing it.
- **Replica AUTH handshake**: the replica now tolerates `-NOAUTH`/`-NOPERM` replies to its
  pre-AUTH `PING` (the master is alive → proceed to AUTH, per `syncWithMaster`) and logs the
  Redis-exact `Unable to AUTH to MASTER: <err>` on a rejected `masterauth`. Fixes replication
  against a password-protected master (including binary/NUL passwords).
- **Background eviction** (`startEvictionTimeProc`): lowering `maxmemory` below current usage now
  evicts in the event-loop cron, firing `evicted` keyspace events and `DEL` propagation without
  waiting for the next write command.
- **Self-published message ordering**: a client that publishes (or triggers a keyspace event) on
  a channel it is itself subscribed to now receives the message *after* the triggering command's
  reply — including after a whole `EXEC` array or script reply (RESP3), matching Redis's
  reply-then-push ordering. Previously the push frame jumped ahead of the reply, desyncing RESP3
  clients.

### Added — parity roadmap Phase 1 (wave 11): geo suite adopted (geohash rewrite)

- **`unit/geo` adopted natively at 0 errors** (22 suites total). The GEO command family was
  reworked to Redis's exact 52-bit interleaved-geohash contract via a shared `geoutil` namespace:
  - **Bit-exact scoring/decoding**: `GEOADD` encodes the score with the Redis latitude range
    (±85.05112878) and step-26 interleave; `GEOPOS`/`GEODIST`/`GEOSEARCH` decode to the cell
    center and measure haversine distance with Redis's constants (earth radius 6372797.560856 m).
  - **`GEOHASH`** returns the standard 11-char base32 hash, forcing the final char to `0` (the low
    3 geohash bits are zero-padding, matching `geohashCommand`).
  - **Edge-case parity**: `GEOADD` rejects out-of-range coordinates and folds `XX`+`NX` into the
    generic `syntax error` (as Redis does); a wrong-type source key returns `WRONGTYPE`; a missing
    source key on `GEORADIUSBYMEMBER`/`GEOSEARCH FROMMEMBER` yields an empty reply (`0` for `STORE`)
    while a present-but-absent member is the `could not decode requested zset member` error — the
    member center is resolved only after the type check, exactly as Redis orders it.
  - **`GEOHASH`/`GEOPOS`** arity relaxed to `-2` (key-only invocation returns an empty array).
- 1 test is skipfile-tracked: unsorted `GEORADIUS` result ordering follows geohash-score order
  rather than Redis's geohash-neighbor cell-scan order (the result set is identical; Redis
  documents unsorted order as unspecified) — delta 20.

### Added — parity roadmap Phase 1 (wave 10): sort suite adopted (full SORT rewrite)

- **`unit/sort` adopted natively at 0 errors, with no skips** (21 suites total). `SORT`/`SORT_RO`
  were rewritten to match Redis's full contract:
  - **`BY`/`GET` hash-field patterns** (`key_*->field`) in addition to key patterns; `GET #`.
  - **"Don't sort"** when the `BY` pattern has no `*` — retains native list/sorted-set order
    (score order for zsets), reversed for `DESC`, with `LIMIT` applied.
  - **Numeric validation**: every score must parse fully as a double or the command errors
    (`One or more scores can't be converted into double`); a missing `BY` key scores 0.
  - **Deterministic ordering**: equal numeric scores break ties lexicographically by element;
    sets are alpha-sorted under `STORE`/scripts.
  - **`SORT_RO`** rejects `STORE` as a syntax error; **`COMMAND GETKEYS`** now extracts the
    (last) `STORE` destination key; and the `LIMIT` offset/count arithmetic is overflow-safe
    (fixes a UBSan signed-overflow).

### Added — parity roadmap Phase 1 (wave 9): dump suite adopted

- **`unit/dump` adopted natively at 0 errors** (20 suites total). `RESTORE` now matches Redis:
  the `BUSYKEY` "target key exists" check runs before payload validation; an already-expired
  absolute TTL (`ABSTTL` in the past) is not stored and, when `REPLACE` removed an existing key,
  propagates as `DEL`/`UNLINK` (per `lazyfree-lazy-server-del`); `IDLETIME`/`FREQ` seed the key's
  LRU idle time / LFU counter (and `RESTORE` is exempt from the dispatch-time access-touch that
  would otherwise reset them); invalid `IDLETIME`/`FREQ` values are rejected.
- 9 tests are skipfile-tracked: advanced `MIGRATE` (multi-key `KEYS`, `AUTH`, and target
  connection caching) — single-key `MIGRATE`/`COPY`/`REPLACE` with TTL, large values, hashes,
  and timeouts all work (delta 19).

### Added — parity roadmap Phase 1 (wave 8): bitfield suite adopted

- **`unit/bitfield` adopted natively at 0 errors** (19 suites total). `BITFIELD` previously
  ignored the `OVERFLOW` clause entirely — `SAT`/`FAIL` behaved like `WRAP` and `INCRBY` did a
  raw signed 64-bit addition (undefined behavior on `INT64_MIN + INT64_MIN`). Now ports Redis's
  signed/unsigned overflow checks: `OVERFLOW SAT` clamps to the type's min/max, `OVERFLOW FAIL`
  returns nil and writes nothing, and the addition is computed in unsigned space so the signed
  add only runs when it cannot overflow. `SET` also applies overflow handling.
- 1 test is skipfile-tracked: `wait_for_ofs_sync` on a master/replica pair (delta 8, the replica
  master-offset-tracking gap).

### Added — parity roadmap Phase 1 (wave 7): pubsub + pubsubshard suites adopted

- **`unit/pubsub` and `unit/pubsubshard` adopted natively at 0 errors** (18 suites total).
  - **`CLIENT REPLY ON/OFF/SKIP`** now actually suppress normal command replies (previously a
    stub that always returned `+OK`), enforced at the `safe_handle_command` chokepoint; pub/sub
    (un)subscribe confirmations bypass suppression, matching Redis.
  - **Keyspace notifications fire only on real mutations**: a no-op `SADD`/`SREM`/`ZREM`/`HDEL`/
    `LREM` (integer reply `0`) publishes nothing, and the stream consumer-group events
    (`xgroup-create`, `xgroup-createconsumer`, `xgroup-delconsumer`) are now emitted — including
    the implicit consumer creation by `XREADGROUP`/`XCLAIM`/`XAUTOCLAIM`.
  - **Sharded pub/sub** (`SSUBSCRIBE`/`SUNSUBSCRIBE`/`SPUBLISH`, previously thin aliases of the
    regular commands) is now a real separate channel namespace with `ssubscribe`/`sunsubscribe`/
    `smessage` reply types, an independent per-client subscription count, `PUBSUB SHARDCHANNELS`/
    `SHARDNUMSUB`, and replication of `SPUBLISH` to replicas.

### Added — parity roadmap Phase 1 (wave 6): quit + auth + bitops suites adopted

- **`unit/quit`, `unit/auth`, and `unit/bitops` adopted natively at 0 errors** (16 suites total).
  - **`AUTH`** error parity: `AUTH <pass>` with no password configured returns Redis's
    "AUTH `<password>` called without any password configured for the default user…" message,
    and `AUTH a b c` (too many args) returns `syntax error` (not a wrong-arity message).
  - **`BITCOUNT`** now rejects a lone start argument (`BITCOUNT key start` with no end) and
    trailing junk as a syntax error instead of silently ignoring it.
- 1 `unit/bitops` test is skipfile-tracked: `rdb_changes_since_last_save` is command-granular
  in PeaDB, so a no-op `SETBIT`/`BITFIELD` still counts as one change (delta 18).

### Added — parity roadmap Phase 1 (wave 5): stream consumer-groups suite adopted

- **`unit/type/stream-cgroups` adopted natively at 0 errors** (13 suites total). This built
  out a real consumer-group model and fixed multiple drop-in gaps:
  - **Consumer model** with per-consumer seen-time/active-time; full `XINFO STREAM … FULL`,
    `XINFO GROUPS`, `XINFO CONSUMERS`; and `XGROUP DESTROY`/`CREATECONSUMER`/`DELCONSUMER`
    (plus `ENTRIESREAD` on `XGROUP CREATE`/`SETID`, and `-`/`+`/`$` id resolution in `SETID`).
  - **`XREADGROUP` rewrite**: honors `COUNT` (previously capped at 1), reads PEL history for a
    non-`>` id (exclusive of the given id, with a nil-fields marker for XDELed entries), and
    creates the consumer eagerly (even a blocking read that never delivers).
  - **`XREADGROUP` effect propagation** to AOF *and* replication: `XGROUP CREATECONSUMER` +
    per-entry `XCLAIM` (ACK) + `XGROUP SETID` (last-id/entries-read) — previously the backlog
    got a partial XCLAIM and the AOF got nothing, so `DEBUG LOADAOF` lost group progress.
  - **`entries-read`/`lag` nil-when-indeterminable** semantics (Redis `SCG_INVALID`), including
    the lazy read-counter recovery on delivery and the tombstone-aware lag estimate.
  - **`XAUTOCLAIM`** now iterates the PEL (surfacing XDELed-but-pending entries in the deleted
    array, with the resume cursor), and validates `COUNT` against Redis's upper bound.
  - **`XACK`/`XDEL` correctness**: `XACK` canonicalizes bare-ms ids and is all-or-nothing on a
    bad id; `XDEL` (and trimming) leave dangling PEL tombstones (bug #5570) instead of purging
    them; `XGROUP CREATE` returns `-BUSYGROUP`/`-NOGROUP` verbatim (no `ERR ` prefix).
  - **Blocking fixes**: a blocked client that disconnects now releases its `blocked_clients`
    slot (was leaking and cascading `wait_for_blocked_clients` timeouts); a blocked
    `XREADGROUP` unblocks with `WRONGTYPE`/`NOGROUP` when its key changes type or its group is
    destroyed/renamed away, and records the failed call in commandstats/errorstats.
- 8 tests are skipfile-tracked: real-Redis listpack stream RDB `RESTORE` (delta 5), replica
  offset-sync (`wait_for_ofs_sync`, delta 8), and consumer-time persistence / blocking-key
  INFO counters (delta 17).

### Added — parity roadmap Phase 1 (wave 4): stream suite adopted

- **`unit/type/stream` adopted natively at 0 errors** (12 suites total), fixing several
  drop-in stream bugs surfaced by the upstream suite:
  - **Trimming no longer advances `max-deleted-entry-id`.** `MAXLEN`/`MINID` trimming
    (in `XADD` and `XTRIM`) reused the same delete path as `XDEL`, which wrongly bumped the
    tombstone marker Redis only touches on an explicit `XDEL` — corrupting later `XSETID`
    monotonicity checks. Trim now removes entries without moving the marker.
  - **`XSETID` validation matches Redis exactly**: rejects an id below the stream's current
    `max-deleted-entry-id`, below a supplied `MAXDELETEDID`, or an `ENTRIESADDED` below the
    stream length, each with Redis's precise error text and check ordering.
  - **`XADD` partial-id overflow** (`ms-*` where the sequence is already at max) now returns
    "equal or smaller", not "exhausted" (the latter is reserved for a fully-maxed last-id).
  - **AOF rewrite round-trips streams** (Redis `rewriteStreamObject` shape): empty streams
    are recreated via the `XADD MAXLEN 0` trick, `XSETID … ENTRIESADDED … MAXDELETEDID`
    restores metadata after XDEL/trim, and consumer groups + PELs are rebuilt via
    `XGROUP CREATE` + `XCLAIM … JUSTID FORCE`. Fixes empty streams vanishing across
    `BGREWRITEAOF`/`DEBUG LOADAOF`.
  - **`XGROUP HELP` and `XINFO HELP`** subcommands (with correct arity-error text).
- 11 approximate-trim tests (`MAXLEN ~` / `MINID ~` / `LIMIT`) are skipfile-tracked: they
  assert Redis's 100-entry listpack-macro-node retained counts, which PeaDB can't reproduce
  without a real listpack backend (compat/delta.md items 5 & 16).

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
