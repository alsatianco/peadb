# PeaDB

A high-performance, Redis 7.2.5-compatible in-memory data store written in modern C++20.

Docker image: `alsatianco/peadb-server` on [Docker Hub](https://hub.docker.com/repository/docker/alsatianco/peadb-server).

PeaDB aims to be **behaviorally indistinguishable** from Redis 7.2.5 — same protocol, same commands, same edge-case semantics — built on cache-friendly data structures and a lean event loop. The command path is single-threaded today (like Redis); a sharded, multi-core architecture is the roadmap target (see [docs/architecture.md](docs/architecture.md)).

## Highlights

- **Full RESP2 / partial RESP3 protocol** — works with existing Redis clients and tools, zero changes required.
- **Broad command coverage** — strings, hashes, lists, sets, sorted sets, streams, HyperLogLog, geo, bitmaps, pub/sub (incl. introspection + sharded aliases), transactions, Lua scripting, and more. (Authoritative, always-current command list, count, and per-command status: [`compat/commands.generated.md`](compat/commands.generated.md).)
- **Persistence** — RDB snapshots and AOF logging with a configurable `appendfsync` policy (`always` / `everysec` / `no`), plus `SAVE` / `BGSAVE` / `BGREWRITEAOF`. RDB files use Redis's format and checksum, so real Redis and PeaDB load each other's files (including streams and pre-7.0 zipmap/ziplist encodings), and a corrupt dump is refused. For remaining persistence and migration limits, read [`compat/delta.md`](compat/delta.md) items 3 and 53 before relying on them.
- **Memory & eviction** — `maxmemory` with all eight `maxmemory-policy` modes (LRU/LFU/random/TTL, approximate sampling), a compact tagged-union value layout, per-class `client-output-buffer-limit` (slow-consumer/lagging-replica protection), and background `UNLINK` / `FLUSH ASYNC` (lazyfree). `used_memory` counts every byte PeaDB allocates, as Redis's does, so `maxmemory` bounds the process; with `noeviction`, only commands that can grow memory get `-OOM` (delta items 38–39). The eviction choice is sampled without Redis's eviction pool (delta item 12).
- **Replication** — native PSYNC2-based replication with multi-replica support, chained replicas, `WAIT` (counts only what replicas have applied), manual replica promotion (the promoted server continues the stream, so Redis replicas resume from it with a partial resync), **automatic reconnect** (exponential backoff) and `masterauth`, configurable `repl-backlog-size`, and master-driven replica expiry (the master propagates `DEL`). Automatic failover/Sentinel is not yet implemented. Full sync forks a disk-backed snapshot and streams it separately from the replica command buffer (delta 46). Reconnects resume cached history with PSYNC; replication timeouts, periodic master PINGs and replica TCP keepalive are active (delta 47). Claims propagate deterministic effects (delta 48).
- **Cluster (experimental, not production-safe)** — 16 384 hash slots with real per-node ownership, Redis 7.2 reply shapes (`CLUSTER SLOTS`/`SHARDS`/`NODES`/`INFO`) and `MOVED`/`ASK` redirection naming each slot's true owner, over an operator-configured topology: no gossip, no automatic failover, no `nodes.conf`. Today cluster mode can write keys on the wrong node (routing checks only the first key; there is no `CROSSSLOT`), and a restarted node serves slots that other nodes own; `redis-cli --cluster` workflows do not work. See [`compat/delta.md`](compat/delta.md) items 6 and 64–67 and [the design record](docs/decisions/p5-cluster-addressing-and-version-provenance.md).
- **Lua scripting** — `EVAL`/`EVALSHA`, `FUNCTION` subsystem, `FCALL`/`FCALL_RO`, with cjson, cmsgpack, bit, and struct libraries. These libraries are PeaDB's own code and some output differs from Redis (for example `cjson.encode` writes integers as `1.0`, delta item 63), and a script whose text contains `while true do` is never run (delta item 54).
- **Redis Modules API** — minimal `RedisModule_*` C API; load `.so` modules at runtime via `MODULE LOAD`.
- **Observability** — populated `SLOWLOG` and `LATENCY` (real per-command timing), keyspace notifications (`notify-keyspace-events`, incl. `expired`; many write commands still send no event or the wrong one, delta item 72), `MONITOR`, `CLIENT LIST`/`KILL`, runtime `CONFIG SET loglevel`, and `INFO` in Redis's format (RSS, fragmentation ratio, fork timing, ops/sec) for `redis_exporter` and dashboards. About 16 standard INFO fields are still missing (delta item 22), and each INFO call walks the whole keyspace (delta item 55).
- **Docker-ready** — published image at `alsatianco/peadb-server`, plus multi-stage Dockerfile and docker-compose for local builds.

## Quick Start

### Prerequisites

- C++20 compiler (GCC 11+ or Clang 14+)
- CMake ≥ 3.16

Lua 5.1.5 is bundled (`third_party/lua-5.1.5`), so no Lua dev package is needed. It is stock Lua 5.1.5, not the patched copy Redis 7.2.5 embeds, so some script results differ from Redis (for example table order for long string keys): see [`compat/delta.md`](compat/delta.md) items 62–63.

On Ubuntu/Debian:

```bash
sudo apt-get install build-essential cmake g++
```

Or use the provided helper:

```bash
scripts/dev/install_deps_ubuntu.sh
```

### Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Run

```bash
./build/peadb-server --port 6379 --bind 127.0.0.1
```

Then connect with any Redis client:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
```

Or use the one-liner:

```bash
scripts/dev/quick_start.sh
```

`quick_start.sh` also does a best-effort raise of the shell `nofile` soft limit to `65536` (override with `PEADB_NOFILE_SOFT`) to keep high-connection tests stable out of the box.

### Docker

Run the published image directly:

```bash
docker run -d --name peadb -p 6379:6379 -v peadb-data:/data alsatianco/peadb-server:latest \
  --port 6379 --bind 0.0.0.0 --dir /data --appendonly yes --requirepass "<your-password>"
```

Arguments after the image name replace the image's default `CMD` (`--port 6379 --bind 0.0.0.0 --dir /data`) completely, so the line above repeats those defaults. Without `--bind 0.0.0.0`, PeaDB listens on `127.0.0.1` inside the container and the published port cannot reach it.

The image turns on neither save points nor AOF, so without `--appendonly yes` (or `--save`) nothing is saved to the `/data` volume unless you run `SAVE` or `BGSAVE` yourself. `docker stop` sends SIGTERM, which shuts PeaDB down like `SHUTDOWN` (with save points it saves first, as Redis does, and a master first waits up to `shutdown-timeout` seconds, default 10, for replicas that are behind); Docker's default grace time is also 10 seconds, so give it more (`docker stop -t`) when the dataset is large or replicas may lag. Read the persistence and replication boundaries in [`compat/delta.md`](compat/delta.md) (items 2/3/8/53) first.

**protected-mode is on by default**: a client reaching the container through the published port is not seen as a loopback connection, so without a password PeaDB refuses it with `-DENIED`. Pass `--requirepass` as above, or, only if your network already isolates the container (for example a private Docker network with no published port), use `--protected-mode no` instead.

See the [Docker Hub tags page](https://hub.docker.com/repository/docker/alsatianco/peadb-server/tags) for the current list of published tags — see [`docs/RELEASING.md`](docs/RELEASING.md) for what each tag means and when `latest` moves.

Or build and run locally with Docker Compose:

```bash
docker compose up -d
```

The container exposes port `6379` (configurable via `PEADB_PORT`) and mounts a named volume at `/data`. Like the image, the compose file turns on neither save points nor AOF, so add `--appendonly yes` to its `command` to keep data in that volume. For high-connection load tests, compose sets `ulimits.nofile` by default (`PEADB_NOFILE_SOFT` / `PEADB_NOFILE_HARD`, both default to `65536`).

## Configuration

PeaDB reads its configuration the way `redis-server` 7.2.5 does: an optional config file as the first argument (`peadb-server /etc/redis/redis.conf`), then command-line options, where `--name value` means the same as a `name value` line and the last value wins (`peadb-server --port 6380 --save ""`). A lone `-` as the first or last argument reads the config from stdin, and PeaDB also accepts `--config <path>` as another way to name the file. Lines are split like Redis splits them (quotes and escapes work, and `#` starts a comment only at the start of a line, so `requirepass pass#word` keeps the `#`). Setting names and their old aliases (`slaveof`, `hash-max-ziplist-entries`, …) are Redis's own, `include` works (also with wildcards), and every value is checked the way Redis checks it.

PeaDB refuses to start, with Redis's error text and the line number, in the cases where Redis's config loader refuses: a config file that cannot be opened, a name Redis does not know, the wrong number of arguments (a `# note` after a value counts as extra arguments), unbalanced quotes, a value Redis rejects, a `logfile` that cannot be opened, `rename-command` with no target, and `rename-command` for a command that does not exist (or no longer exists after an earlier rename) or onto a name that is taken. It also refuses the Redis settings it cannot honour safely: `user` and `aclfile` (PeaDB has no ACL users), `loadmodule`, `masteruser`, `tls-port` other than 0, `tls-replication yes`, `tls-cluster yes`, `enable-debug-command no` or `local`, `bind` with more than one address, and `--sentinel`. These settings are judged on their final value, so a later line or option can undo an earlier one (`--bind 0.0.0.0` after the stock `bind 127.0.0.1 -::1` line starts). Every other Redis setting that PeaDB does not apply yet is accepted, and the log gets one warning for each, naming its last line ([`compat/delta.md`](compat/delta.md) item 24 lists them and the few checks Redis makes that PeaDB does not), unless the value is what PeaDB always does (for example `daemonize no` or `databases 16`).

The settings PeaDB applies (each works as a config-file line and as a `--name` option):

| Setting | Default | Description |
|------|---------|-------------|
| `port` | `6379` | Listening port; `0` opens no TCP listener (then `unixsocket` is needed), as in Redis |
| `bind` | `127.0.0.1` | Bind address; PeaDB listens on one address only (delta item 69) |
| `loglevel` | `notice` | `debug`, `verbose`, `notice`, `warning` or `nothing` |
| `dir` | `.` | Data directory for snapshots and AOF; it must exist. PeaDB makes it the working directory, as Redis does, so later relative paths are inside it |
| `dbfilename` | `dump.rdb` | RDB snapshot filename (a file name, not a path) |
| `maxclients` | `10000` | Maximum concurrent client connections; PeaDB raises the open-files limit to fit them, or lowers this value |
| `appendonly` | `no` | Enable AOF persistence |
| `appendfilename` | `appendonly.aof` | AOF filename (a file name, not a path) |
| `appendfsync` | `everysec` | AOF fsync policy (`always` / `everysec` / `no`) |
| `requirepass` | — | Require clients to `AUTH` with this password (empty = open, like Redis) |
| `replicaof` (`slaveof`) | — | Start as a replica of `host port`; `no one` starts as a master. See [replica persistence and restart](compat/delta.md) item 45 |
| `masterauth` | — | Password a replica sends during each master handshake; empty skips AUTH |
| `masteruser` | — | Master ACL username; empty uses password-only AUTH. Config/CLI/CONFIG SET, effective on reconnect; see [replication authentication](compat/delta.md) item 50 |
| `protected-mode` | `yes` | Refuse non-loopback clients when no password is set |
| `enable-module-command` | `no` | Gate `MODULE LOAD` (`yes` / `no` / `local`) — `MODULE LOAD` runs arbitrary native code |
| `enable-protected-configs` | `no` | Allow `CONFIG SET dir` and `CONFIG SET dbfilename` (`yes`, or `local` for loopback and unix-socket clients) |
| `rename-command` | — | `rename-command FROM TO` renames a command; `rename-command FROM ""` removes it, as in Redis |
| `include` | — | Read another config file at this point; a path with `*`, `?` or `[` is a wildcard, and one that matches nothing is skipped |
| `maxmemory` | `0` (off) | Memory limit (e.g. `100mb`, `2gb`); above it, keys are evicted per `maxmemory-policy`, else commands that can grow memory (Redis's `denyoom` ones: `SET`, `RPUSH`, …) get `-OOM`, while `DEL`, `EXPIRE`, reads and the like still run |
| `maxmemory-eviction-tenacity` | `10` | How long one eviction pass may run before the rest goes on in the background (`10`: 500 µs, `100`: no limit) |
| `maxmemory-policy` | `noeviction` | `noeviction` \| `allkeys-lru` \| `allkeys-lfu` \| `allkeys-random` \| `volatile-lru` \| `volatile-lfu` \| `volatile-random` \| `volatile-ttl` (approximate sampling) |
| `maxmemory-samples` | `5` | Keys sampled per eviction when choosing a victim (higher = more accurate, slower) |
| `lfu-log-factor` | `10` | How slowly the LFU access counter grows (`OBJECT FREQ`); counters never decay (`lfu-decay-time` is always `0`) |
| `timeout` | `0` (off) | Close clients idle longer than N seconds (replicas, blocked, MULTI, and pub/sub clients are exempt) |
| `tcp-keepalive` | `300` | Seconds of silence before the first TCP keepalive probe on a new client connection; `0` turns keepalive off |
| `proto-max-bulk-len` | `512mb` | Largest bulk argument a client may send, and largest string `SETRANGE`/`APPEND`/`SETBIT` may build |
| `client-query-buffer-limit` | `1gb` | A client whose unread input grows past this is disconnected |
| `tcp-backlog` | `511` | `listen(2)` backlog (Redis default) |
| `unixsocket` | — | Also listen on an AF_UNIX socket path |
| `unixsocketperm` | `0` | Octal file mode for the unix socket (e.g. `700`); `0` leaves the mode it was created with |
| `save` | none | Save points, e.g. `save 900 1` (each line adds one; `save ""` clears them; `save 60 0` saves every 60 s); triggers `BGSAVE` + saves on `SHUTDOWN`, SIGTERM and SIGINT (change that with `shutdown-on-sigterm`/`shutdown-on-sigint`). With no `save` line Redis uses `3600 1 300 100 60 10000`, PeaDB uses none (delta item 2) |
| `shutdown-timeout` | `10` | Seconds a master's `SHUTDOWN`, SIGTERM or SIGINT waits for replicas that have not acknowledged every write; writes wait meanwhile, reads are served (`0`: no wait; `SHUTDOWN NOW` does not wait either) |
| `client-output-buffer-limit` | Redis defaults | Per-class (`normal`/`replica`/`pubsub`) output-buffer limits; overflowing clients are disconnected |
| `notify-keyspace-events` | `""` (off) | Keyspace notification classes (Redis flag syntax) |
| `repl-backlog-size` | `1mb` | Replication backlog size for partial resync |
| `repl-timeout` | `60` | Seconds of inactivity before a replica reconnects during connect/handshake/transfer/streaming, or a master disconnects a replica whose ACKs stop |
| `repl-ping-replica-period` (`repl-ping-slave-period`) | `10` | Seconds between top-level master PINGs in the replication stream |
| `min-replicas-to-write` | `0` | Refuse writes unless this many replicas are connected (`min-replicas-max-lag` is not applied) |
| `replica-serve-stale-data` | `yes` | Whether a replica answers reads while its master link is down (with `no`, only the commands Redis allows on a stale replica run) |
| `busy-reply-threshold` (`lua-time-limit`) | `5000` | Milliseconds a script may run before other clients get `-BUSY` |
| `lazyfree-lazy-server-del` | `no` | Propagate server-side deletes as `UNLINK` |
| `stop-writes-on-bgsave-error` | `yes` | Refuse writes and `PING` with `-MISCONF` after a failed background save while save points are set, until a save succeeds (a failed `fork()` counts, as in Redis) |
| `aof-load-truncated` | `yes` | Accept an AOF whose last command is cut short, and cut that command off the file, as Redis does (delta item 30) |
| `slowlog-log-slower-than`, `slowlog-max-len` | `10000`, `128` | `SLOWLOG` threshold (µs) and length |
| `latency-monitor-threshold` | `0` (off) | `LATENCY` monitor threshold (ms) |
| `hash-max-listpack-entries`, `hash-max-listpack-value`, `list-max-listpack-size`, `set-max-intset-entries`, `set-max-listpack-entries`, `set-max-listpack-value`, `zset-max-listpack-entries`, `zset-max-listpack-value` | Redis defaults | Encoding thresholds reported by `OBJECT ENCODING` |
| `cluster-enabled` | `no` | Enable cluster mode; startup-only, like Redis's `IMMUTABLE_CONFIG` — `CONFIG SET cluster-enabled` is refused |
| `cluster-announce-ip` | — | Address this node announces to peers/clients; empty falls back to `bind` — a documented simplification of Redis's live-connection auto-detection (see `docs/decisions/p5-cluster-addressing-and-version-provenance.md`). With `cluster-enabled yes`, the server refuses to start if this falls back to a wildcard (`0.0.0.0`, `::`, `*`, or empty) |
| `cluster-announce-port` | `0` | Port this node announces; `0` falls back to `port` |
| `cluster-announce-bus-port` | `0` | Cluster bus port this node announces; `0` falls back to the announced port + 10000 |

`CONFIG GET` and `CONFIG SET` work like Redis 7.2.5's: `CONFIG GET` knows every Redis setting and alias, and `CONFIG SET` takes several settings at once, all or nothing, with Redis's checks and error text (an unknown name, an immutable setting, a protected one such as `dir`, a bad value). The settings in the table above take effect at once, `appendonly yes` included, except `port` and `bind`, which PeaDB cannot change while it runs yet (delta item 25). For a Redis setting PeaDB does not apply, `CONFIG SET` refuses every value except the one that describes what PeaDB always does (so `rdbcompression yes` is refused); a setting with no such value keeps what it is given, has no effect, and gets a warning in the log (for example `hz`). Delta item 24 lists these settings. Changes are in memory only: `CONFIG REWRITE` is not supported yet (item 25).

## Supported Commands

<details>
<summary>Command list (see <a href="compat/commands.generated.md">commands.generated.md</a> for the authoritative, always-current list)</summary>

**Connection & Server** — `PING`, `ECHO`, `QUIT`, `HELLO`, `AUTH`, `ACL`, `INFO`, `COMMAND`, `CONFIG`, `DEBUG`, `SELECT`, `SAVE`, `BGSAVE`, `LASTSAVE`, `BGREWRITEAOF`, `WAIT`, `TIME`, `DBSIZE`, `FLUSHALL`, `FLUSHDB`, `SHUTDOWN`

**Strings** — `GET`, `SET`, `SETEX`, `PSETEX`, `SETNX`, `MGET`, `MSET`, `MSETNX`, `GETDEL`, `GETSET`, `GETEX`, `APPEND`, `STRLEN`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `SETBIT`, `GETBIT`, `SETRANGE`, `GETRANGE`, `SUBSTR`, `LCS`

**Keys** — `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `KEYS`, `RANDOMKEY`, `SCAN`, `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `DUMP`, `RESTORE`, `RESTORE-ASKING`, `MIGRATE`, `SORT`, `OBJECT`, `SWAPDB`

**Hashes** — `HSET`, `HGET`, `HMGET`, `HMSET`, `HSETNX`, `HDEL`, `HLEN`, `HEXISTS`, `HGETALL`, `HVALS`, `HKEYS`, `HINCRBY`, `HINCRBYFLOAT`, `HSCAN`

**Lists** — `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `BLPOP`, `BRPOP`, `LMOVE`, `BLMOVE`, `BRPOPLPUSH`

**Sets** — `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`, `SPOP`, `SMOVE`, `SINTERCARD`, `SSCAN`

**HyperLogLog** — `PFADD`, `PFCOUNT`, `PFMERGE` (self-consistent dense form; see `compat/delta.md`)

**Sorted Sets** — `ZADD` (NX/XX/GT/LT/INCR), `ZRANGE`, `ZSCAN`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZPOPMIN`, `BZPOPMAX`

**Streams** — `XADD`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`, `XGROUP`, `XREADGROUP`, `XREAD` (blocking), `XACK`, `XPENDING`, `XINFO`, `XSETID`, `XTRIM`, `XCLAIM`, `XAUTOCLAIM` — with a full PEL (delivery time/count), deterministic claim effects through replication and AOF replay/rewrite ([delta 48](compat/delta.md)), and **RDB persistence + replication** of streams

**Pub/Sub** — `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` (CHANNELS/NUMSUB/NUMPAT), `SSUBSCRIBE`/`SUNSUBSCRIBE`/`SPUBLISH` (sharded aliases)

**Transactions** — `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

**Scripting** — `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT` (LOAD/EXISTS/FLUSH/KILL), `FUNCTION` (LOAD/LIST/DELETE/FLUSH/STATS/KILL), `FCALL`, `FCALL_RO`

**Replication** — `SYNC`, `PSYNC`, `REPLCONF`, `REPLICAOF`, `SLAVEOF`, `WAIT`

**Cluster** — `CLUSTER` (INFO/MYID/NODES/SLOTS/SHARDS/KEYSLOT/MEET/ADDSLOTS/DELSLOTS/ADDSLOTSRANGE/DELSLOTSRANGE/SETSLOT/COUNTKEYSINSLOT/GETKEYSINSLOT/HELP, plus `MEETBACK`, a PeaDB-only extension not in Redis), `ASKING`

**Modules** — `MODULE` (LOAD/LIST/UNLOAD)

</details>

## Testing

PeaDB has a comprehensive suite spanning unit, integration, fuzz, and differential testing.

Parity against real Redis is measured by the **upstream Redis 7.2.5 TCL suite running in native mode** (the official harness spawns `peadb-server` as a redis-server stand-in). Adopted suites, per-suite baselines, and the monotonic no-regression ratchet live in [`compat/redis_suite_ledger.json`](compat/redis_suite_ledger.json); every skipped upstream test is a tracked debt in `scripts/redis/skipfiles/` tied to a [`compat/delta.md`](compat/delta.md) entry.

```bash
# Run all tests (11 stages: unit, integration, coverage, portcheck, compat, fuzz, stability, perf, sentinel, redis, diff)
scripts/run_all_tests.sh

# Upstream Redis TCL suite, native mode + pass-rate report
SUITE_MODE=native scripts/redis/run_redis_tests.sh
python3 scripts/redis/parse_tcl_results.py artifacts/redis-native

# Unit tests only (via CTest)
cd build && ctest --output-on-failure

# Integration tests only
scripts/ci/run_integration_tests.sh

# Fuzz / stability checks (ASAN/UBSAN)
scripts/qa/run_fuzz_sanity.sh
scripts/qa/run_stability_checks.sh

# Differential tests (compare output against a real Redis instance)
python3 tests/diff/run_diff_tests.py

# Upstream Redis TCL test suite
scripts/redis/run_redis_tests.sh
```

Integration tests are organized by milestone (m0–m9), polish stages (p0–p3), and hardening batches (b*), covering 100+ Python test files across all data types, persistence modes, replication, scripting, modules, and edge cases.

### ASAN Build

```bash
cmake -S . -B build-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
cmake --build build-asan -j$(nproc)
```

This build replaces the repo-root `peadb-server` with the sanitised binary, on purpose. Every integration test resolves the server as `ROOT / "peadb-server"`, so this is how `scripts/ci/run_integration_tests.sh` ends up running against the sanitiser instead of a plain Release build. The nightly CI "Integration smoke (ASAN)" step relies on the same swap; it runs the tests from `build-asan/`. If you would rather keep the sanitised binary out of the way in `build-asan/` instead, add `-DPEADB_SERVER_OUTPUT_DIR=build-asan` to the first command above — this is what `scripts/qa/run_stability_checks.sh` does. Either way, rebuild Release afterwards (`cmake --build build`) to put a plain `peadb-server` back at the repo root.

## Benchmarks

Indicative single-connection throughput (representative, not peak):

| Operation | ops/sec |
|-----------|---------|
| SET | 63,694 |
| GET | 59,524 |
| LPUSH | 62,893 |
| LPOP | 61,728 |
| INCR | 63,694 |
| SADD | 63,694 |

Reproduce with `scripts/dev/redis_vs_peadb_benchmark.sh` (compares PeaDB against a Redis baseline). The command path is single-threaded, so throughput is competitive at low concurrency while tail latency rises under very high concurrency — the sharded, multi-core architecture on the [roadmap](docs/architecture.md#design-principles--roadmap) targets this.

## Compatibility, Persistence & Migration

Standard Redis clients connect to PeaDB unmodified. Native RDB snapshots, DUMP/RESTORE and MIGRATE now exchange every supported type with Redis 7.2.5, including stream history, consumers and pending messages ([`compat/delta.md`](compat/delta.md) items 5 and 51). Named ACL master authentication and deterministic stream claim propagation are implemented (items 50 and 48). Release publication requires the authenticated migration and rollback acceptance gate (item 49); the RDB bridge scripts check replies and preserve databases, stream metadata and functions (item 52). Older zipmap/ziplist encodings load natively; files selecting a database outside 0–15 are refused (item 53). Read the [migration guide](docs/compatibility.md#live-migration--rollback) before relying on these paths.

Full detail — drop-in posture, tested clients, `redis.conf` support, migration & rollback steps, the feature-status table, persistence internals, and security/monitoring notes — lives in **[docs/compatibility.md](docs/compatibility.md)**. The authoritative, always-current gap registry is **[compat/delta.md](compat/delta.md)**.

To report a security vulnerability privately, or to read the known security gaps (no TLS, no multi-user ACL, no Sentinel, cluster mode not production-safe) before you deploy, see **[SECURITY.md](SECURITY.md)**.

## Documentation

| Document | What it covers |
|----------|----------------|
| [docs/compatibility.md](docs/compatibility.md) | Drop-in posture, migration & rollback, feature status, persistence & durability, ops notes |
| [docs/architecture.md](docs/architecture.md) | How PeaDB works internally + design principles and roadmap (for contributors) |
| [compat/delta.md](compat/delta.md) | **Canonical** registry of compatibility gaps vs Redis 7.2.5 |
| [compat/commands.generated.md](compat/commands.generated.md) | Auto-generated per-command coverage scoreboard |
| [scripts/README.md](scripts/README.md) | Guide to the helper, CI, QA, and dev scripts |
| [CLAUDE.md](CLAUDE.md) | Build, test, and architecture notes for contributors and AI assistants |
| [CHANGELOG.md](CHANGELOG.md) | Release history and the `[Unreleased]` parity-wave log |
| [SECURITY.md](SECURITY.md) | Supported versions, private vulnerability reporting, known security gaps |
| [docs/RELEASING.md](docs/RELEASING.md) | Release tagging convention and the multi-arch publish sequence |

## Project Structure

```
include/          C++ headers (command, config, datastore, protocol, server, …)
src/              Implementation files + main entry point
tests/
  unit/           C++ unit tests (CTest)
  integration/    Python integration tests (100+ files)
  fuzz/           RESP parser and command dispatcher fuzz tests
  diff/           Differential tests (PeaDB vs Redis)
scripts/
  ci/             CI pipelines and checks
  qa/             Fuzz, stability, perf, and sentinel validation
  dev/            Developer helpers (quick_start, install_deps)
  redis/          Upstream Redis test runner + RDB bridge scripts
compat/           Compatibility delta tracker + generated scoreboard
docs/             Architecture, compatibility, and design-decision records
artifacts/        Benchmark results and performance baselines
```

## License

[MIT](LICENSE) © 2026 alsatianco
