# PeaDB

A high-performance, Redis 7.2.5-compatible in-memory data store written in modern C++20.

Docker image: `alsatianco/peadb-server` on [Docker Hub](https://hub.docker.com/repository/docker/alsatianco/peadb-server).

PeaDB aims to be **behaviorally indistinguishable** from Redis 7.2.5 — same protocol, same commands, same edge-case semantics — built on cache-friendly data structures and a lean event loop. The command path is single-threaded today (like Redis); a sharded, multi-core architecture is the roadmap target (see [docs/architecture.md](docs/architecture.md)).

## Highlights

- **Full RESP2 / partial RESP3 protocol** — works with existing Redis clients and tools, zero changes required.
- **Broad command coverage** — strings, hashes, lists, sets, sorted sets, streams, HyperLogLog, geo, bitmaps, pub/sub (incl. introspection + sharded aliases), transactions, Lua scripting, and more. (Authoritative, always-current command list, count, and per-command status: [`compat/commands.generated.md`](compat/commands.generated.md).)
- **Persistence** — RDB snapshots and AOF logging with a configurable `appendfsync` policy (`always` / `everysec` / `no`), plus `SAVE` / `BGSAVE` / `BGREWRITEAOF`. RDB loads verify their CRC64 and refuse a corrupt dump.
- **Memory & eviction** — enforced `maxmemory` with all eight `maxmemory-policy` modes (LRU/LFU/random/TTL, approximate sampling) so PeaDB works as a cache, a compact tagged-union value layout, per-class `client-output-buffer-limit` (slow-consumer/lagging-replica protection), and background `UNLINK` / `FLUSH ASYNC` (lazyfree).
- **Replication** — native PSYNC2-based replication with multi-replica support, `WAIT`, manual replica promotion, **automatic reconnect** (exponential backoff) and `masterauth`, configurable `repl-backlog-size`, and correct replica expiry (the master propagates `DEL` so clocks can't diverge). Automatic failover/Sentinel is not yet implemented.
- **Cluster** — 16 384 hash-slot routing, `CLUSTER KEYSLOT`/`INFO`/`NODES`/`SLOTS`, `MEET` peer discovery, and `MOVED`/`ASK` redirection (surface-level; full gossip and slot migration in progress).
- **Lua scripting** — `EVAL`/`EVALSHA`, `FUNCTION` subsystem, `FCALL`/`FCALL_RO`, with cjson, cmsgpack, bit, and struct libraries.
- **Redis Modules API** — minimal `RedisModule_*` C API; load `.so` modules at runtime via `MODULE LOAD`.
- **Observability** — populated `SLOWLOG` and `LATENCY` (real per-command timing), keyspace notifications (`notify-keyspace-events`, incl. `expired`), `MONITOR`, `CLIENT LIST`/`KILL`, runtime `CONFIG SET loglevel`, and Redis-compatible `INFO` (RSS, fragmentation ratio, fork timing, ops/sec) for `redis_exporter` and dashboards.
- **Docker-ready** — published image at `alsatianco/peadb-server`, plus multi-stage Dockerfile and docker-compose for local builds.

## Quick Start

### Prerequisites

- C++20 compiler (GCC 11+ or Clang 14+)
- CMake ≥ 3.16

Lua 5.1.5 is bundled (`third_party/lua-5.1.5`) — the exact interpreter Redis 7.2.5 embeds — so no Lua dev package is needed and script semantics match Redis byte-for-byte.

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
docker run -d --name peadb -p 6379:6379 -v peadb-data:/data alsatianco/peadb-server:latest
```

Available public tags: `alsatianco/peadb-server:latest`, `alsatianco/peadb-server:0.1`.

Or build and run locally with Docker Compose:

```bash
docker compose up -d
```

The container exposes port `6379` (configurable via `PEADB_PORT`) and persists data to a named volume. For high-connection load tests, compose sets `ulimits.nofile` by default (`PEADB_NOFILE_SOFT` / `PEADB_NOFILE_HARD`, both default to `65536`).

## Configuration

PeaDB accepts Redis-style configuration via CLI flags or a config file — either redis-server style (`peadb-server /path/peadb.conf`, first positional argument) or `--config <path>` — using redis.conf `key value` syntax. CLI flags override config-file values.

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `6379` | Listening port |
| `--bind` | `127.0.0.1` | Bind address |
| `--loglevel` | `info` | Log level (`error`, `warn`, `info`, `debug`) |
| `--dir` | `.` | Data directory for snapshots and AOF |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |
| `--config` | — | Path to a config file |
| `maxclients` | — | Maximum concurrent client connections |
| `appendonly` | `false` | Enable AOF persistence |
| `appendfilename` | `appendonly.aof` | AOF filename |
| `appendfsync` | `everysec` | AOF fsync policy (`always` / `everysec` / `no`) |
| `--requirepass` | — | Require clients to `AUTH` with this password (empty = open, like Redis) |
| `--masterauth` | — | Password a replica sends to its master during sync |
| `--protected-mode` | `yes` | Refuse non-loopback clients when no password is set |
| `--enable-module-command` | `no` | Gate `MODULE LOAD` (`yes` / `no` / `local`) — `MODULE LOAD` runs arbitrary native code |
| `--maxmemory` | `0` (off) | Memory limit (e.g. `100mb`, `2gb`); above it, keys are evicted per `--maxmemory-policy`, else denyoom writes get `-OOM` |
| `--maxmemory-policy` | `noeviction` | `noeviction` \| `allkeys-lru` \| `allkeys-lfu` \| `allkeys-random` \| `volatile-lru` \| `volatile-lfu` \| `volatile-random` \| `volatile-ttl` (approximate sampling) |
| `--maxmemory-samples` | `5` | Keys sampled per eviction when choosing a victim (higher = more accurate, slower) |
| `--timeout` | `0` (off) | Close clients idle longer than N seconds (replicas, blocked, MULTI, and pub/sub clients are exempt) |
| `--tcp-backlog` | `511` | `listen(2)` backlog (Redis default) |
| `unixsocket` | — | Also listen on an AF_UNIX socket path (config-file key) |
| `--save` | — | Save points, e.g. `"900 1 300 10"` (`""` disables); triggers `BGSAVE` + saves on `SHUTDOWN` |
| `--client-output-buffer-limit` | Redis defaults | Per-class (`normal`/`slave`/`pubsub`) output-buffer limits; overflowing clients are disconnected |
| `notify-keyspace-events` | `""` (off) | Keyspace notification classes (Redis flag syntax; config-file key or `CONFIG SET`) |
| `rename-command` | — | Rename or disable a command (`rename-command FROM TO`, empty `TO` disables; config-file key) |
| `--repl-backlog-size` | `1mb` | Replication backlog size for partial resync |

`appendonly`, `appendfilename`, `appendfsync`, and `save` are also accepted as `--`-prefixed CLI flags (in addition to config-file keys); `maxclients`, `unixsocket`, `notify-keyspace-events`, and `rename-command` are config-file keys only.

Recognized redis.conf keys are a subset of Redis's; unknown directives are accepted but ignored. Runtime configuration is available via `CONFIG GET` / `CONFIG SET` (in-memory only — there is no `CONFIG REWRITE`).

## Supported Commands

<details>
<summary>Command list (see <a href="compat/commands.generated.md">commands.generated.md</a> for the authoritative, always-current list)</summary>

**Connection & Server** — `PING`, `ECHO`, `QUIT`, `HELLO`, `AUTH`, `ACL`, `INFO`, `COMMAND`, `CONFIG`, `DEBUG`, `SELECT`, `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `WAIT`, `TIME`, `DBSIZE`, `FLUSHALL`, `FLUSHDB`, `SHUTDOWN`

**Strings** — `GET`, `SET`, `SETEX`, `PSETEX`, `SETNX`, `MGET`, `MSET`, `MSETNX`, `GETDEL`, `GETSET`, `GETEX`, `APPEND`, `STRLEN`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `SETBIT`, `GETBIT`, `SETRANGE`, `GETRANGE`, `SUBSTR`, `LCS`

**Keys** — `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `KEYS`, `RANDOMKEY`, `SCAN`, `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `DUMP`, `RESTORE`, `MIGRATE`, `SORT`, `OBJECT`, `SWAPDB`

**Hashes** — `HSET`, `HGET`, `HMGET`, `HMSET`, `HSETNX`, `HDEL`, `HLEN`, `HEXISTS`, `HGETALL`, `HVALS`, `HKEYS`, `HINCRBY`, `HINCRBYFLOAT`, `HSCAN`

**Lists** — `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `BLPOP`, `BRPOP`, `LMOVE`, `BLMOVE`, `BRPOPLPUSH`

**Sets** — `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`, `SPOP`, `SMOVE`, `SINTERCARD`, `SSCAN`

**HyperLogLog** — `PFADD`, `PFCOUNT`, `PFMERGE` (self-consistent dense form; see `compat/delta.md`)

**Sorted Sets** — `ZADD` (NX/XX/GT/LT/INCR), `ZRANGE`, `ZSCAN`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZPOPMIN`, `BZPOPMAX`

**Streams** — `XADD`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`, `XGROUP`, `XREADGROUP`, `XREAD` (blocking), `XACK`, `XPENDING`, `XINFO`, `XSETID`, `XTRIM`, `XCLAIM`, `XAUTOCLAIM` — with a full PEL (delivery time/count) and **RDB persistence + replication** of streams

**Pub/Sub** — `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` (CHANNELS/NUMSUB/NUMPAT), `SSUBSCRIBE`/`SUNSUBSCRIBE`/`SPUBLISH` (sharded aliases)

**Transactions** — `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

**Scripting** — `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT` (LOAD/EXISTS/FLUSH/KILL), `FUNCTION` (LOAD/LIST/DELETE/FLUSH/STATS/KILL), `FCALL`, `FCALL_RO`

**Replication** — `SYNC`, `PSYNC`, `REPLCONF`, `REPLICAOF`, `SLAVEOF`, `WAIT`

**Cluster** — `CLUSTER` (INFO/MYID/NODES/SLOTS/KEYSLOT/MEET/ADDSLOTS/SETSLOT/…), `ASKING`

**Modules** — `MODULE` (LOAD/LIST/UNLOAD)

</details>

## Testing

PeaDB has a comprehensive suite spanning unit, integration, fuzz, and differential testing.

Parity against real Redis is measured by the **upstream Redis 7.2.5 TCL suite running in native mode** (the official harness spawns `peadb-server` as a redis-server stand-in). Adopted suites, per-suite baselines, and the monotonic no-regression ratchet live in [`compat/redis_suite_ledger.json`](compat/redis_suite_ledger.json); every skipped upstream test is a tracked debt in `scripts/redis/skipfiles/` tied to a [`compat/delta.md`](compat/delta.md) entry.

```bash
# Run all tests (10 stages: unit, integration, coverage, compat, fuzz, stability, perf, sentinel, redis, diff)
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

PeaDB is a drop-in replacement for core Redis workloads — standard clients connect unmodified, and you can migrate in via `REPLICAOF` (native PSYNC2 sync) or the RDB bridge scripts, and roll back the same way. Durability matches Redis (`appendfsync always`/`everysec`/`no`).

Full detail — drop-in posture, tested clients, `redis.conf` support, migration & rollback steps, the feature-status table, persistence internals, and security/monitoring notes — lives in **[docs/compatibility.md](docs/compatibility.md)**. The authoritative, always-current gap registry is **[compat/delta.md](compat/delta.md)**.

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
