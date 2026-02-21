# PeaDB

A high-performance, drop-in replacement for Redis, written in modern C++20.

PeaDB aims to be **behaviorally indistinguishable** from Redis 7.2.5 — same protocol, same commands, same edge-case semantics — while leveraging multi-core CPUs and cache-friendly data structures for superior throughput.

## Highlights

- **Full RESP2/RESP3 protocol** — works with every existing Redis client and tool, zero changes required.
- **~147 commands** — strings, hashes, lists, sets, sorted sets, streams, pub/sub, transactions, Lua scripting, and more.
- **Persistence** — RDB snapshots, AOF logging, and `SAVE`/`BGSAVE`/`BGREWRITEAOF`.
- **Replication** — PSYNC2-based replication with multi-replica support and replica promotion.
- **Cluster** — 16 384 hash-slot routing, `MOVED`/`ASK` redirection, slot migration, gossip protocol.
- **Lua scripting** — `EVAL`/`EVALSHA`, `FUNCTION` subsystem, `FCALL`/`FCALL_RO`, with cjson, cmsgpack, bit, and struct libraries.
- **Redis Modules API** — partial `RedisModule_*` C API; load `.so` modules at runtime via `MODULE LOAD`.
- **Docker-ready** — multi-stage Dockerfile and docker-compose included.

## Quick Start

### Prerequisites

- C++20 compiler (GCC 11+ or Clang 14+)
- CMake ≥ 3.16
- Lua 5.1 development libraries

On Ubuntu/Debian:

```bash
sudo apt-get install build-essential cmake g++ liblua5.1-0-dev
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

```bash
docker compose up -d
```

The container exposes port `6379` (configurable via `PEADB_PORT`) and persists data to a named volume.
For high-connection load tests, compose sets `ulimits.nofile` by default (`PEADB_NOFILE_SOFT` / `PEADB_NOFILE_HARD`, both default to `65536`).

## Configuration

PeaDB accepts Redis-style configuration via CLI flags or a config file.

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `6379` | Listening port |
| `--bind` | `127.0.0.1` | Bind address |
| `--loglevel` | `info` | Log level (`error`, `warn`, `info`, `debug`) |
| `--dir` | `.` | Data directory for snapshots and AOF |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |
| `--config` | — | Path to a config file |
| `appendonly` | `false` | Enable AOF persistence |
| `appendfilename` | `appendonly.aof` | AOF filename |

Runtime configuration is available via `CONFIG GET` / `CONFIG SET`.

## Supported Commands

<details>
<summary>Full command list (~147 commands)</summary>

**Connection & Server** — `PING`, `ECHO`, `QUIT`, `HELLO`, `AUTH`, `ACL`, `INFO`, `COMMAND`, `CONFIG`, `DEBUG`, `SELECT`, `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `WAIT`, `TIME`, `DBSIZE`, `FLUSHALL`, `FLUSHDB`, `SHUTDOWN`

**Strings** — `GET`, `SET`, `SETEX`, `PSETEX`, `SETNX`, `MGET`, `MSET`, `MSETNX`, `GETDEL`, `GETSET`, `GETEX`, `APPEND`, `STRLEN`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `SETBIT`, `GETBIT`, `SETRANGE`, `GETRANGE`, `SUBSTR`, `LCS`

**Keys** — `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `KEYS`, `RANDOMKEY`, `SCAN`, `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `DUMP`, `RESTORE`, `MIGRATE`, `SORT`, `OBJECT`, `SWAPDB`

**Hashes** — `HSET`, `HGET`, `HMGET`, `HMSET`, `HSETNX`, `HDEL`, `HLEN`, `HEXISTS`, `HGETALL`, `HVALS`, `HKEYS`, `HINCRBY`, `HINCRBYFLOAT`, `HSCAN`

**Lists** — `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `BLPOP`, `BRPOP`, `LMOVE`, `BLMOVE`, `BRPOPLPUSH`

**Sets** — `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`, `SPOP`, `SSCAN`

**Sorted Sets** — `ZADD` (NX/XX/GT/LT/INCR), `ZRANGE`, `ZSCAN`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZPOPMIN`, `BZPOPMAX`

**Streams** — `XADD`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`, `XGROUP`, `XREADGROUP`, `XREAD`, `XACK`, `XPENDING`, `XINFO`

**Pub/Sub** — `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`

**Transactions** — `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

**Scripting** — `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT` (LOAD/EXISTS/FLUSH/KILL), `FUNCTION` (LOAD/LIST/DELETE/DUMP/RESTORE/FLUSH/STATS), `FCALL`, `FCALL_RO`

**Replication** — `SYNC`, `PSYNC`, `REPLCONF`, `REPLICAOF`, `SLAVEOF`, `WAIT`

**Cluster** — `CLUSTER` (INFO/MYID/NODES/SLOTS/KEYSLOT/MEET/ADDSLOTS/SETSLOT/…), `ASKING`

**Modules** — `MODULE` (LOAD/LIST/UNLOAD)

</details>

## Testing

PeaDB has a comprehensive test suite spanning unit, integration, fuzz, and differential testing.

```bash
# Run all tests (unit + integration, 9 stages)
scripts/run_all_tests.sh

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

Integration tests are organized by milestone (m0–m9 and polish stages p0–p3), covering ~95+ Python test files across all data types, persistence modes, replication, scripting, modules, and edge cases.

### ASAN Build

```bash
cmake -S . -B build-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
cmake --build build-asan -j$(nproc)
```

## Benchmarks

Baseline numbers on a single connection (representative, not peak):

| Operation | ops/sec |
|-----------|---------|
| SET | 63,694 |
| GET | 59,524 |
| LPUSH | 62,893 |
| LPOP | 61,728 |
| INCR | 63,694 |
| SADD | 63,694 |

Full `redis-benchmark` and `memtier_benchmark` results are archived under `artifacts/`.

## RDB Import / Export

Bridge scripts are provided for migrating data to and from Redis:

```bash
# Import an existing RDB file into PeaDB via a Redis intermediary
python3 scripts/redis/import_rdb_via_redis.py <dump.rdb>

# Export PeaDB data to an RDB file via a Redis intermediary
python3 scripts/redis/export_rdb_via_redis.py <output.rdb>
```

## Known Compatibility Deltas

PeaDB targets behavioral parity with Redis 7.2.5. Current known gaps:

| Area | Severity | Notes |
|------|----------|-------|
| ACL completeness | High | Basic `AUTH` / default user only; full ACL engine deferred |
| RDB binary compatibility | High | Import/export via bridge scripts; native RDB parser in progress |
| AOF parity gaps | Medium | Core AOF works; full edge-case parity in progress |
| Module ecosystem certification | High | Minimal `RedisModule_*` API; RedisJSON / RediSearch certification deferred |

See [compat/delta.md](compat/delta.md) for the full delta tracker.

## Project Structure

```
include/          C++ headers (command, config, datastore, protocol, server, …)
src/              Implementation files + main entry point
tests/
  unit/           C++ unit tests (CTest)
  integration/    Python integration tests (~95+ files)
  fuzz/           RESP parser and command dispatcher fuzz tests
  diff/           Differential tests (PeaDB vs Redis)
scripts/
  ci/             CI pipelines and checks
  qa/             Fuzz, stability, perf, and sentinel validation
  dev/            Developer helpers (quick_start, install_deps)
  redis/          Upstream Redis test runner + RDB bridge scripts
compat/           Compatibility delta tracker
docs/decisions/   Architecture decision records
artifacts/        Benchmark results and performance baselines
```

## License

[MIT](LICENSE) © 2026 alsatianco
