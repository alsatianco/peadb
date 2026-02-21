# D0 Decisions

Date: 2026-02-17

## D0.1 Redis Compatibility Target

- Pinned upstream Redis baseline: `7.2.5`
- Source: official Redis release tarball fetched on demand for compatibility testing
- Commit: `f60370ce28b946c1146dcea77c9c399d39601aaa`

Rationale:
- Widely deployed stable series.
- Includes RESP3, ACL, modules, replication/cluster maturity.
- Keeps compatibility target fixed for deterministic behavior and tests.

## D0.2 Auth/ACL Scope for M1-M3

In scope for M1-M3:
- `AUTH` command parsing and authentication flow with a single configured password.
- `HELLO` authentication path (`HELLO 3 AUTH <user> <pass>` semantics for default user only).
- ACL surface commands can be present as stubs when needed by probes/tests, but no full multi-user policy engine yet.

Out of scope for M1-M3:
- Full Redis ACL rule language and per-category/per-command grants.
- Full user lifecycle (`ACL SETUSER/DELUSER/...`) semantics.

Quality bar:
- Error precedence and error text for implemented AUTH paths must match Redis 7.2.5.
- Deferred ACL features must be tracked as explicit deltas in `compat/delta.md`.

## D0.3 Linux / io_uring Baseline

- Minimum kernel target: Linux `5.15+`
- `liburing` target: `2.4+`

Policy:
- Runtime backend is pluggable.
- `epoll` fallback is required for environments without usable io_uring.
- Semantics must be backend-independent.

## D0.4 Initial Certification Matrix

Clients (version pinned):
- `redis-cli` from Redis `7.2.5`
- `redis-py==5.0.1`
- `node-redis@4.6.13`
- `go-redis/v9@v9.5.1`

Modules (version pinned):
- RedisJSON `v2.8.7`
- RedisBloom `v2.8.2`
- RedisTimeSeries `v1.12.2`
- RediSearch `v2.10.7`

Execution plan:
- M8 starts with one module passing unmodified load/basic operation.
- Expand to full list in nightly certification jobs.
