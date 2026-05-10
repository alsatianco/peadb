# Compatibility Delta

Date: 2026-05-10
Redis pin: 7.2.5 (`f60370ce28b946c1146dcea77c9c399d39601aaa`)

This file is the **canonical status registry** for PeaDB compatibility gaps.
README and FAQ links should reference this document rather than repeating status.

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
- Missing: full Redis RDB parity (broader opcode/encoding coverage, edge-case behavior, and rarely-used features).
- Tracking: ongoing RDB parity hardening.

3. AOF binary/semantic parity gaps
- Status: temporary delta.
- Owner: persistence-compat
- Severity: medium
- Target milestone: P3
- Implemented: AOF append + startup replay + rewrite for supported commands.
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
- Status: `surface` (data loss risk)
- Owner: persistence-compat
- Severity: high
- Target milestone: P2
- Implemented: XADD/XREAD/XREADGROUP in memory; stream keys are visible and queryable.
- Missing: RDB save encodes stream keys — they are silently skipped during BGSAVE/SAVE.
  A warning is logged to identify affected keys. Stream data does NOT survive restart.
- Tracking: P2-1 in improvement plan.

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
- Status: `surface`
- Owner: security-compat
- Severity: high
- Target milestone: P3
- Implemented: AUTH command accepted; ACL SETUSER/GETUSER/LIST/LOG/CAT registered.
- Missing: real password enforcement and ACL rule evaluation. Any password is accepted.
- Tracking: security hardening milestone.

8. REPLICAOF in-process streaming
- Status: `partial` (functional but fragile)
- Owner: replication-compat
- Severity: medium
- Target milestone: P2
- Implemented: REPLICAOF triggers sync via a Python helper script (system() call).
  Full-resync only — PSYNC2 always triggers full resync, no partial resync.
- Missing: in-process replication client (no Python dependency), partial resync,
  bounded replication backlog.
- Tracking: P2-2 and P3-1 in improvement plan.
