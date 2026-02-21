# PeaDB — Project Requirements

> A high-performance, drop-in replacement for Redis, written in modern C++.
>
> **Design principle:** Maximum compatibility with Redis. If Redis does it, PeaDB does it identically — including edge cases.  
> **Specification:** Redis is the executable specification; for undocumented behavior, Redis source code defines truth.

Date: 2026-02-17

---

## 1. Project Overview

**Project Name:** peadb  
**Language:** Modern C++ (C++20 or newer)  
**Goal:** Implement a behaviorally indistinguishable, production-grade, Redis-compatible in-memory data store with full feature parity and superior multi-core scalability, while preserving maximum external compatibility.

PeaDB must be safe to deploy as a drop-in replacement in existing Redis production environments.

---

## 2. Goals

1. **Drop-in replacement.** Any existing Redis client, library, tool, or module must work against PeaDB with zero changes.
2. **Performance superiority.** Leverage multi-core CPUs, modern Linux I/O (`io_uring`), and cache-friendly data structures to outperform Redis on throughput, latency, and memory efficiency *without semantic changes*.
3. **Operational transparency.** Existing Redis configs, monitoring dashboards, and deployment playbooks must work unmodified.
4. **Migration path.** Users must be able to migrate Redis → PeaDB and PeaDB → Redis with no data loss and minimal downtime.

---

## 3. Non-Goals

- PeaDB is **not** a new database with a new API. It is Redis, faster.
- PeaDB does **not** introduce new data types, commands, or semantics beyond what Redis provides (additive extensions are allowed only if they do not break existing parsers/tools and are clearly namespaced).
- PeaDB does **not** target Windows for production. Linux is the primary platform.

---

## 4. Compatibility Requirements (Non-Negotiable)

PeaDB MUST be indistinguishable from Redis at all observable layers.

### 4.1 Canonical Specification & Validation Gates
- Redis behavior is the spec; Redis tests are the contract.
- MUST pass:
  - Redis official TCL test suite (`redis/redis/tests/`) in CI (release gate).
  - Redis cluster test suite (release gate for cluster mode).
  - Sentinel compatibility tests / validation (release gate for HA claims).
- For any discrepancy:
  - Prefer changing PeaDB to match Redis.
  - If deviation is unavoidable, it MUST be explicitly documented and tested as a known incompatibility (should be extremely rare).

### 4.2 Protocol Compatibility
- Full RESP2 support.
- Full RESP3 support (default when negotiated).
- Pub/Sub protocol compatibility.
- Replication protocol compatibility.
- Cluster bus protocol compatibility.
- Support Redis inline command format (required for `redis-cli` compatibility).
- No client changes required.

Performance requirements on hot paths:
- RESP parsing MUST minimize allocations and copies (zero-copy/minimal-copy design).
- SHOULD use SIMD acceleration for parsing where beneficial.

### 4.3 Command Compatibility
PeaDB MUST implement:
- All Redis commands (including less common and “ops” commands where tests/tooling depend on them).
- All command flags and modifiers (NX, XX, GT, LT, etc.).
- Identical return types and wire encodings.
- Identical error messages where clients/tests rely on them.
- Identical **error precedence** and argument validation order.
- Identical blocking behavior and wake-up behavior.

Additional compatibility constraints:
- `OBJECT ENCODING` MUST return Redis-compatible encoding names regardless of PeaDB internals.
- `COMMAND` metadata (`COMMAND INFO`, `COMMAND DOCS`, `COMMAND COUNT`) MUST be compatible.

### 4.4 Behavioral Compatibility (Edge Cases Included)
The following MUST match Redis semantics exactly:
- Expiration timing behavior (lazy + active model)
- TTL resolution and drift behavior
- Eviction policies (including Redis LRU/LFU sampling behavior)
- MULTI/EXEC atomicity
- WATCH invalidation semantics
- Transaction behavior details (including rollback/abort behavior as observed)
- Lua script execution guarantees and atomicity
- Script replication behavior
- Blocking command wake-up order/fairness expectations
- Streams ID generation and ordering rules
- Consumer group PEL semantics
- WAIT guarantees and replication offset semantics
- Determinism rules that impact replication, scripts, and streams

If Redis behavior is undocumented, Redis source code defines the specification.

---

## 5. Distributed Compatibility

### 5.1 Replication (PSYNC)
MUST implement Redis replication with full interop:
- PSYNC behavior (PSYNC2).
- replid handling, replication offsets, backlog semantics.
- Partial resynchronization.
- Multi-replica support.
- Replica promotion behavior consistent with Redis expectations.

**Migration requirement (adoption-critical):**
1. PeaDB MUST replicate from a Redis master.
2. PeaDB MUST function as a Redis-compatible replica.
3. A PeaDB replica MUST be promotable to master in an environment managed by existing Redis tooling (including Sentinel expectations where applicable).
4. No application changes required.

### 5.2 Sentinel Compatibility
- PeaDB does not need to reimplement Sentinel (it is a separate process), but MUST be manageable by existing Redis Sentinel deployments:
  - Must be monitorable and electable.
  - Failover and promotion timing must match Sentinel expectations.
  - Must answer probing/introspection commands correctly (`PING`, `INFO` replication section, etc.).

### 5.3 Cluster Compatibility
In cluster mode, PeaDB MUST match Redis Cluster semantics:
- 16384 hash slots.
- MOVED and ASK behavior.
- Slot migration behavior (IMPORTING/MIGRATING flows).
- Config epoch handling and cluster state transitions.
- Gossip protocol compatibility (cluster bus on `N+10000`).
- Cross-slot command handling identical to Redis Cluster behavior.
- Mixed clusters: SHOULD be able to join an existing Redis cluster as a peer (protocol-level interop).

---

## 6. Persistence Compatibility

MUST provide Redis-compatible persistence:
- Must load Redis RDB files.
- Must generate RDB compatible with Redis.
- AOF format compatibility.
- AOF rewrite compatibility.
- Replication-based persistence compatibility expectations.

Performance constraints (must not change semantics):
- SHOULD implement forkless / low-latency snapshotting while producing standard RDB output.
- SHOULD use async I/O for persistence (e.g., `io_uring`).

---

## 7. Modules API Compatibility

- MUST support the Redis Modules C API (`RedisModule_*`).
- **Binary compatibility is the target**: existing compiled modules (`.so`) should load and run without recompilation.
- If full binary compatibility is not feasible for an early milestone, a compatibility shim MAY exist temporarily, but:
  - It MUST be explicitly scoped and documented.
  - It MUST still support widely used modules (RedisJSON, RediSearch, RedisTimeSeries, RedisBloom) without modification as a certification goal.

Design constraint:
- PeaDB internals (command dispatch, key access, type system, threading) MUST be designed around the module ABI boundary to avoid “loads but misbehaves” outcomes.

---

## 8. Architectural Requirements

### 8.1 Core Execution Model
- Sharded architecture (slot-based).
- Single-threaded execution per shard (no fine-grained locking inside shard on the hot path).
- Lock-free inter-shard communication preferred.
- Deterministic per-key execution.

Cross-shard correctness requirements:
- Replication stream MUST be deterministic.
- Lua replication MUST match Redis semantics.
- Transaction ordering MUST match Redis.
- Stream ID generation MUST match Redis exactly.

### 8.2 Networking
- High-performance async I/O.
- `io_uring` (Linux) preferred.
- Zero-copy (or minimal-copy) replication buffers.
- SIMD-accelerated RESP parsing (where beneficial and safe).

### 8.3 Memory Management
- Prefer allocator strategies that reduce fragmentation and improve locality:
  - Custom slab allocator and/or per-shard memory arenas.
  - NUMA-aware allocation.
- Inline small string optimization.
- Avoid excessive pointer indirection.

Compatibility constraint:
- External memory behavior MUST remain compatible where it affects observable outcomes (eviction decisions, TTL behavior, memory reporting commands).

### 8.4 Data Structures
Internal structures may differ from Redis but MUST preserve semantics:
- Hash tables: cache-friendly, open addressing preferred.
- Sorted sets: cache-optimized but behavior-identical structure.
- Streams: semantics identical; internal representation flexible.
- Lists: must preserve blocking behavior.
- HyperLogLog: identical probabilistic behavior (bit-level compatibility where externally observable).
- Bitmaps and bit operations: identical semantics and edge cases.

---

## 9. Testing Requirements (Mandatory)

### 9.1 Differential Testing Framework
A continuous differential testing framework MUST exist:
- For each command / scenario:
  - Execute against Redis.
  - Execute against PeaDB.
  - Compare:
    - Return values
    - Errors (including precedence/message where relevant)
    - State (keys/values/metadata)
    - TTL values
    - Replication offsets/ACK behavior
    - Persistence artifacts where applicable (e.g., RDB load equivalence; byte-identical output is not always required, but load/behavioral equivalence is)

Continuous fuzzing required (RESP, command args, RDB/AOF loaders, replication streams).

### 9.2 Stress & Stability Testing
MUST include:
- Long-running memory stability tests (leaks, fragmentation growth).
- Replication interruption/resume tests.
- Cluster failover chaos testing.
- Blocking command concurrency tests.
- Large dataset tests (e.g., 100M+ keys) where feasible.

### 9.3 Certification Checklist (before claiming compatibility)
- Redis test suites pass (standalone + cluster as applicable).
- Sentinel promotion validated.
- Third-party client libraries verified (curated list).
- Popular modules verified (certified set).

---

## 10. Performance Goals

Performance improvements must be invisible to external behavior.

Target improvements:
- Linear (or near-linear) multi-core scaling under concurrent clients.
- Reduced memory fragmentation and lower overhead per key.
- Faster replication (throughput and stability under reconnects).
- Faster RDB loading and snapshotting with reduced latency impact.
- Reduced tail latency under high concurrency and persistence activity.

No semantic changes allowed for performance gains.

---

## 11. Engineering Principles

- Redis source code is canonical specification.
- Compatibility over elegance.
- Determinism over cleverness.
- Production safety over micro-optimizations.
- Continuous differential validation.
- No semantic deviation without upstream equivalence.

---

## 12. Long-Term Vision

PeaDB aims to:
- Serve as a fully compatible Redis drop-in replacement.
- Enable safe production migration (including rollback).
- Provide superior multi-core scalability and better latency under persistence workloads.
- Maintain strict behavioral equivalence while evolving internals.

End of Requirements.
