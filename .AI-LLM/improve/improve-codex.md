# PeaDB Improvement Proposal (Codex, revised after cross-analysis)

Date: 2026-05-10

## 0) What other analyses had that this now adds

Compared to my prior draft, this revision adds:

1. Concrete persistence correctness gaps (AOF fsync policy, per-write reopen, BGSAVE child lifecycle, BGREWRITEAOF sync behavior, stream omission in RDB save).
2. Compatibility truth model (generated compatibility scoreboards + release gates + definition of done for “Redis-compatible” claims).
3. Algorithmic correctness/perf hot spots with specific fixes (`SCAN` cursor model, ZSET range complexity, active-expire sampling bias, `RANDOMKEY` O(N)).
4. Observability gaps in `INFO` (placeholder/static fields that should not be treated as production-grade metrics).
5. Better claim hygiene for Cluster/Sentinel/Modules and stale docs reconciliation (including Pub/Sub status drift).
6. Security realism (AUTH/ACL are largely surface-level today).

## 1) Current-state snapshot

1. Core centralization remains very high.
- `src/command.cpp`: ~6.7K LOC, 207 command registrations, large global mutable state.

2. Test surface is broad but skewed.
- 95 integration tests, 2 C++ unit tests.
- Differential tests exist but are too narrow to prove parity.

3. Build/CI signal quality is incomplete.
- No committed hosted CI workflow.
- Warning-focused build shows compiler warnings and no warning gate.

4. Runtime shape is still prototype-grade single-loop.
- Single event loop, no sharding, no io_uring backend yet.

## 2) High-risk findings

## A) Correctness and compatibility risks

1. `WAIT` blocks command path.
- Handler sleeps in a loop, which can stall the single server loop under load.

2. Replication path is partial.
- `PSYNC` always full-resync.
- No bounded backlog model.
- `REPLICAOF` bootstrap relies on detached thread + shelling out to Python.

3. Persistence durability and behavior gaps.
- AOF appends reopen file per write, no explicit `appendfsync` policy.
- `BGREWRITEAOF` behavior is synchronous despite background name.
- `BGSAVE` lacks robust in-progress child tracking/reaping guardrails.
- RDB save path currently skips stream keys (`src/rdb.cpp`), risking silent data-loss semantics.

4. Security/auth surface is mostly stub-level.
- `AUTH` currently returns `OK` for accepted argument shapes.
- ACL is minimal (`ACL SETUSER` success stub; no real enforcement model).

5. Redis-compat drift in docs/claims.
- README/FAQ/decision docs have stale or conflicting statements vs code.
- Example: historical Pub/Sub “stub” claims conflict with current message routing code paths.

## B) Runtime/data-structure risks

1. `SCAN` cursor model is unstable under mutation.
- Current cursor is index-like over `unordered_map` iteration order.

2. ZSET range operations are expensive.
- Current representation implies repeated sort/materialization patterns.

3. Active expiry sampling is biased.
- Fixed front-of-map walk per cycle can delay tail reclamation patterns.

4. Blocking wakeups are poll-based.
- `notify_key_written` is effectively inert today; wake latency relies on periodic scans.

5. Memory layout overhead is high.
- `Entry` carries multi-type payload fields simultaneously.

## C) Operational risks

1. `INFO` exposes placeholder/static metrics in several sections.
- Can mislead operators and dashboards if treated as real telemetry.

2. Test scripts use destructive cleanup patterns.
- `pkill -9 -f peadb-server` can kill unrelated sessions.

3. Integration harnesses are fragmented.
- Many tests duplicate server lifecycle and hardcode ports, limiting safe parallelism.

## 3) Revised prioritized plan

## P0 (Immediate: 1-5 days) — correctness first

1. Unblock event loop semantics.
- Make `WAIT` asynchronous in event loop (ACK-driven + timeout state), no sleep loop in handler.

2. Persistence safety baseline.
- Convert AOF to persistent fd + explicit `appendfsync` mode (`always|everysec|no`).
- Add error propagation for AOF write/open failures.
- Add BGSAVE child PID state + non-overlap guard + reap handling.
- Clarify BGREWRITEAOF semantics (either truly background or rename behavior/docs now).

3. Remove destructive process killing in test scripts.
- Track owned child PIDs/process groups and terminate deterministically.

4. Fix config precedence semantics.
- Apply config file first, then CLI overrides; add tests.

5. Establish compatibility truth outputs.
- Generate command inventory/status and fail CI on untracked mismatches.

## P1 (Short-term: 1-2 weeks) — compatibility gates and observability

1. Add hosted CI tiers.
- PR: build + unit + integration smoke + differential smoke + compatibility metadata checks.
- Nightly: Redis Tcl subset, ASAN/UBSAN, fuzz sanity, stability scripts.

2. Strengthen differential and coverage gates.
- Replace “command-name mention” coverage with executable command invocation coverage.
- Expand differential corpus by domain + error precedence cases.

3. Reconcile docs to a single source of truth.
- `compat/delta.md` becomes canonical status registry; README/FAQ link to it.
- Explicitly label features as `full|partial|surface|stub`.

4. Harden `INFO` accuracy.
- Replace placeholder counters/fields with tracked values or clearly marked unavailable fields.

## P2 (Medium: 2-4 weeks) — safe refactor for velocity

1. Split `command.cpp` by domain with no behavior change.
- Keep central dispatcher thin and explicit.

2. Encapsulate global state.
- Move replication/pubsub/cluster/module/script state into structured context objects.

3. Add command-contract tests.
- Assert arity, flags, key positions, and metadata consistency snapshots.

4. Expand C++ unit testing beyond 2 tests.
- Protocol parser, RDB edge cases, command dispatch invariants, cluster keyslot behavior.

## P3 (Medium-long: 3-8 weeks) — data-structure + protocol parity

1. Address algorithmic parity/perf issues.
- Redis-like SCAN cursor strategy.
- Better active-expire sampling strategy.
- ZSET structure upgrade (dual index model).
- Faster `RANDOMKEY` strategy under large keyspaces.

2. Replication parity upgrades.
- Bounded backlog and PSYNC2 partial resync semantics.
- Accurate replication reporting in `INFO`.

3. Persistence parity upgrades.
- RDB stream coverage and edge encodings.
- AOF rewrite and fsync behavior aligned with Redis expectations.

## P4 (Long-term architecture) — after parity gates are strong

1. Entry payload redesign (`variant`/tagged payload) for memory/cache efficiency.
2. Poller abstraction extraction and future io_uring backend.
3. Sharded execution model aligned to `project-requirements.md`, with deterministic cross-shard coordination for transactions/scripts/replication.

## 4) Claim hygiene policy (new)

A feature may be called “Redis-compatible” only if all are true:

1. It passes targeted integration tests.
2. It passes Redis-vs-PeaDB differential cases (including error shape/precedence where relevant).
3. Relevant upstream Redis tests pass, or waiver is explicit in `compat/delta.md`.
4. Persistence/replication effects are covered when feature mutates state.
5. RESP2/RESP3 encoding behavior is validated when applicable.
6. Docs/status matrices are updated.

Until then, status must be `surface`, `partial`, or `experimental`.

## 5) Recommended first 12 PRs

1. Make `WAIT` nonblocking.
2. Replace AOF reopen-per-write with persistent fd and add `appendfsync` policy.
3. Add BGSAVE child tracking/reap + overlap guard.
4. Replace `pkill -9` cleanup with owned-process cleanup.
5. Add hosted CI workflow (PR + nightly split).
6. Upgrade command coverage gate from mention-based to invocation-based.
7. Expand differential suite and publish generated mismatch reports.
8. Reconcile README/FAQ/compat docs to one canonical status source.
9. Add tests proving Pub/Sub message delivery and subscribed-mode behavior.
10. Split `command.cpp` into domain files (no semantic changes).
11. Introduce explicit runtime context structs for replication/pubsub/persistence.
12. Fix or explicitly delta-track stream omission in RDB save path.

## 6) Success metrics

1. No event-loop stall from `WAIT` under concurrent load tests.
2. No destructive cross-process kills in test scripts.
3. CI has deterministic pass/fail for compatibility gates.
4. Differential mismatch count trends to zero or tracked waivers only.
5. Docs and compatibility matrices stay synchronized with code.
6. Refactor reduces average PR blast radius in command-related changes.
