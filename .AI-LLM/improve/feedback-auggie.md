# Review of commit `2f42eda` ("Improvement")

_Reviewer: Auggie · Date: 2026-05-10 · Base: `383130c` → `2f42eda` (29 files, +3 384/−481)._
_Build: `cmake -S . -B build -DBUILD_TESTING=ON && cmake --build build -j8` — clean._
_Unit tests: `ctest` — 4/4 PASS (config_parser, wrongtype_framework, command_contract, mutation_epoch)._
_Integration tests: `scripts/ci/run_integration_tests.sh` — **94 PASS / 1 FAIL** (regression introduced, see B1)._

---

## 1. Summary verdict

The commit is a substantial, well-scoped step forward. It lands the majority of the **Tier 1 / P0 / Critical** items every reviewer flagged, removes the destructive `pkill -9` cleanup, adds a real CI skeleton, and restores docs/compat truth. However it ships three **broken or no-op** pieces (B1–B3) that any reviewer would block on, and several headline items are scaffolding only (S1–S4) — they appear in the diff but do nothing at runtime. Net assessment: **good direction, needs a follow-up before merge to main**.

---

## 2. Coverage matrix vs. the four analyses

Legend: ✅ done · 🟡 partial / scaffold only · ❌ not addressed · 🐛 introduced a bug

| # | Item | auggie | opus | codex | gpt | Status in 2f42eda |
|---|------|:--:|:--:|:--:|:--:|---|
| 1 | `WAIT` non-blocking (event-loop deferred) | 1.6 | C1 | P0.1 | 2.2 | ✅ `WaitState` + loop tick resolves |
| 2 | AOF persistent fd + `appendfsync` | 1.1 | C3 | P0.2 | 2.1 | ✅ `g_aof_fd`, `aof_fsync_if_needed`, `always`/`everysec`/`no` |
| 3 | BGSAVE child reaping + concurrency guard | 1.5 | C4 | P0.2 | 2.1 | ✅ `g_bgsave_child_pid`, `reap_background_children`, "already in progress" |
| 4 | BGREWRITEAOF async (fork + atomic rename) | — | C5 | P0.2 | 2.1 | ✅ Forks; reaped by same path |
| 5 | Atomic rename for RDB + AOF rewrite | 1.8 | C7 | P0.2 | 2.1 | ✅ `*.tmp` → `fsync` → `rename` (RDB & AOF) |
| 6 | Config precedence (file → CLI overrides) | 1.9 | §10 | P0.4 | P1 | ✅ `main.cpp` two-pass parse |
| 7 | RDB streams: silent drop → loud-fail | 1.7 | C2 | P3.3 | 2.1 | ✅ Logs warning per skipped stream key (delta.md updated) |
| 8 | PSYNC2 partial resync + bounded backlog | 3.5 | C8/§11 | P3.2 | 2.2 | ✅ 1 MiB ring `ReplBacklog`, `+CONTINUE` path |
| 9 | REPLICAOF in-process (no shell-out) | 3.5 | C6 | A.2 | 2.2 | 🟡 `replica_do_fullsync` does PING/REPLCONF/PSYNC + RDB load — but **no streaming loop after RDB**, so post-sync writes are lost (regression vs Python helper which at least re-runs periodically) |
| 10 | `mutation_epoch` reconciliation (single counter) | 3.8 | §11b | — | 2.1 | ✅ `g_mutation_epoch` deleted; `store().increment_mutation_epoch()` is the only path; covered by new `mutation_epoch_test` |
| 11 | SCAN reverse-binary-increment cursor | 3.1 | §6 | P3.1 | P4 | ✅ `scan_next_cursor` over bucket array |
| 12 | ZSet ordered index (avoid per-call sort) | 3.2 | §9 | P3.1 | P4 | ✅ `zset_ordered: std::map<{score,member},bool>`; ZRANGE/ZPOP use it |
| 13 | Active-expiry random start | 3.3 | §8 | P3.1 | P4 | ✅ `rng() % bucket_count` start, walks ring |
| 14 | RANDOMKEY O(N) → bucket walk | — | §9b | — | P4 | ✅ Random bucket start, returns first non-expired |
| 15 | `WriteBuf::compact` half-buffer threshold | 1.4 | §5b | — | 5 | ✅ `if (offset > size/2)` |
| 16 | Coalesce pipelined replies | 1.2 | §5a | — | 5 | ❌ `enqueue_output` still flushes per command |
| 17 | Replication offset double-encoding fix | 1.3 | §4 | — | 2.2 | 🟡 `g_master_repl_offset` is now overwritten by `backlog.total` after `backlog_append`, so the prior `+= encode_command_resp_size(args)` line in `handle_command` is dead arithmetic — encode still happens twice |
| 18 | `notify_key_written` actually wakes BLPOP | 3.4 | §7 | B.4 | 5 | 🟡 Registry + flag added; but the loop still polls **all** blocked sessions every tick — the flag is set then `clear_pending_wakeups()` with no use of the per-key fd list. Net latency unchanged. |
| 19 | Output-buffer hard limit | — | §14 | — | 5 | ✅ 256 MiB cap; client dropped on overrun. No soft limit / per-class. |
| 20 | Memory accounting (used_memory) | — | §12 | — | P4 | ✅ `estimate_used_memory()` walks all DBs; INFO MEMORY uses it |
| 21 | INFO observability (hits/misses/expired/evicted/avg_ttl/replicas) | — | §22 | C.1 | 6 | ✅ Real counters in `DataStore`; INFO STATS / KEYSPACE / REPLICATION wired |
| 22 | `pkill -9` removed; per-PID cleanup | 2.5 | §18 | P0.3 | P1 | ✅ `stop_server` with TERM→KILL; `trap cleanup EXIT` |
| 23 | Compiler warning gate | 2.4 | §20 | 1 | P1 | 🟡 `-Wall -Wextra -Wno-unused-parameter` added to `peadb_core` (not `-Werror`); CI job runs build but doesn't enforce zero warnings |
| 24 | `.clang-format` + lint job | 4.5 | §20 | 1 | P1 | 🟡 File added; PR workflow runs `clang-format --Werror`. Existing tree was **not** reformatted, so first PR job will likely fail (clang-format not installed locally so I couldn't verify) |
| 25 | CI workflow (PR + nightly) | 2.1 | §18 | P1.1 | P1 | ✅ Two YAMLs, ASAN+UBSAN nightly, integration smoke per-PR |
| 26 | Coverage gate: text → invocation | 2.6 | §16 | P1.2 | P0 | ❌ Old `check_command_test_coverage.py` still in use; new `generate_compat_scoreboard.py` is broken (B2) |
| 27 | Generated compat scoreboard | 5.2 | §16 | P1.5 | P0 | 🐛 Generator regex matches `r.SET(` / `execute_command("X")`; tests use `send_cmd(s, "X", …)` over raw sockets. Output reports **all 207 commands as `stub`** — checked into git that way. Confidence-destroying. |
| 28 | JUnit XML reports | 2.7 | — | P1.5 | P1 | ❌ Not added |
| 29 | Compat docs canonical (delta.md) | §0 | §0 | P1.3 | 8 | ✅ Header now says "canonical status registry"; status labels enumerated; entries 5–8 (streams, cluster bus, AUTH/ACL, REPLICAOF) added |
| 30 | README 147 → 207 | §0 | §0 | 5 | 5 | ✅ Both occurrences updated |
| 31 | New unit tests beyond 2 | 2.2 | §19 | P2.4 | 5 | 🟡 +2 tests (command_contract golden + mutation_epoch). Missing the EXPIRE matrix / ZADD flag / MULTI-EXEC abort tests called out by all four reviewers. |
| 32 | `Poller` extracted to header | 4.3 | §24A | — | P3 | ✅ `include/poller.hpp` |
| 33 | `thread_local` audit | 4.1 | §13 | — | P3 | 🟡 `g_in_lua_script`, `g_script_*`, `g_module_dma_buf` de-`thread_local`'d. But REPLICAOF still spawns a detached `std::thread` (line ~2079) — the audit committed to "single-threaded" without deleting that thread. Inconsistent. |
| 34 | cjson / cmsgpack stubs (Tcl `unit/scripting`) | 3.7 | §17 | — | 2.3 | 🟡 cmsgpack circular-ref / sort-order changed (sort removed, natural order); cjson not touched. No test added that proves the upstream Tcl case now passes. |
| 35 | Cluster bus binary protocol | 3.6 | §15 | — | 7 | ❌ Status only documented in delta.md (entry 6) — implementation unchanged |
| 36 | AUTH / ACL real enforcement | — | — | A.4 | 2.4 | ❌ Documented as `surface` in delta.md entry 7; no code change |
| 37 | Pub/Sub doc reconciliation | §0 | §29 | A.5 | 5 | ✅ Removed from "stub" claim; covered by delta.md update |
| 38 | `RespValue` typed return | 5.3 | §4 | P2.5 | 3 | ❌ Deferred (correctly — was Tier 6) |
| 39 | `Entry` → `std::variant` | T6 | §2 | P4.1 | P4 | ❌ Deferred (correctly) |
| 40 | DB-explicit DataStore API (kill `current_db_`) | 5.4 | §5 | P2.2 | 3 | ❌ Deferred |
| 41 | Sharding scaffolding | T6 | §25 | P4.3 | LT | 🟡 `ShardLoop`, `crc16`, `shard_init`, `shard_dispatch_fd` added — see S1 |
| 42 | io_uring scaffolding | T6 | §24 | P4.2 | LT | 🟡 `IoUringPoller` in `poller.hpp` — see S2 |
| 43 | `ServerContext` aggregate | T6 | §5 | P2.2 | 3 | 🟡 Header added — see S3 |

Tally: **22 ✅ · 12 🟡 · 8 ❌ · 1 🐛** (out of 43 reviewer items tracked).

---

## 3. Bugs & regressions to fix before merge

**B1. Existing integration test `test_m9_nightly_workflow.py` now FAILS.** When `.github/workflows/nightly.yml` did not exist, the test self-skipped. Now it activates and asserts `"tests/test_helper.tcl" in txt` and `"--single unit/" not in txt` and `"artifacts" in txt.lower()`. The new nightly workflow includes none of these, so the assertion trips. Either extend the workflow to actually invoke the upstream Redis Tcl runner (`scripts/redis/run_redis_tests.sh` exists) or update the assertion. This is a textbook "missed downstream change".

**B2. `generate_compat_scoreboard.py` regex mis-detects test invocations.** The generator looks for `r.SET(`, `execute_command("CMD"`, and `r.execute("CMD"`. The integration tests in this repo do not use redis-py — they speak RESP over raw sockets via `send_cmd(s, "CMD", …)`. Result: the committed `compat/commands.generated.md` reports `full: 0, partial: 0, stub: 207` for commands like `SET`, `GET`, `LPUSH`, `BLPOP`, `XADD` that have dedicated test files. This is exactly the "documentation drift" the scoreboard was supposed to fix; in its current form it makes things worse because it ships a confidently wrong artifact. Fix: scan for `send_cmd\(s, *"([A-Z][A-Z0-9_]+)"` (and equivalents like `\bsend\(.*"([A-Z][A-Z0-9_]+)\\r\\n`) — or, much better, parse `OK`-marked RESP from a test-log corpus produced by the integration runner.

**B3. `notify_key_written` is still effectively a no-op.** The new code maintains `g_key_waiters[key] -> vector<int>`, registers fds in BLPOP/BRPOP, sets `g_has_pending_wakeups = true` on `LPUSH`/`RPUSH`, and the server loop calls `clear_pending_wakeups()` — but never iterates `g_key_waiters[key]` to mark specific sessions ready. The fall-through path is the existing `if (blocked_count > 0) for (auto& [bfd, session] : sessions) ...`, which scans every connection on every loop tick anyway. So the new registry adds bookkeeping cost (insert on every BLPOP, erase on every unblock) for zero latency improvement. Either remove the registry or actually use it: in the loop, iterate the per-key fd list, look up the session, and call `try_unblock_client` only on those fds.

**B4. REPLICAOF: in-process full sync, but no streaming after RDB.** `replica_do_fullsync` connects, completes PSYNC, streams the RDB into a tmpfile, calls `rdb_load`, and returns. The TCP socket is closed (`::close(fd)` before `rdb_load`). There is no command-streaming loop reading subsequent writes from the master. The previous Python helper at least kept the master in sync because `sync_from_redis.py` could be re-invoked. Today's replica is frozen at sync time. This regresses the spirit of `gpt §2.2` and `opus C6` — security improved, completeness reduced. Mitigation: keep the socket open and run a small RESP-reader loop that calls `handle_command` on each parsed array, with `g_replaying_aof = true`-style suppression for AOF/replication side-effects.

**B5. `IoUringPoller::add_read` is API misuse.** `io_uring_prep_recv(sqe, fd, nullptr, 0, MSG_PEEK)` with a null buffer + length 0 will not behave as a readiness probe; liburing expects a real buffer. This file is dead code today (S2) so it doesn't affect runtime, but it would crash on first use.

---

## 4. Scaffolding-only items (no functional effect today)

**S1. Sharding (`shard_loop.{hpp,cpp}`).** `shard_init`/`shard_dispatch_fd`/`ShardLoop` are defined but **never called** from `server.cpp` or `main.cpp` (verified via `grep -rn`). `crc16` and `key_hash_slot` are useful primitives, but the rest is unreachable. Marking the file as scaffolding is fine; not noting it in `compat/delta.md` or any doc isn't.

**S2. `IoUringPoller`.** Defined in `poller.hpp` under `#ifdef PEADB_HAVE_IO_URING`. Never instantiated. `server.cpp` uses the plain `Poller`. CMake gates the macro on liburing pkg-config — so on Linux CI it would compile in but still not be used.

**S3. `ServerContext`.** Header defines `ReplicationState`, `PersistenceState`, `ClusterState`, `ServerStats`, `ServerCallbacks`. The actual globals (`g_master_repl_offset`, `g_aof_fd`, `g_bgsave_*`, `g_slot_routes`, `g_total_commands_processed`, …) all still live in the `command.cpp` anonymous namespace. Only `callbacks.count_synced_replicas` and `callbacks.client_list` are written; nothing reads them. The `ServerContext` exists so reviewers can tick a box; it does not yet replace any global. This is the change opus §5, codex P2.2, and gpt 3 all called for as a **prerequisite** for splitting `command.cpp` and for sharding — until callers actually consume it, the prerequisite is not satisfied.

**S4. `thread_local` removal without resolving the threading question.** auggie 4.1 / opus §13 explicitly framed this as a binary choice: either commit to single-threaded (and remove the detached threads) or add proper synchronization. This commit removes `thread_local` from `g_in_lua_script`, `g_script_readonly_context`, `g_script_client_resp_version`, `g_script_current_session`, `g_script_allow_oom`, `g_script_inner_error_seen`, `g_module_dma_buf` — but `REPLICAOF` still spawns `std::thread([host, p]() { … }).detach()` which calls into command-layer paths (`g_loading_replication.store`, `g_replica_stale = …`, `rdb_load → store()` writes). Today none of those touch the now-non-`thread_local` script state, so it works, but the invariant the original code relied on is gone with no replacement. A comment in `command.cpp` pinning "no thread other than the event loop may call into command/script paths" would close the gap.

---

## 5. Items the commit got right that are easy to overlook

- The mutation_epoch reconciliation is **clean**: `g_mutation_epoch` deleted, `store().increment_mutation_epoch()` is the single write path, `WATCH` switched to `store().mutation_epoch()`, and a unit test pins the invariant. This was item 11b in opus and 3.8 in auggie — frequently the kind of refactor that gets done halfway.
- The `ReplBacklog` ring (1 MiB, byte-offset addressed, wrap-aware `get_slice`) is the right shape for PSYNC2 and is wired correctly into all three replication append sites (single-write, EXEC-batch, EXEC-single).
- `flush_replication_stream` was rewritten to send one chunked payload per loop iteration instead of one per event — that is the single biggest win for replica throughput in this commit and it was not loudly called out.
- Atomic rename is consistent across both RDB save and AOF rewrite (`open(O_TRUNC) → write → fsync → rename → reopen persistent fd`); the AOF reopen step is easy to forget.
- The CI scripts' use of `trap cleanup EXIT` plus per-PID `stop_server` is the correct Redis-style fix for `pkill -9`. The 10 × 0.5 s grace period before SIGKILL matches Redis's own shutdown behaviour.

---

## 6. Recommended follow-up PR (single, focused)

1. Fix B1 by extending `nightly.yml` to invoke `scripts/redis/run_redis_tests.sh` (it already exists) and upload `artifacts/` so the assertions pass — or rewrite the assertion to match current intent.
2. Fix B2 by rewriting the regex set in `generate_compat_scoreboard.py` to recognise `send_cmd(s, "CMD"` (and the `b"*N\r\n$M\r\nCMD"` raw-RESP form for the few tests that bypass the helper). Re-run, commit the regenerated `compat/commands.generated.md`, then promote the scoreboard to a CI gate (fail on stub-without-waiver).
3. Fix B3 by either (a) deleting `g_key_waiters` / `notify_key_written` and reverting to the existing poll, or (b) walking `g_key_waiters[key]` from inside the blocked-client section so wake-up is genuinely O(waiters), not O(connections).
4. Fix B4 by adding a streaming loop after `rdb_load` in `replica_do_fullsync`. ~80 lines, same RESP parser already used in `protocol.cpp`.
5. Fix B5 by deleting `IoUringPoller` until there is a caller, or by giving it a real per-fd buffer pool. Dead `#ifdef`'d code attracts copy-paste.
6. Add the EXPIRE-options / ZADD-flags / MULTI-EXEC unit tests called out by all four reviewers (each <100 lines, use the same `handle_command` direct-call style as `mutation_epoch_test`).
7. Either delete `ShardLoop` / `shard_*` until the sharding work begins, or move them under `experimental/` and gate with a CMake option so they don't appear in default builds.

With those seven items the commit goes from "good direction, needs a follow-up" to "the right Tier 1 baseline for everything else queued in opus / codex / gpt".
