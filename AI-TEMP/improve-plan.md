# PeaDB Redis-Parity Work — Handoff / Continuation Plan

> **Purpose of this file:** you are a fresh Claude Code instance with **no prior conversation
> context and no auto-memory** from the previous developer/AI. This document contains everything
> you need to pick up the PeaDB Redis-7.2.5 parity work exactly where it was left off. Read it
> top to bottom before doing anything. Written 2026-07-07.

---

## 0. TL;DR — where things stand right now

- **Project:** PeaDB, a Redis-7.2.5-compatible server in C++20 (`/Users/duc.nguyen/git2/peadb`, git branch `main`).
- **Goal:** make PeaDB behaviorally indistinguishable from Redis 7.2.5, driven by adopting the **upstream Redis TCL test suite** one suite at a time, in a **CI-gated, monotonically-improving** way.
- **Current parity:** **29 upstream suites adopted native+green**, **1862 `ok`**, **0 `err`**, **66 skipfile lines** (all documented), **pass-rate 96.6%**. Upstream-proven command count 213/234.
- **Latest commit:** `903b3a8` (wave 16). The tree is clean (only untracked `.claude/` and `AI-TEMP/`).
- **How work is done:** pick the next unadopted suite → run it → fix whatever it surfaces (real features/bugs) or skipfile with a documented reason → register it → run the two mandatory gates → commit. One suite (or a small batch) per commit.

**The single most important habit:** a suite is only "adopted" when it **runs to completion with exit 0 / "All tests passed"**, NOT merely `err=0`. A mid-suite hang or exception records a truncated-but-green-looking `ok` count. (This bit us: `unit/auth` and `unit/pubsub` had been "adopted while hanging" — see §7.)

---

## 1. Build, run, test — exact commands

```bash
# Build (Release). Requires cmake, g++ (C++20). Lua 5.1.5 is bundled in third_party/.
cd /Users/duc.nguyen/git2/peadb
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON   # first time only
cmake --build build -j$(sysctl -n hw.ncpu)                         # macOS; use nproc on Linux

# C++ unit tests (fast)
cd build && ctest --output-on-failure

# The server binary lands at ./peadb-server
```

### The upstream TCL harness (the parity engine)

The Redis 7.2.5 source + test tree is cached at `~/.cache/peadb/redis-7.2.5/`. The harness spawns
`peadb-server` as a `redis-server` stand-in ("native mode").

```bash
# Run ONE upstream suite natively (this is what you do constantly while adopting a suite):
cd ~/.cache/peadb/redis-7.2.5 && SUITE_MODE=native \
  REDIS_SERVER_BIN=/Users/duc.nguyen/git2/peadb/peadb-server \
  timeout 250 ./runtest --single unit/<SUITE> --clients 1 \
  --tags "-tls -cluster -modules -needs:pfdebug -leaks" --dont-clean \
  --skipfile /Users/duc.nguyen/git2/peadb/scripts/redis/skipfiles/<suite_with_underscores>.skip \
  > /tmp/out.log 2>&1
# (the `-leaks` tag turns leak-check tests into [ignore]; that matches the canonical run)
# Drop the --skipfile flag if the suite has no skipfile yet.

# Count results (strip ANSI first — the harness colorizes):
c=$(sed 's/\x1b\[[0-9;]*m//g' /tmp/out.log)
echo "ok=$(echo "$c"|grep -c '\[ok\]') err=$(echo "$c"|grep -c '\[err\]') exc=$(echo "$c"|grep -c '\[exception\]') skip=$(echo "$c"|grep -c '\[skip\]') ignore=$(echo "$c"|grep -c '\[ignore\]')"
echo "$c" | grep -E "All tests passed without errors|tests failed"
echo "$c" | grep -E "\[err\]:|\[exception\]:|Expected"   # see failures
```

**Gotchas with the harness:**
- The TCL control server uses a **fixed port 21079** — you cannot run two `runtest` instances at
  once. If you get `couldn't open socket: address already in use`, kill leftovers:
  `pkill -f "tests/tmp/redis.conf"; pkill -f "test_helper.tcl"` then retry.
- The Bash tool's default timeout is 2 min; pass a larger `timeout` for slow suites, or run in
  background (`run_in_background: true`) and Monitor for completion. Some suites (scripting, other,
  latency-monitor with its 1M-key expire test) are slow.
- zsh word-splitting: `$var` is NOT split into words in zsh (unlike bash). Don't build a
  `--skipfile $sf` arg in a shell variable and pass it unquoted — pass the flag literally.
- The canonical **`ignored`** count in the ledger = `[ignore]` (leak tests) + `[skip]` (skipfile
  lines). `parse_tcl_results.py` reports it; trust that over hand-counting.

---

## 2. The two MANDATORY pre-commit gates (never skip either)

Per `CLAUDE.md`, before every commit you MUST run both and confirm they pass:

```bash
# Gate 1: full suite (unit + integration + fuzz + ASAN/UBSAN + sentinel + FULL native TCL sweep + differential)
bash scripts/run_all_tests.sh
# Look for "PASSED: N / FAILED: 0" and the per-stage "--- PASS: ... ---" lines. It runs ALL 29
# adopted native suites in its stage 9, so a regression anywhere shows here. Takes ~20-30 min.

# Gate 2: performance regression vs real Redis
bash scripts/dev/redis_vs_peadb_benchmark.sh
# Exit 0 = pass. Ratios near 1.0x are fine. The benchmark is NOISY on a loaded machine
# (metrics swing 0.8x–1.3x run to run); a single outlier that swaps which metric is "slow"
# between runs is noise, not a regression. The stable signal is the data-size p50 latency table
# (~1.0x). If ONE metric is consistently slow across 2+ runs, investigate.
```

Both are long-running. Run them in the background (`run_in_background: true`) and use the Monitor
tool with an `until grep -q ...; do sleep 10; done` loop on a sentinel you append (e.g.
`echo "EXIT=$?" >> logfile`). **Do not** poll with foreground `sleep`.

After Gate 1 passes, regenerate the parity artifacts + ratchet + scoreboard:
```bash
python3 scripts/redis/parse_tcl_results.py artifacts/redis-native \
  --out-json artifacts/redis-parity/pass_rate.json --out-md artifacts/redis-parity/pass_rate.md
python3 scripts/ci/check_suite_ratchet.py      # must print "suite ratchet OK"
python3 scripts/ci/generate_compat_scoreboard.py   # regenerates compat/commands.generated.md
python3 scripts/ci/check_compat_delta.py       # validates delta.md structure
```
(`artifacts/` is gitignored — don't commit it. `commands.generated.md` IS committed.)

---

## 3. The suite-adoption workflow (the exact recipe)

This is the loop that has produced every commit. Follow it precisely.

1. **Baseline the suite** — run it native (§1). Note ok/err/exc and read the first failure. If it
   `exit=124` (timeout) or `[exception]`, the suite HANGS/aborts partway — you must get it to run
   to completion. If `exit=1` with `[err]`, those are the failing assertions.
2. **Understand it fully before coding.** Read the whole `~/.cache/peadb/redis-7.2.5/tests/unit/<suite>.tcl`.
   For non-trivial suites, spawn an "understand" workflow (see §9 — ultracode is ON) with parallel
   agents: (a) map every test + its exact expected strings, (b) audit PeaDB's current
   implementation of the touched features, (c) read the authoritative Redis behavior from the
   cached C source (`~/.cache/peadb/redis-7.2.5/src/*.c`). Redis source is the source of truth for
   exact error strings / semantics.
3. **Implement** the gaps in `src/command.cpp` (most work lives here) / `src/rdb.cpp` /
   `src/protocol.cpp` / `src/datastore.cpp`. Iterate against the live suite (it's the oracle):
   rebuild → run → read next failure → fix → repeat.
4. **Skipfile only what is genuinely out of reach**, and document it. Create/extend
   `scripts/redis/skipfiles/<suite_with_underscores>.skip` (comment lines with `#`, one exact
   test-NAME per line). Every skip line needs a comment naming its `compat/delta.md` item, and you
   must add/update that delta item in the SAME commit (the ratchet enforces this).
5. **Hot-path / broad changes → adversarial review.** If you touched the RESP parser, the dispatch
   chokepoint, RDB, or anything every command hits, run an adversarial-review workflow (find →
   independently verify; see §9). This caught a real DoS regression in wave 12 that the passing
   suite could not see.
6. **Register the suite:**
   - Add `"unit/<suite>"` to the `SUITES=( ... )` array in `scripts/redis/run_redis_tests.sh`.
   - Add a ledger entry to `compat/redis_suite_ledger.json` (alphabetical), using the CANONICAL
     counts (run once with the `-leaks` tag + `--dont-clean`, so leak tests are `[ignore]`d):
     `{"last_counts": {"err": 0, "ignored": <ignore+skip>, "ok": <N>}, "mode": "native", "skipfile_lines": <N>}`.
7. **Docs in the same commit:** update `CHANGELOG.md` (`[Unreleased]`, a "wave N" subsection) and
   `compat/delta.md` if a gap opened/closed/changed.
8. **Run BOTH gates** (§2), regenerate artifacts+ratchet+scoreboard.
9. **Commit** — one concise single-line Conventional Commits subject (`feat:`/`fix:`), imperative,
   no body. Stage files explicitly (NOT `git add -A` — never commit the untracked `.claude/` or
   `AI-TEMP/`). Commit directly to `main` (project rule: linear history, no branches, no `git push`).
10. **Record the wave in the auto-memory** file (see §11) — a fresh AI won't have it, but keep it
    current for continuity.

---

## 4. Verification / ratchet mechanics (how the numbers are enforced)

- `compat/redis_suite_ledger.json` — the canonical per-suite baselines. `mode` is
  `native` / `external` / `excluded` (excluded needs `exclusion_reason`).
- `scripts/redis/parse_tcl_results.py <dir> --out-json ... --out-md ...` — parses the harness logs
  in `artifacts/redis-native/*.log` into `artifacts/redis-parity/pass_rate.json`. Pass-rate =
  `Σok / Σ(ok + err + skipfile_lines)`, so skipping a test never improves the number.
- `scripts/ci/check_suite_ratchet.py` — reads `pass_rate.json` vs the ledger. FAILS on: any `err`
  in an adopted suite; a suite in the ledger missing from results; an `ok` regression; a skipfile
  growth or ledger-count drop UNLESS the same change also touches `compat/delta.md`. Shrinking a
  skipfile is always allowed.
- `scripts/ci/generate_compat_scoreboard.py` — regenerates `compat/commands.generated.md` (status
  ladder: `full` = green-suite-covered AND diff-fuzzed; `upstream`; `diff`; `tested`; `registered`).
- `scripts/ci/check_compat_delta.py` — every numbered `delta.md` item must have
  `- Status:`, `- Owner:`, `- Severity:`, `- Target milestone:`, `- Implemented:`, `- Missing:`
  bullets (exact `- Missing:` label, no parenthetical before the colon — that errors the validator).

**Adopting a suite lowers the aggregate % slightly if it has skips** (its skip lines enter the
denominator). That's expected and ratchet-OK as long as no existing suite regressed and delta.md
is touched for the new skips.

---

## 5. Key source files & conventions

- `src/command.cpp` — ALL command handlers in one file (`command_table()` static-local builds the
  table once). `handle_command()` is the dispatcher. Anonymous-namespace helpers + globals at top;
  public functions after `} // namespace`. The **dispatch chokepoint** (near the end of
  `handle_command`, around the `spec.handler(...)` call) is where per-command instrumentation lives:
  commandstats, SLOWLOG (`slowlog_add`), LATENCY (`latency_add` + `record_cmd_latency`),
  mutation-epoch bump + AOF/replication propagation, keyspace notifications. Many features hook here.
- `src/server.cpp` — single-threaded epoll/kqueue loop, `SessionState`, accept path (sets
  `session.addr`, `session.authenticated`), `safe_handle_command` wrapper, the event-loop cron
  (calls `run_active_expire_cycle()`, `background_save_cron()`, `run_eviction_cycle()`), the
  `try_unblock_client()` serve path (line ~602), and `g_pubsub_send`.
- `src/protocol.cpp` — RESP request parser (`parse_one_command`). Stateless: re-parses the whole
  unconsumed buffer each read (this is WHY the multibulk count is capped at 1M — see wave 12).
  Returns sentinel command names (`__empty__`, `__err_proto__`, `__err_multibulk__`,
  `__err_bulklen__`, `__err_unauth_*__`) that `handle_command` turns into RESP errors.
- `src/rdb.cpp` — RDB reader/writer. Small collections use a REAL Redis listpack encoding now
  (wave 14 fixed a data-corruption bug). Streams use a PeaDB-specific type byte 200 (NOT
  Redis-loadable — deltas 2/5).
- `src/datastore.cpp` / `include/datastore.hpp` — `DataStore`, 16 DBs, all types. `Entry` is a
  tagged union (variant). `debug_digest_value` (order-independent whole-value digest, used by
  consistency/reload tests).
- `include/command.hpp` — `SessionState`, `BlockedState`, extern globals/callbacks shared with server.cpp.
- Conventions: C++20, `-Wall -Wextra -Wpedantic -Werror`, 4-space indent, 100-col, Google-ish
  clang-format. Naming enforced by clang-tidy (functions `camelBack`, members `lower_case_`, etc.).
- `compat/delta.md` — canonical gap registry (items 1–21 currently). `compat/commands.generated.md`
  is auto-generated (don't hand-edit).

---

## 6. Current status — 29 adopted suites

Adopted (all native, 0 err): keyspace, type/string, type/incr, type/hash, type/set, type/list,
type/list-2, type/list-3, type/zset, type/stream, type/stream-cgroups, scan, expire, multi,
scripting, quit, auth, bitops, pubsub, pubsubshard, bitfield, dump, sort, geo, protocol, printver,
other, slowlog, latency-monitor.

Excluded in ledger (permanent / out-of-scope): `unit/cluster`, `unit/moduleapi`,
`unit/oom-score-adj` (Linux-only), `unit/tls`.

---

## 7. What was done this session (waves 11–16) — the details

Each is one commit on `main`. This is the accumulated knowledge; read it before touching related code.

**Wave 11 (`760c677`): unit/geo adopted + unit/auth & unit/pubsub COMPLETED.**
Geo: rewrote the GEO family via a `geoutil` namespace — bit-exact 52-bit interleaved geohash
(lat range ±85.05112878, step 26), haversine (earth R=6372797.560856). `do_geo_search` handles
GEORADIUS/BYMEMBER/GEOSEARCH/GEOSEARCHSTORE; center-member resolved AFTER the type check (missing
key→empty/0-for-STORE, wrong type→WRONGTYPE, present-but-absent member→"could not decode"). GEOHASH
forces the 11th char to '0'. GEOADD folds XX+NX into `syntax error`. GEOHASH/GEOPOS arity -2. 1 skip
(delta 20): unsorted GEORADIUS returns global geohash-score order, not Redis's geohash-neighbor
cell-scan order (result SET identical; Redis docs the order unspecified). **BIG LESSON:** auth &
pubsub had been "adopted while hanging" — their ledger baselines were recorded at the first test
that hung the harness. Fixed 5 real gaps → auth 6→10 ok, pubsub 28→34 ok: (1) unauthenticated
protocol limits (>10 multibulk / >16KB bulk from an unauthed client rejected before buffering);
(2) connect-time auth = !requirepass_is_set() (clientSetDefaultAuth) so a later CONFIG SET
requirepass doesn't retroactively de-auth open conns; RESET re-derives; (3) replica handshake
tolerates -NOAUTH/-NOPERM to the pre-AUTH PING + logs "Unable to AUTH to MASTER"; (4) background
eviction cron `run_eviction_cycle()` (lowering maxmemory evicts + fires `evicted` events without a
following write); (5) RESP3 self-publish ordering — messages to the PUBLISHing client itself are
deferred (server.cpp `self_dispatch_fd`/`self_pubsub_pending`) and appended AFTER the command reply.

**Wave 12 (`83fe1bf`): unit/protocol.** RESP request-parser + DEBUG PROTOCOL rewrite. Redis-exact
framing errors (`expected '$', got 'X'`, `invalid multibulk/bulk length`, `unbalanced quotes in
request`), empty-line + non-positive multibulk = silent no-op, any non-`*` byte → inline, 64KB
single-line desync cap. DEBUG PROTOCOL made RESP2/RESP3-aware (bignum/verbatim/double/null → bulk
in RESP2, true/false → :1/:0, map/set → flat arrays, attrib only in RESP3). **LESSON:** an
adversarial-review workflow caught a real DoS I introduced (I removed the 1M multibulk cap thinking
Redis uses INT_MAX — but Redis's parser is STATEFUL, PeaDB's is STATELESS and re-parses the whole
buffer each recv → O(N²) with a huge declared count). Restored the 1M cap. Suite passed 25/25 even
WITH the bug — suite-green ≠ regression-free for hot-path changes.

**Wave 13 (`169d8e4`): unit/printver + unit/type/list-3.** list-3 already green. printver needed
INFO to emit Redis-standard `redis_git_sha1:00000000` / `redis_git_dirty:0` (was `peadb_git_*`;
also fixes drop-in tooling like redis_exporter).

**Wave 14 (`627ab21`): unit/other + MAJOR listpack RDB data-corruption fix.** The consistency
/reload-digest test surfaced a real bug: ANY list/hash/set/zset element **≥64 bytes** was silently
corrupted across RDB save/load, DUMP/RESTORE, replication full-sync. Root cause (`src/rdb.cpp`): the
home-grown listpack string header used a `0x40-0x7F` prefix that collided with the reader's 6-bit
branch (`enc & 0x80 == 0`), so a ≥64B string decoded as 0-length and desynced the container.
Rewrote writer+reader to REAL Redis listpack encoding (6-bit `0x80|len` / 12-bit `0xE0|..` / 32-bit
`0xF0`+LE4 strings; 7-bit/13-bit/16/24/32/64-bit int decode; size-derived backlen skip). Now
round-trips at every size AND is genuinely Redis-loadable. unit/other also needed: HELP for
PUBSUB/COMMAND/CONFIG/MODULE/CLIENT/FUNCTION; RESET exits pub/sub + MONITOR + runs immediately in
MULTI (added RESET to the queueing-bypass); CONFIG unknown-subcommand → lowercase "unknown
subcommand..."; INFO rdb_changes_since_last_save → delta not absolute. 5 skips (delta 21 + refs to
7/18). **LESSON:** the reload-digest consistency test is a high-value data-integrity oracle — when
it fails, diff `/tmp/repldump1.txt` vs `repldump2.txt` parsed as CSV (Python) to find the exact
differing key, then binary-search the element size.

**Wave 15 (`4db68fc`): unit/slowlog + MIGRATE AUTH/AUTH2.** SlowlogEntry gained peer+name;
argv trim fixed (keep 31 real + "... (N more arguments)"; long-arg 128 + "... (N more bytes)");
`redact_slowlog_args()` masks ACL SETUSER/GETUSER, CONFIG SET requirepass/masterauth/masteruser,
MIGRATE AUTH/AUTH2, AUTH, HELLO...AUTH → "(redacted)"; EXEC excluded (inner cmds logged individually
since EXEC calls handle_command per-cmd); SLOWLOG GET rejects count<-1. **Blocking-command slowlog:**
Redis logs a blocking cmd only when it UNBLOCKS with the full span + original argv. Added
`BlockedState.block_start_ms` (set at chokepoint when a cmd newly blocks; chokepoint SKIPS slowlog
while blocked) and wrapped `try_unblock_client` (body → `_impl`) to log on completion. Implemented
MIGRATE AUTH `<pass>` / AUTH2 `<user> <pass>` (parse + send AUTH to target before SELECT).

**Wave 16 (`903b3a8`): unit/latency-monitor + unit/type/list-2.** Full LATENCY build-out:
**LATENCY HISTOGRAM** = per-command log2 histograms (`g_cmd_latency_hist` map<fullname,{calls,
buckets[64]}>, recorded at the chokepoint); fullname uses the container "parent|sub" form
(is_container_command set) so config|resetstat, client|id are separate keys; reset by CONFIG
RESETSTAT; reply is a RESP map via `encode_map(kv,resp)` (flat array RESP2 / `%` RESP3); filtering
(plain name expands to all `name|*`, full `parent|sub` exact, unknown/never-called skipped). Also:
LATENCY HISTORY returns the full 160-cap sample series (added history deque to LatencyEvent + append
in latency_add), LATENCY GRAPH outputs "<event> - high H ms, low L ms (all time high M ms)",
expire-cycle events timed in run_active_expire_cycle, LATENCY HELP rejects extra args.
**Gotcha:** `encode_map` takes a FLAT vector of already-encoded RESP elements (k,v,k,v).

---

## 8. General learnings / gotchas (carry these forward)

- **Run suites to completion**, not just err=0 (see §0). Verify `exit 0` / "All tests passed".
- **Redis C source is the spec** for exact error text/semantics — grep `~/.cache/peadb/redis-7.2.5/src/`.
  Error messages are often lowercase (e.g. `networking.c` "unknown subcommand or wrong number of
  arguments for '%s'. Try %s HELP.") and `assert_match` in the TCL harness is case-SENSITIVE.
- **The dispatch chokepoint** in `handle_command` is the one place to add per-command
  instrumentation. EXEC's inner commands re-enter `handle_command` (so they hit the chokepoint);
  EXEC itself also hits it (exclude it where Redis flags CMD_SKIP_SLOWLOG). Blocked commands hit the
  chokepoint at block time with `session.blocked.type != None` — gate instrumentation on that.
- **Do not add silent no-op stubs for features that don't work** (e.g. CLIENT TRACKING) — a client
  relying on them would get wrong data. Skipfile the test with a delta note instead. This is
  "production-grade software" — correctness/honesty over green checkmarks.
- **Don't blindly copy a Redis constant** without checking the surrounding architectural assumption
  (stateful vs stateless parser bit us in wave 12).
- **`upper`/`lower`/`now_ms`/`encode_*`** helpers live in `command.cpp`; if you use one before its
  definition, add a forward declaration near line ~343 (where `lower`/`upper` are already forward-declared).
- **Env-race flakes:** in earlier full runs, auth/pubsub sometimes "timed out" under concurrent
  load; artifacts always had err=0 and they passed in isolation. Since wave 11 fixed the real hang,
  the full sweep has been clean. If a suite times out in the sweep but passes in isolation with
  err=0, re-run before treating it as a regression.
- **macOS dev box:** you're on darwin. Some tests only assert on Linux (guarded by `if uname ==
  Linux`) and are no-ops here (e.g. "Process title"), so a green macOS run may hide a Linux-CI
  failure — skipfile such tests rather than rely on the OS guard (the roadmap targets Linux CI).

---

## 9. Ultracode / workflows

Ultracode was ON in the previous session — the developer opted into multi-agent orchestration.
Use the **Workflow** tool for substantive suites:
- **Understand phase:** parallel agents mapping tests + Redis semantics + PeaDB gaps (returns a
  complete spec before you code). ~2-3 agents, `effort: 'high'`.
- **Adversarial review phase (for hot-path/RDB/parser/chokepoint changes):** find→verify pipeline,
  several lenses (framing/liveness/safety/regression), each finding independently verified with a
  "default to REFUTED" prompt. This is what catches bugs the passing suite can't see.
Confirm with the user whether ultracode is still desired before spawning big fleets; otherwise use
single `Agent` calls or work inline.

---

## 10. Next candidates (unadopted suites) + known requirements

Roughly easiest→hardest. Always **baseline first** (§1) — some may already be green (free adoptions,
like list-2/list-3 were).

- **`unit/functions`** — the previous session's intended next target. Needs a real FUNCTION Lua-
  library engine: `FUNCTION LOAD` compiling a library with `redis.register_function`, `FCALL`/
  `FCALL_RO`, FUNCTION LIST/DUMP/RESTORE/STATS/DELETE/FLUSH. Baseline shows `[exception]: ERR Error
  compiling function` — the library-load path is a stub. **Substantial** (the Lua library wrapper
  around the existing EVAL engine). Give it a focused turn.
- **`unit/info` / `unit/info-command`** — INFO field parity audit; `errorstats`/`latencystats`
  sections; COMMAND INFO/DOCS/COUNT/GETKEYS metadata. Medium; lots of small field checks.
- **`unit/introspection` / `introspection-2`** — CLIENT LIST full fields, CLIENT KILL real filters
  (TYPE/LADDR/MAXAGE), CLIENT NO-EVICT/NO-TOUCH, CONFIG REWRITE, CONFIG GET glob. Medium-large.
- **`unit/keyspace-events`** — not present in 7.2.5 (folded into pubsub); ignore.
- **`unit/lazyfree`, `unit/wait`, `unit/shutdown`, `unit/networking`, `unit/querybuf`,
  `unit/limits`, `unit/replybufsize`, `unit/obuf-limits`, `unit/client-eviction`** — server-ops /
  connection-management suites; mixed effort. Baseline each.
- **`unit/maxmemory`, `unit/memefficiency`** — eviction fidelity + real listpack/intset backends
  (memefficiency is Phase 6, needs real encodings — likely defer).
- **`unit/hyperloglog`** — needs the real HYLL byte format (delta 10); large.
- **`unit/acl`, `unit/acl-v2`, `unit/tracking`** — Phase 5 XL: full multi-user ACL engine / CLIENT
  TRACKING + invalidation push. Big; these are why several tests are skipfiled today (delta 7).
- **`unit/pause`** — real CLIENT PAUSE/UNPAUSE (queue-not-reject in the event loop); prerequisite
  for FAILOVER. Medium-large.
- **`integration/*` (replication, rdb, aof, psync2, failover)** — Phase 4, XL, adopt incrementally.
- **`unit/aofrw`** — multi-part AOF (appenddirname + manifest); Phase 4 XL.

Out of scope (documented deltas, do NOT build): Sentinel; full Cluster gossip; module
certification; TLS (optional/last); io-threads/sharding/jemalloc.

---

## 11. The auto-memory (the previous AI's persistent notes)

A fresh Claude Code instance won't have the previous session's auto-memory. The richest file is
`~/.claude/projects/-Users-duc-nguyen-git2-peadb/memory/project_parity_roadmap.md` — it holds the
per-wave narrative (waves 0–16) with all the gotchas. If you have access to that directory, READ
it. If not, §7–§8 above reproduce the load-bearing parts. Other memory files (may or may not be
present): production-hardening, memory+eviction track, autonomous-readiness pass — these describe
pre-parity-roadmap work (Entry tagged-union, eviction, security/durability batches) that is DONE.

The approved roadmap plan file is at
`/Users/duc.nguyen/.claude/plans/this-project-is-peadb-sunny-lamport.md` (6-phase measurement-first
ratchet). Scope confirmed by the owner: target Redis 7.2.5 exactly; single node + replication only
(Sentinel/Cluster/modules = permanent documented deltas); behavioral-parity depth is the priority;
1 strong dev + AI. Two headline metrics (both monotone-up): TCL pass-rate (≥95% target) and
scoreboard `full` count.

---

## 12. Concrete "start here" for the next session

1. `cd /Users/duc.nguyen/git2/peadb && git log --oneline -3` (confirm HEAD is `903b3a8` or later).
2. `cmake --build build -j$(nproc)` to confirm it builds clean.
3. Pick the next suite (§10 — `unit/functions` was queued, or baseline a few and pick a green/near-green one).
4. Follow the adoption workflow (§3). Use the understand+review workflows (§9).
5. Run both mandatory gates (§2), regenerate artifacts+ratchet+scoreboard (§2/§4), commit (§3.9),
   update memory/this doc.

Keep the discipline: real fixes over skips, run-to-completion, both gates green, one coherent
commit per suite, docs in the same commit.
